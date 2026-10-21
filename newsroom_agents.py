"""
Parallel Gemini Managed Agents Engine for the Tech Newsroom Demo.

Spins up 4 isolated Linux VMs in parallel using `antigravity-preview-09-2026`:
  1. VM #1 (Infographic Agent) -> Scrapes live tech news & generates `/workspace/output/infographic.png`
  2. VM #2 (Video Agent)       -> Scrapes live tech news & generates `/workspace/output/news_video.mp4`
  3. VM #3 (Podcast Agent)     -> Scrapes live tech news & generates `/workspace/output/podcast.wav` + `transcript.md`
  4. VM #4 (Live Q&A Agent)    -> Scrapes live tech news + top reader comments into `/workspace/output/live_briefing.md`
                                  for live voice & text Q&A (`gemini-3.8-live` / `gemini-3.1-flash-live-preview`)

Demonstrates key Managed Agents capabilities:
  - `environment={"type": "remote", "sources": [...], "network": {...}}`
  - Declarative inline `.agents/skills/<role>/SKILL.md` and helper scripts
  - Egress proxy header injection (`network.allowlist` -> `transform`)
  - Parallel sandbox execution & real-time SSE step streaming (`thought`, `code_execution`, `google_search`)
  - Extracting generated binary files from each VM via `client.environments.files.download()`
  - Multi-turn conversation & environment reuse (`previous_interaction_id` + `environment=environment_id`)
"""

import asyncio
import base64
import json
import math
import os
import shutil
import subprocess
import time
import traceback
import urllib.request
import wave
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import requests
from google import genai
from google.genai import types

MANAGED_AGENT_ID = "antigravity-preview-09-2026"
SUPPORTED_AGENT_MODELS = (
    "gemini-3.8-flash",
    "gemini-4-argon",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
)
IMAGE_MODELS = (
    "gemini-nano-banana-2.1",
    "gemini-3.1-flash-image",
)
TTS_MODELS = (
    "gemini-3.8-flash-tts",
    "gemini-3.8-flash-lite-tts",
    "gemini-3.1-flash-tts-preview",
)

# ---------------------------------------------------------------------------
# Podcast Style & Voice Presets
# ---------------------------------------------------------------------------
PODCAST_STYLE_CONFIGS: Dict[str, Dict[str, str]] = {
    "energetic": {
        "label": "Energetic morning show",
        "script_instruction": (
            "Write an energetic, upbeat, fast-paced 6-line morning show dialogue between `Alex:` and `Sam:` "
            "with lively enthusiasm and sharp banter about the top 3 stories."
        ),
        "tts_director_note": (
            "Speak in an energetic, upbeat, fast-paced morning show style with lively enthusiasm:"
        ),
    },
    "bedtime": {
        "label": "Bedtime story",
        "script_instruction": (
            "Write a warm, gentle, soothing 6-line bedtime story dialogue between `Alex:` and `Sam:` "
            "that narrates today's top 3 technology stories like a calm, cozy late-night tale."
        ),
        "tts_director_note": (
            "Speak in a soft, warm, gentle, soothing bedtime story tone with a slow, relaxing, unhurried pace:"
        ),
    },
    "calm": {
        "label": "Calm news summary",
        "script_instruction": (
            "Write a calm, clear, measured 6-line public-radio news summary dialogue between `Alex:` and `Sam:` "
            "explaining what happened in the top 3 stories and why it matters."
        ),
        "tts_director_note": (
            "Speak in a calm, measured, clear broadcast news briefing tone:"
        ),
    },
    "skeptical": {
        "label": "Skeptical engineers",
        "script_instruction": (
            "Write a dry, witty, analytical 6-line conversation between two skeptical senior engineers `Alex:` and `Sam:` "
            "questioning the hype and examining the practical trade-offs of the top 3 stories."
        ),
        "tts_director_note": (
            "Speak like two witty, thoughtful, slightly skeptical software engineers having a natural conversation:"
        ),
    },
}

# ---------------------------------------------------------------------------
# 3 Required Parallel Podcast Styles (Distinct Scripts, Voices, Separate Files)
# ---------------------------------------------------------------------------
THREE_PODCAST_STYLES: List[Dict[str, str]] = [
    {
        "id": "energetic",
        "label": "Energetic morning show",
        "voice_alex": "Kore",
        "voice_sam": "Puck",
        "script_instruction": (
            "Write an energetic, upbeat, fast-paced 6-line morning show dialogue between `Alex:` and `Sam:` "
            "with lively coffee-fueled banter, high enthusiasm, and sharp back-and-forth about today's top stories."
        ),
        "tts_director_note": (
            "Speak in an energetic, upbeat, fast-paced morning show style with lively enthusiasm:"
        ),
        "wav_file": "podcast_energetic.wav",
        "script_file": "transcript_energetic.md",
    },
    {
        "id": "bedtime",
        "label": "Bedtime story",
        "voice_alex": "Sulafat",
        "voice_sam": "Enceladus",
        "script_instruction": (
            "Write a warm, gentle, soothing 6-line bedtime story dialogue between `Alex:` and `Sam:` "
            "that narrates today's top technology stories like a calm, cozy late-night meditation for tired engineers to fall asleep to."
        ),
        "tts_director_note": (
            "Speak in a soft, warm, gentle, soothing bedtime story tone with a slow, relaxing, unhurried pace:"
        ),
        "wav_file": "podcast_bedtime.wav",
        "script_file": "transcript_bedtime.md",
    },
    {
        "id": "skeptical",
        "label": "Skeptical engineers",
        "voice_alex": "Fenrir",
        "voice_sam": "Zephyr",
        "script_instruction": (
            "Write a dry, witty, analytical 6-line dialogue between two cynical senior engineers `Alex:` and `Sam:` "
            "questioning the marketing hype, examining benchmarks, and debating real-world production trade-offs."
        ),
        "tts_director_note": (
            "Speak like two witty, thoughtful, slightly skeptical senior software engineers having a natural conversation:"
        ),
        "wav_file": "podcast_skeptical.wav",
        "script_file": "transcript_skeptical.md",
    },
]

# ---------------------------------------------------------------------------
# Shared Sandbox Script: Live Hacker News & Tech News Scraper
# Mounted at `/workspace/tools/scrape_hn.py` inside all 4 Linux VMs
# ---------------------------------------------------------------------------
SANDBOX_SCRAPER_SCRIPT = """#!/usr/bin/env python3
import html
import json
import os
import re
import sys
import urllib.request

def strip_html(text):
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\\s+", " ", text).strip()

def fetch_hn_top_stories(limit=5, topic_filter="", include_comments=False):
    os.makedirs("/workspace/output", exist_ok=True)
    url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    req = urllib.request.Request(url, headers={"User-Agent": "GeminiManagedAgent/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        story_ids = json.loads(resp.read().decode("utf-8"))[:40]

    stories = []
    topic_lower = topic_filter.strip().lower()
    for sid in story_ids:
        try:
            item_url = f"https://hacker-news.firebaseio.com/v0/item/{sid}.json"
            with urllib.request.urlopen(item_url, timeout=6) as r:
                item = json.loads(r.read().decode("utf-8"))
            if not item or item.get("type") != "story" or "title" not in item:
                continue
            title = item.get("title", "")
            if topic_lower and topic_lower not in ("hacker news", "top", "tech", "all", "top hacker news"):
                keywords = [k for k in topic_lower.split() if len(k) > 2]
                if keywords and not any(k in title.lower() for k in keywords):
                    continue

            top_comments = []
            if include_comments:
                for cid in (item.get("kids") or [])[:3]:
                    try:
                        c_url = f"https://hacker-news.firebaseio.com/v0/item/{cid}.json"
                        with urllib.request.urlopen(c_url, timeout=4) as cr:
                            c_item = json.loads(cr.read().decode("utf-8"))
                        if c_item and c_item.get("text") and not c_item.get("deleted"):
                            clean_c = strip_html(c_item.get("text", ""))
                            if clean_c:
                                top_comments.append({
                                    "by": c_item.get("by", "reader"),
                                    "text": clean_c[:360]
                                })
                    except Exception:
                        continue

            stories.append({
                "id": sid,
                "title": title,
                "url": item.get("url", f"https://news.ycombinator.com/item?id={sid}"),
                "hn_url": f"https://news.ycombinator.com/item?id={sid}",
                "score": item.get("score", 100),
                "comments": item.get("descendants", 25),
                "by": item.get("by", "anonymous"),
                "top_comments": top_comments,
            })
            if len(stories) >= limit:
                break
        except Exception:
            continue

    if len(stories) < 3 and topic_lower:
        return fetch_hn_top_stories(limit=limit, topic_filter="", include_comments=include_comments)

    out_path = "/workspace/output/stories.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(stories, f, indent=2)
    print(f"Saved {len(stories)} live stories to {out_path}")
    for idx, s in enumerate(stories, 1):
        print(f"  {idx}. [{s['score']} pts / {s['comments']} comments] {s['title']}")
    return stories

if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--comments"]
    with_comments = "--comments" in sys.argv[1:]
    topic = " ".join(args) if args else ""
    fetch_hn_top_stories(limit=5, topic_filter=topic, include_comments=with_comments)
"""

# ---------------------------------------------------------------------------
# 4 Infographic Visual Style Specifications
# ---------------------------------------------------------------------------
INFOGRAPHIC_STYLES: List[Dict[str, str]] = [
    {
        "id": "style1",
        "filename": "infographic_style1_minimal.png",
        "label": "Style 1: Minimal / Monochrome / Line Art",
        "prompt": """Create visual explanatory material from the supplied source, expressing the distinctive palette, line work, typography, textures, motifs, and composition of the following style.

## VISUAL STYLE: Minimal / Monochrome / Line Art

### Color Palette
- Background: #FFFFFF (White)
- Primary text color: #000000 (Black)
- Accent color: #D3D3D3 (Light Gray)

### Typography
- Headings: Thin sans serif or serif.
- Style: Small and understated placement.

### Layout Grid
- Expansive white space (White Space)

### Illustration Style
- Ultra-thin line art (One Line Art).
- Abstract faces and plants.

### Tone & Voice
- Simple, Refined, Silence, Fashionable, Mature

## Preserve the style's character
- Keep the specified color combinations, brightness, and saturation. When quietness, monochrome, or minimalism defines it, preserve those qualities.
- Improve readability through text placement, line breaks, type size, and contrast directly behind the text. Provide readable text areas alongside rich visual expression.

## Content and readability
- Preserve names, values, units, conditions, negations, and comparison baselines; add no unsupported facts or effects.
- Base factual diagrams, charts, and connecting lines on the source. Distinguish decorative motifs from measurements and causal relationships; do not turn equivalent elements into an invented sequence of steps.
- Make heading and body roles, and diagram-to-explanation relationships, clear.
- Use fonts suitable for English, combining characteristic headings with readable body text. Keep required text complete, adequately sized, and unobscured by other text or patterns. Do not print production instructions.""",
    },
    {
        "id": "style2",
        "filename": "infographic_style2_space_retro.png",
        "label": "Style 2: Space / Retro",
        "prompt": """Create visual explanatory material from the supplied source, expressing the distinctive palette, line work, typography, textures, motifs, and composition of the following style.

## VISUAL STYLE: Collage / Space / Retro

### Color Palette
- Background: #000000 (Space)
- Primary text color: #FFFFFF (White)
- Accent colors: #FFA500 (Orange), #40E0D0 (Turquoise)

### Typography
- Headings: Retrofuturistic font.
- Style: Treated like a title logo.

### Layout Grid
- A surreal scene like an astronaut holding a bouquet

### Illustration Style
- Collage of retro space photographs.
- The feel of old magazine cutouts.

### Tone & Voice
- Surreal, Space, Nostalgia, Dream, Collage

## Preserve the style's character
- Keep the specified color combinations, brightness, and saturation. When bold color areas, ornament, expressive figures, or density define the style, give those features a clear presence.
- Improve readability through text placement, line breaks, type size, and contrast directly behind the text. Provide readable text areas alongside rich visual expression.

## Content and readability
- Preserve names, values, units, conditions, negations, and comparison baselines; add no unsupported facts or effects.
- Base factual diagrams, charts, and connecting lines on the source. Distinguish decorative motifs from measurements and causal relationships; do not turn equivalent elements into an invented sequence of steps.
- Make heading and body roles, and diagram-to-explanation relationships, clear.
- Use fonts suitable for English, combining characteristic headings with readable body text. Keep required text complete, adequately sized, and unobscured by other text or patterns. Do not print production instructions.""",
    },
    {
        "id": "style3",
        "filename": "infographic_style3_pop_art.png",
        "label": "Style 3: Pop Art / Dots / Comic",
        "prompt": """Create visual explanatory material from the supplied source, expressing the distinctive palette, line work, typography, textures, motifs, and composition of the following style.

## VISUAL STYLE: Pop Art / Dots / Comic

### Color Palette
- Background: #FFFF00 (Yellow)
- Primary text color: #000000 (Black)
- Accent colors: #FF0000 (Red), #0000FF (Blue)

### Typography
- Headings: Comic-book font.
- Style: Put lettering in speech balloons or treat it like sound effects.

### Illustration Style
- American-comic-style poster art with Ben-Day halftone dots.

### Data Visualization
- Flat fills in highly saturated primary colors.

### Tone & Voice
- Mass-market, Commercial, Impactful, American, Energetic

## Preserve the style's character
- Keep the specified color combinations, brightness, and saturation. When bold color areas, ornament, expressive figures, or density define the style, give those features a clear presence.
- Improve readability through text placement, line breaks, type size, and contrast directly behind the text. Provide readable text areas alongside rich visual expression.

## Content and readability
- Preserve names, values, units, conditions, negations, and comparison baselines; add no unsupported facts or effects.
- Base factual diagrams, charts, and connecting lines on the source. Distinguish decorative motifs from measurements and causal relationships; do not turn equivalent elements into an invented sequence of steps.
- Make heading and body roles, and diagram-to-explanation relationships, clear.
- Use fonts suitable for English, combining characteristic headings with readable body text. Keep required text complete, adequately sized, and unobscured by other text or patterns. Do not print production instructions.""",
    },
    {
        "id": "style4",
        "filename": "infographic_style4_watercolor.png",
        "label": "Style 4: Sketch / Watercolor / Travel",
        "prompt": """Create visual explanatory material from the supplied source, expressing the distinctive palette, line work, typography, textures, motifs, and composition of the following style.

## VISUAL STYLE: Sketch / Watercolor / Travel

### Color Palette
- Background: Rough watercolor-paper texture.
- Primary text color: #4682B4 (Steel Blue)
- Accent colors: #A0522D (Sienna), #556B2F (Dark Olive Green), #D3D3D3 (Wash/Fade)

### Typography
- Headings: Handwritten font or casual serif.
- Style: Like a pencil-written note.

### Layout Grid
- Loose, open, vignette style

### Illustration Style
- Urban-sketch style with loose pencil lines and pale watercolor fills.
- Street-corner scenes, buildings, café items, and maps.

### Data Visualization
- Wet-on-wet (bleed) and pencil sketch marks.

### Tone & Voice
- Relaxed, Artistic, Personal, Nostalgic, Organic

## Preserve the style's character
- Keep the specified color combinations, brightness, and saturation.
- Improve readability through text placement, line breaks, type size, and contrast directly behind the text. Provide readable text areas alongside rich visual expression.

## Content and readability
- Preserve names, values, units, conditions, negations, and comparison baselines; add no unsupported facts or effects.
- Base factual diagrams, charts, and connecting lines on the source. Distinguish decorative motifs from measurements and causal relationships; do not turn equivalent elements into an invented sequence of steps.
- Make heading and body roles, and diagram-to-explanation relationships, clear.
- Use fonts suitable for English, combining characteristic headings with readable body text. Keep required text complete, adequately sized, and unobscured by other text or patterns. Do not print production instructions.""",
    },
]

# ---------------------------------------------------------------------------
# VM #1 Sandbox Helper: 4-Style Infographic Generator
# Mounted at `/workspace/tools/generate_infographic.py`
# ---------------------------------------------------------------------------
SANDBOX_INFOGRAPHIC_SCRIPT = """#!/usr/bin/env python3
import base64
import concurrent.futures
import json
import math
import os
import subprocess
import sys
import urllib.request

os.makedirs("/workspace/output", exist_ok=True)

def load_stories():
    path = "/workspace/output/stories.json"
    if not os.path.exists(path):
        subprocess.run([sys.executable, "/workspace/tools/scrape_hn.py"], check=False)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return [
        {"title": "Next-Gen AI Agents Run in Sandboxed Linux VMs", "score": 842, "comments": 310, "by": "devrel"},
        {"title": "Open-Source High-Speed Inference Engine Released", "score": 615, "comments": 195, "by": "systems"},
        {"title": "Breakthrough in Solid-State Battery Energy Density", "score": 530, "comments": 240, "by": "science"},
        {"title": "WebAssembly Component Model Reaches New Milestone", "score": 412, "comments": 148, "by": "wasm"},
        {"title": "Autonomous Satellites Demonstrate In-Orbit Mesh Networking", "score": 389, "comments": 112, "by": "space"},
    ]

def build_top_3_takeaways(stories):
    takeaways_path = "/workspace/output/takeaways.md"
    if os.path.exists(takeaways_path):
        with open(takeaways_path, "r", encoding="utf-8") as f:
            existing = f.read().strip()
        if existing:
            return existing

    top3 = stories[:3]
    lines = ["Top 3 Takeaways from Today's Technology Headlines:"]
    for i, s in enumerate(top3, 1):
        lines.append(
            f"{i}. {s.get('title', 'Tech Story')} — {s.get('score', 0)} points and {s.get('comments', 0)} comments (posted by {s.get('by', 'reader')})."
        )
    summary = "\\n".join(lines)
    with open(takeaways_path, "w", encoding="utf-8") as f:
        f.write(summary)
    return summary

def try_gemini_image_rest(api_key, full_prompt, out_path):
    if not api_key:
        return False
    for img_model in ("gemini-nano-banana-2.1", "gemini-3.1-flash-image"):
        # 1. Try Interactions REST endpoint
        try:
            url = "https://generativelanguage.googleapis.com/v1beta/interactions"
            payload = {
                "model": img_model,
                "input": full_prompt,
                "response_format": {"type": "image", "aspect_ratio": "4:3"},
            }
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": api_key,
                    "Api-Revision": "2026-05-20",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=75) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            img_b64 = None
            if isinstance(body.get("output_image"), dict):
                img_b64 = body["output_image"].get("data")
            if not img_b64:
                for step in reversed(body.get("steps") or []):
                    if isinstance(step, dict) and step.get("type") == "model_output":
                        for part in reversed(step.get("content") or []):
                            if isinstance(part, dict) and part.get("type") == "image" and part.get("data"):
                                img_b64 = part["data"]
                                break
            if img_b64:
                with open(out_path, "wb") as f:
                    f.write(base64.b64decode(img_b64))
                print(f"Generated {out_path} via Interactions API ({img_model})")
                return True
        except Exception as e:
            print(f"Interactions image note for {out_path} ({img_model}): {e}")

        # 2. Try generateContent REST endpoint
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{img_model}:generateContent"
            payload = {
                "contents": [{"parts": [{"text": full_prompt}]}],
                "generationConfig": {"responseModalities": ["IMAGE"]},
            }
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": api_key,
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=75) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            for cand in body.get("candidates") or []:
                for part in (cand.get("content") or {}).get("parts") or []:
                    inline = part.get("inlineData") or {}
                    if inline.get("data"):
                        with open(out_path, "wb") as f:
                            f.write(base64.b64decode(inline["data"]))
                        print(f"Generated {out_path} via generateContent API ({img_model})")
                        return True
        except Exception as e:
            print(f"generateContent image note for {out_path} ({img_model}): {e}")

    return False

def render_styled_fallback(style_id, stories, out_path, note=""):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "matplotlib", "pillow"], check=True)
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np

    top3 = stories[:3]
    if style_id == "style1":
        # Style 1: Minimal / Monochrome / Line Art (#FFFFFF bg, #000000 text, #D3D3D3 accent)
        fig = plt.figure(figsize=(10, 7.5), dpi=150, facecolor="#FFFFFF")
        ax = fig.add_axes([0, 0, 1, 1], facecolor="#FFFFFF")
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.axis("off")

        # Continuous ultra-thin one-line botanical/abstract motif on right
        t = np.linspace(0, 2 * np.pi, 400)
        x_line = 82 + 9 * np.sin(t) + 3 * np.cos(3 * t)
        y_line = 52 + 28 * np.cos(t * 0.7) + 4 * np.sin(4 * t)
        ax.plot(x_line, y_line, color="#000000", linewidth=0.7)
        ax.plot([10, 68], [84, 84], color="#D3D3D3", linewidth=0.8)

        ax.text(10, 88, note or "TOP THREE TAKEAWAYS", color="#000000", fontsize=11, fontweight="light", family="serif")
        for idx, s in enumerate(top3, 1):
            y = 70 - (idx - 1) * 23
            title = s.get("title", "")[:54]
            pts = s.get("score", 0)
            comms = s.get("comments", 0)
            ax.text(10, y, f"0{idx}", color="#D3D3D3", fontsize=18, family="serif")
            ax.text(17, y + 1, title, color="#000000", fontsize=11.5, family="sans-serif")
            ax.text(17, y - 4.5, f"{pts} points   /   {comms} comments", color="#000000", fontsize=8.5, alpha=0.65)
            bar_w = min(45, max(5, pts / 15.0))
            ax.plot([17, 17 + bar_w], [y - 8, y - 8], color="#000000", linewidth=0.9)
            ax.plot([17 + bar_w, 62], [y - 8, y - 8], color="#D3D3D3", linewidth=0.6)

        plt.savefig(out_path, dpi=150, facecolor="#FFFFFF")
        plt.close(fig)

    elif style_id == "style2":
        # Style 2: Space / Retro (#000000 bg, #FFFFFF text, #FFA500 orange, #40E0D0 turquoise)
        fig = plt.figure(figsize=(10, 7.5), dpi=150, facecolor="#000000")
        ax = fig.add_axes([0, 0, 1, 1], facecolor="#000000")
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.axis("off")

        np.random.seed(42)
        sx = np.random.uniform(2, 98, 90)
        sy = np.random.uniform(2, 98, 90)
        ax.scatter(sx, sy, s=np.random.uniform(2, 18, 90), c="#FFFFFF", alpha=0.55)

        # Retro orbital rings & collage cutout frames
        circle1 = plt.Circle((82, 76), 11, color="#FFA500", fill=True, alpha=0.85)
        circle2 = plt.Circle((82, 76), 15, color="#40E0D0", fill=False, linewidth=1.8, linestyle="--")
        ax.add_patch(circle1)
        ax.add_patch(circle2)

        ax.text(8, 88, note or "RETRO SPACE BRIEFING // TOP 3", color="#FFA500", fontsize=16, fontweight="bold", family="monospace")
        ax.plot([8, 65], [84, 84], color="#40E0D0", linewidth=2.0)

        for idx, s in enumerate(top3, 1):
            y = 66 - (idx - 1) * 24
            accent = "#40E0D0" if idx % 2 == 1 else "#FFA500"
            ax.add_patch(plt.Rectangle((8, y - 10), 82, 17, facecolor="#111318", edgecolor=accent, linewidth=1.6))
            ax.text(11, y + 1, f"TAKEAWAY 0{idx}: {s.get('title', '')[:50]}", color="#FFFFFF", fontsize=11.5, fontweight="bold", family="monospace")
            ax.text(11, y - 5.5, f"SIGNAL SCORE: {s.get('score', 0)} PTS   |   TRANSMISSIONS: {s.get('comments', 0)} COMMENTS", color=accent, fontsize=9, family="monospace")

        plt.savefig(out_path, dpi=150, facecolor="#000000")
        plt.close(fig)

    elif style_id == "style3":
        # Style 3: Pop Art / Dots / Comic (#FFFF00 yellow bg, #000000 text, #FF0000 red, #0000FF blue)
        fig = plt.figure(figsize=(10, 7.5), dpi=150, facecolor="#FFFF00")
        ax = fig.add_axes([0, 0, 1, 1], facecolor="#FFFF00")
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.axis("off")

        # Halftone Ben-Day dots grid
        gx, gy = np.meshgrid(np.linspace(2, 98, 28), np.linspace(2, 98, 22))
        ax.scatter(gx.flatten(), gy.flatten(), s=18, c="#FF0000", alpha=0.22)

        ax.add_patch(plt.Rectangle((6, 83), 88, 11, facecolor="#FF0000", edgecolor="#000000", linewidth=3.0))
        ax.text(50, 88.5, (note or "TOP 3 TECH HEADLINE TAKEAWAYS!").upper(), color="#FFFFFF", fontsize=15, fontweight="bold", ha="center", va="center")

        for idx, s in enumerate(top3, 1):
            y = 64 - (idx - 1) * 24
            fill_col = "#FFFFFF"
            badge_col = "#0000FF" if idx != 2 else "#FF0000"
            # Comic speech balloon box with thick black shadow
            ax.add_patch(plt.Rectangle((8.8, y - 10.8), 84, 17, facecolor="#000000"))
            ax.add_patch(plt.Rectangle((7.5, y - 9.5), 84, 17, facecolor=fill_col, edgecolor="#000000", linewidth=2.8))
            ax.add_patch(plt.Rectangle((7.5, y - 9.5), 12, 17, facecolor=badge_col, edgecolor="#000000", linewidth=2.8))
            ax.text(13.5, y - 1, f"#{idx}!", color="#FFFFFF", fontsize=15, fontweight="bold", ha="center", va="center")
            ax.text(22, y + 1.5, s.get("title", "")[:48].upper(), color="#000000", fontsize=11, fontweight="bold")
            ax.text(22, y - 5.2, f"POW! {s.get('score', 0)} POINTS   ·   {s.get('comments', 0)} COMMENTS!", color=badge_col, fontsize=9.5, fontweight="bold")

        plt.savefig(out_path, dpi=150, facecolor="#FFFF00")
        plt.close(fig)

    else:
        # Style 4: Sketch / Watercolor / Travel (Watercolor paper #F7F3E9, #4682B4 steel blue, #A0522D sienna, #556B2F olive)
        fig = plt.figure(figsize=(10, 7.5), dpi=150, facecolor="#F7F3E9")
        ax = fig.add_axes([0, 0, 1, 1], facecolor="#F7F3E9")
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.axis("off")

        # Soft watercolor wash circles
        ax.add_patch(plt.Circle((22, 75), 18, color="#4682B4", alpha=0.12))
        ax.add_patch(plt.Circle((78, 55), 22, color="#A0522D", alpha=0.10))
        ax.add_patch(plt.Circle((45, 25), 20, color="#556B2F", alpha=0.11))

        ax.text(9, 88, note or "Field Notes: Top 3 Technology Takeaways", color="#4682B4", fontsize=16, style="italic", family="serif", fontweight="bold")
        ax.plot([9, 75], [84.5, 84.8], color="#A0522D", linewidth=1.2, linestyle="-.")

        for idx, s in enumerate(top3, 1):
            y = 67 - (idx - 1) * 23
            col = ["#4682B4", "#A0522D", "#556B2F"][(idx - 1) % 3]
            ax.plot([9, 89], [y - 9, y - 8.7], color="#D3D3D3", linewidth=1.4)
            ax.text(9, y + 1, f"Note {idx} — {s.get('title', '')[:54]}", color="#4682B4", fontsize=12, family="serif", fontweight="bold")
            ax.text(12, y - 5, f"Recorded {s.get('score', 0)} reader points & {s.get('comments', 0)} discussion notes (by {s.get('by', 'reader')})", color=col, fontsize=9.8, style="italic", family="serif")

        plt.savefig(out_path, dpi=150, facecolor="#F7F3E9")
        plt.close(fig)

    print(f"Saved styled infographic ({style_id}) to {out_path}")

def main():
    note = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    stories = load_stories()
    takeaways_text = build_top_3_takeaways(stories)
    api_key = os.environ.get("GEMINI_API_KEY", "")

    with open("/workspace/tools/infographic_styles.json", "r", encoding="utf-8") as f:
        styles = json.load(f)

    def process_one(st):
        out_path = os.path.join("/workspace/output", st["filename"])
        full_prompt = (
            f"{st['prompt']}\\n\\n"
            f"## SUPPLIED SOURCE MATERIAL (Top 3 Takeaways from Today's Headlines):\\n"
            f"{takeaways_text}\\n"
            f"{note}"
        )
        if not try_gemini_image_rest(api_key, full_prompt, out_path):
            render_styled_fallback(st["id"], stories, out_path, note=note)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(process_one, styles))

if __name__ == "__main__":
    main()
"""


# ---------------------------------------------------------------------------
# VM #2 Sandbox Helper: Video Broadcast Generator (Omni Flash + FFmpeg Fallback)
# Mounted at `/workspace/tools/generate_video.py`
# ---------------------------------------------------------------------------
SANDBOX_VIDEO_SCRIPT = """#!/usr/bin/env python3
import base64
import json
import os
import shutil
import subprocess
import sys

os.makedirs("/workspace/output", exist_ok=True)

def load_stories():
    path = "/workspace/output/stories.json"
    if not os.path.exists(path):
        subprocess.run([sys.executable, "/workspace/tools/scrape_hn.py"], check=False)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return [
        {"title": "Next-Gen AI Agents Run in Sandboxed Linux VMs", "score": 842, "comments": 310},
        {"title": "Open-Source High-Speed Inference Engine Released", "score": 615, "comments": 195},
        {"title": "Breakthrough in Solid-State Battery Energy Density", "score": 530, "comments": 240},
    ]

def build_one_liner_summaries(stories):
    defaults = [
        "Autonomous AI workloads transition from static prompts to multi-turn sandboxed execution.",
        "New C++ inference engine cuts model serving latency by 65% with zero memory fragmentation.",
        "Ceramic solid-state separator achieves 500 Wh/kg cell density with zero thermal runaway risk.",
        "Cross-language composability arrives as WASI 0.2 achieves full browser parity.",
        "Distributed mesh networking demonstrates low-latency orbital communication.",
    ]
    items = []
    for idx, s in enumerate(stories[:5]):
        items.append({
            "title": s.get("title", f"Tech Story {idx+1}"),
            "summary": s.get("summary") or defaults[idx % len(defaults)],
            "score": s.get("score", 100),
            "comments": s.get("comments", 20),
        })
    s1_t = items[0]["title"] if len(items) > 0 else "Godot C++ Architecture"
    s2_t = items[1]["title"] if len(items) > 1 else "Audio-Reactive LED Hardware"
    s3_t = items[2]["title"] if len(items) > 2 else "Untested Core Infrastructure"
    s4_t = items[3]["title"] if len(items) > 3 else "Legendary 1993 Server"

    omni_prompt = (
        "A blistering 20-second cinematic 3D motion graphics reel rendered in Octane with hyper-dynamic camera sweeps, kinetic typography, and synchronized electro-industrial sound design. "
        f"[0:00-0:05] Camera plunges into a luminous blue Godot engine matrix where glowing C++ code fragments assemble around dynamic low-poly game geometry for '{s1_t}'. "
        f"[0:05-0:10] Seamless spatial morph into a golden-ratio Fibonacci spiral: thousands of micro-LEDs ignite into an audio-reactive phyllotaxis pattern pulsating to driving synth kicks for '{s2_t}'. "
        f"[0:10-0:15] Camera dives through the LED core into a shadowy digital underworld of towering server monoliths under simulated chaos stress tests for '{s3_t}'. "
        f"[0:15-0:20] A blast of hydraulic fog clears to spotlight a battle-hardened retro server rack humming with vibrant amber CRT glow and an infinite uptime counter for '{s4_t}', culminating in a high-voltage cybernetic title drop."
    )
    narration_script = (
        f"Unleash pure power: link any C++ library directly into Godot! "
        f"Witness Phyllotaxis, where botanical math sparks an audio-reactive LED spectacle. "
        f"Probe the dark, untested digital systems running in silence, "
        f"and salute a legendary server still humming strong after decades! "
        f"That's your technology news pulse!"
    )
    with open("/workspace/output/omni_prompt.txt", "w", encoding="utf-8") as f:
        f.write(omni_prompt)
    with open("/workspace/output/narration_script.md", "w", encoding="utf-8") as f:
        f.write(narration_script)

    sb_lines = [
        "# Top Hacker News Stories — 20-Second Video Storyboard",
        "",
        "A sleek motion & animated graphics news summary displaying the top Hacker News stories with full broadcast narration and kinetic 3D visuals.",
        "",
        "## 🎙️ Spoken Broadcast Narration Script (20s Voiceover)",
        f"> {narration_script}",
        "",
        "## 🎬 Gemini Omni Generation Prompt (Kinetic 3D Motion)",
        "```text",
        omni_prompt,
        "```",
        "",
        "## 📌 Top Stories — 1-Liner Concise Summaries",
        "",
    ]
    for idx, it in enumerate(items, 1):
        sb_lines.append(f"{idx}. **{it['title']}** (▲ {it['score']} pts · {it['comments']} comments)")
        sb_lines.append(f"   - *1-Liner Takeaway:* {it['summary']}")
        sb_lines.append("")
    with open("/workspace/output/storyboard.md", "w", encoding="utf-8") as f:
        f.write("\n".join(sb_lines))
    return items, omni_prompt, narration_script

def try_omni_flash_video(stories, custom_prompt=""):
    try:
        from google import genai
        client = genai.Client()
        summaries, omni_prompt, narration_script = build_one_liner_summaries(stories)
        prompt1 = custom_prompt or omni_prompt
        print("Creating 20s motion graphics video with gemini-omni-1.1-flash...")
        inter1 = client.interactions.create(
            model="gemini-omni-1.1-flash",
            input=prompt1,
            response_format={"type": "video", "aspect_ratio": "16:9"},
        )
        vid_b64 = None
        if getattr(inter1, "output_video", None) and getattr(inter1.output_video, "data", None):
            vid_b64 = inter1.output_video.data

        if getattr(inter1, "id", None):
            try:
                ext_prompt = (
                    "Extend this cinematic motion graphics animation for another 10 seconds smoothly continuing the sequence, "
                    "maintaining the dark obsidian aesthetic, kinetic typography transitions, and glowing holographic lines."
                )
                inter2 = client.interactions.create(
                    model="gemini-omni-1.1-flash",
                    previous_interaction_id=inter1.id,
                    input=ext_prompt,
                    response_format={"type": "video", "aspect_ratio": "16:9"},
                )
                if getattr(inter2, "output_video", None) and getattr(inter2.output_video, "data", None):
                    vid_b64 = inter2.output_video.data
            except Exception as ext_err:
                print("Omni extension note:", ext_err)

        if vid_b64:
            out_path = "/workspace/output/news_video.mp4"
            with open(out_path, "wb") as f:
                f.write(base64.b64decode(vid_b64))
            print(f"Saved 20s Omni video to {out_path}")
            return True
    except Exception as e:
        print(f"Omni Flash note: {e}")
    return False

def build_sandbox_mp4_reel(stories):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "matplotlib", "pillow"], check=True)
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

    summaries, omni_prompt, narration_script = build_one_liner_summaries(stories)
    frames_dir = "/tmp/video_frames_20s"
    os.makedirs(frames_dir, exist_ok=True)
    fps = 10
    total_seconds = 20
    total_frames = fps * total_seconds

    slides = [
        {
            "type": "intro",
            "tag": "TECHNOLOGY NEWS BRIEFING",
            "title": "TOP HACKER NEWS STORIES",
            "summary": "A 20-Second Sleek Motion Summary of Today's Key Developments",
            "sub": f"{len(summaries)} Stories from the Community",
        }
    ]
    for idx, s in enumerate(summaries[:4], 1):
        slides.append({
            "type": "story",
            "idx": idx,
            "tag": f"STORY 0{idx} // 04",
            "title": s["title"],
            "summary": s["summary"],
            "sub": f"▲ {s.get('score', 100)} pts   Comments: {s.get('comments', 20)}",
        })

    frames_per_slide = total_frames // len(slides)

    for s_idx, slide in enumerate(slides):
        for f in range(frames_per_slide):
            frame_idx = s_idx * frames_per_slide + f
            prog_overall = frame_idx / float(total_frames)
            prog_slide = f / float(frames_per_slide)

            fig = plt.figure(figsize=(12.8, 7.2), dpi=100, facecolor="#0A0E17")
            ax = fig.add_axes([0, 0, 1, 1], facecolor="#0A0E17")
            ax.set_xlim(0, 100)
            ax.set_ylim(0, 100)
            ax.axis("off")

            # Cyber grid lines
            for y in range(10, 100, 15):
                ax.plot([5, 95], [y, y], color="#162032", linewidth=0.5, alpha=0.35)
            for x in range(10, 100, 15):
                ax.plot([x, x], [10, 90], color="#162032", linewidth=0.5, alpha=0.35)

            # Top HUD Bar
            ax.add_patch(plt.Rectangle((6, 88), 88, 0.4, color="#1F2E47"))
            ax.text(6, 91, "● LIVE BRIEFING // 20s MOTION REEL", color="#00F0FF", fontsize=12, fontweight="bold", family="sans-serif")
            ax.text(94, 91, f"{int(prog_overall * 20):02d}s / 20s", color="#8B9BB4", fontsize=12, ha="right", family="monospace")

            # Continuous Progress Bar at bottom
            ax.add_patch(plt.Rectangle((6, 6), 88, 1.2, color="#131D2D"))
            ax.add_patch(plt.Rectangle((6, 6), 88 * min(1.0, prog_overall), 1.2, color="#00F0FF"))

            # Slide content with smooth fade-in
            alpha = min(1.0, prog_slide * 3.5)
            ax.text(6, 78, slide["tag"], color="#00F0FF" if slide.get("type") == "story" else "#A78BFA", fontsize=15, fontweight="bold", alpha=alpha)

            # Title formatting
            words = slide["title"].split()
            lines, cur = [], []
            for w in words:
                cur.append(w)
                if len(" ".join(cur)) > 30:
                    lines.append(" ".join(cur[:-1]))
                    cur = [w]
            if cur:
                lines.append(" ".join(cur))

            for l_i, line in enumerate(lines[:3]):
                ax.text(6, 64 - l_i * 11, line, color="#FFFFFF", fontsize=26, fontweight="bold", alpha=alpha)

            # 1-Liner Summary Box
            box_y = 64 - len(lines[:3]) * 11 - 2
            ax.add_patch(plt.Rectangle((6, box_y - 8), 88, 8, color="#0F172A", ec="#38BDF8", lw=1, alpha=alpha * 0.85))
            ax.text(8, box_y - 3, "1-LINER TAKEAWAY:", color="#38BDF8", fontsize=11, fontweight="bold", alpha=alpha)
            ax.text(8, box_y - 6.5, slide["summary"][:95], color="#E2E8F0", fontsize=13, alpha=alpha)

            # Sub / HN Score
            ax.text(6, 14, slide["sub"], color="#94A3B8", fontsize=14, fontweight="bold", alpha=alpha)

            plt.savefig(os.path.join(frames_dir, f"frame_{frame_idx:04d}.png"), dpi=100, facecolor="#0A0E17")
            plt.close(fig)

    out_mp4 = "/workspace/output/news_video.mp4"
    narr_wav = "/workspace/output/narration.wav"
    ffmpeg_bin = shutil.which("ffmpeg")
    if not ffmpeg_bin:
        try:
            subprocess.run(["pip", "install", "-q", "imageio-ffmpeg"], check=True)
            import imageio_ffmpeg
            ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            pass

    if ffmpeg_bin:
        if os.path.exists(narr_wav) and os.path.getsize(narr_wav) > 1000:
            cmd = [
                ffmpeg_bin, "-y",
                "-framerate", str(fps),
                "-i", os.path.join(frames_dir, "frame_%04d.png"),
                "-i", narr_wav,
                "-f", "lavfi", "-i", "sine=frequency=220:sample_rate=44100",
                "-filter_complex", "[1:a]volume=1.35,apad=whole_dur=20[voice]; [2:a]volume=0.03,afade=t=out:st=18:d=2[synth]; [voice][synth]amix=inputs=2:duration=first:dropout_transition=2,afade=t=out:st=19:d=1[aout]",
                "-map", "0:v", "-map", "[aout]",
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k", "-t", "20",
                out_mp4
            ]
        else:
            cmd = [
                ffmpeg_bin, "-y",
                "-framerate", str(fps),
                "-i", os.path.join(frames_dir, "frame_%04d.png"),
                "-f", "lavfi", "-i", "sine=frequency=220:sample_rate=44100",
                "-filter_complex", "[1:a]volume=0.03,afade=t=out:st=18:d=2[a]",
                "-map", "0:v", "-map", "[a]",
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-t", "20",
                out_mp4
            ]
        subprocess.run(cmd, check=True)
        print(f"Saved 20s sleek motion graphics video to {out_mp4}")

if __name__ == "__main__":
    custom = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    stories = load_stories()
    if not try_omni_flash_video(stories, custom_prompt=custom):
        build_sandbox_mp4_reel(stories)
"""


# ---------------------------------------------------------------------------
# VM #3 Sandbox Helper: Two-Host Podcast Generator (gemini-3.8-flash-tts / gemini-3.1-flash-tts-preview)
# Mounted at `/workspace/tools/generate_podcast.py`
# ---------------------------------------------------------------------------
SANDBOX_PODCAST_SCRIPT = """#!/usr/bin/env python3
import base64
import json
import os
import subprocess
import sys
import urllib.request
import wave

os.makedirs("/workspace/output", exist_ok=True)

def load_stories():
    path = "/workspace/output/stories.json"
    if not os.path.exists(path):
        subprocess.run([sys.executable, "/workspace/tools/scrape_hn.py"], check=False)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return [
        {"title": "Next-Gen AI Agents Run in Sandboxed Linux VMs", "score": 842, "comments": 310},
        {"title": "Open-Source High-Speed Inference Engine Released", "score": 615, "comments": 195},
        {"title": "Breakthrough in Solid-State Battery Energy Density", "score": 530, "comments": 240},
    ]

def load_podcast_config():
    cfg_path = "/workspace/tools/podcast_config.json"
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "voice_alex": "Kore",
        "voice_sam": "Puck",
        "style": "energetic",
        "tts_director_note": "Speak in an energetic, upbeat, fast-paced morning show style with lively enthusiasm:",
    }

def save_wave_pcm(filename, pcm_bytes, channels=1, rate=24000, sample_width=2):
    with wave.open(filename, "wb") as wf:
        wf.setnchannels(channels)
        wf.setsampwidth(sample_width)
        wf.setframerate(rate)
        wf.writeframes(pcm_bytes)

def extract_audio_b64_from_interaction_dict(data):
    if not isinstance(data, dict):
        return None
    out_aud = data.get("output_audio")
    if isinstance(out_aud, dict) and out_aud.get("data"):
        return out_aud["data"]
    for step in reversed(data.get("steps") or []):
        if isinstance(step, dict) and step.get("type") == "model_output":
            for part in reversed(step.get("content") or []):
                if isinstance(part, dict) and part.get("type") == "audio" and part.get("data"):
                    return part["data"]
    return None

def try_rest_interactions_tts(api_key, model_name, tts_prompt, voice_alex, voice_sam):
    url = "https://generativelanguage.googleapis.com/v1beta/interactions"
    payload = {
        "model": model_name,
        "input": tts_prompt,
        "response_format": {"type": "audio"},
        "generation_config": {
            "speech_config": [
                {"speaker": "Alex", "voice": voice_alex},
                {"speaker": "Sam", "voice": voice_sam},
            ]
        },
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
            "Api-Revision": "2026-05-20",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return extract_audio_b64_from_interaction_dict(body)

def try_rest_generate_content_tts(api_key, model_name, tts_prompt, voice_alex, voice_sam):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
    payload = {
        "contents": [{"parts": [{"text": tts_prompt}]}],
        "generationConfig": {
            "responseModalities": ["AUDIO"],
            "speechConfig": {
                "multiSpeakerVoiceConfig": {
                    "speakerVoiceConfigs": [
                        {
                            "speaker": "Alex",
                            "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice_alex}},
                        },
                        {
                            "speaker": "Sam",
                            "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice_sam}},
                        },
                    ]
                }
            },
        },
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    for cand in body.get("candidates") or []:
        content = cand.get("content") or {}
        for part in content.get("parts") or []:
            inline = part.get("inlineData") or {}
            if inline.get("data"):
                return inline["data"]
    return None

def build_podcast(stories, custom_style=""):
    cfg = load_podcast_config()
    voice_alex = cfg.get("voice_alex", "Kore")
    voice_sam = cfg.get("voice_sam", "Puck")
    director_note = custom_style or cfg.get("tts_director_note", "Speak in a natural, engaging podcast conversation style:")

    s1 = stories[0]["title"] if len(stories) > 0 else "Cloud sandboxes for AI agents"
    s2 = stories[1]["title"] if len(stories) > 1 else "Open-source inference engines"
    s3 = stories[2]["title"] if len(stories) > 2 else "New battery technology"

    transcript_path = "/workspace/output/transcript.md"
    if os.path.exists(transcript_path):
        with open(transcript_path, "r", encoding="utf-8") as f:
            script_text = f.read().strip()
    else:
        script_text = f\"\"\"Alex: Good morning. Here are the three biggest technology stories readers are talking about today.
Sam: Starting at number one with {stories[0].get('score', 500)} points: {s1}.
Alex: Number two is {s2}, which already has {stories[1].get('comments', 120) if len(stories) > 1 else 120} comments from engineers comparing results.
Sam: And third on the list today is {s3}.
Alex: That is your daily summary. Thanks for listening.\"\"\"
        with open(transcript_path, "w", encoding="utf-8") as f:
            f.write(script_text)

    out_wav = "/workspace/output/podcast.wav"
    api_key = os.environ.get("GEMINI_API_KEY", "")
    tts_prompt = f"{director_note}\\n\\n{script_text}"

    for model_name in ("gemini-3.8-flash-tts", "gemini-3.8-flash-lite-tts", "gemini-3.1-flash-tts-preview"):
        for fn_name, fn in (
            ("Interactions API", try_rest_interactions_tts),
            ("generateContent API", try_rest_generate_content_tts),
        ):
            try:
                print(f"Synthesizing podcast audio with {model_name} ({fn_name}, voices={voice_alex}/{voice_sam})...")
                audio_b64 = fn(api_key, model_name, tts_prompt, voice_alex, voice_sam)
                if audio_b64:
                    raw_bytes = base64.b64decode(audio_b64)
                    if raw_bytes[:4] == b"RIFF":
                        with open(out_wav, "wb") as f:
                            f.write(raw_bytes)
                    else:
                        save_wave_pcm(out_wav, raw_bytes)
                    print(f"Saved spoken podcast audio ({len(raw_bytes)} bytes) -> {out_wav}")
                    return True
            except Exception as e:
                print(f"Attempt {model_name} via {fn_name} note: {e}")

    print("Sandbox TTS will be completed by host verifier.")
    return False

if __name__ == "__main__":
    style = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    stories = load_stories()
    build_podcast(stories, custom_style=style)
"""

# ---------------------------------------------------------------------------
# VM #4 Sandbox Helper: Live Q&A Briefing Builder (`gemini-3.8-live`)
# Mounted at `/workspace/tools/prepare_live_qa.py`
# ---------------------------------------------------------------------------
SANDBOX_LIVE_QA_SCRIPT = """#!/usr/bin/env python3
import json
import os
import subprocess
import sys

os.makedirs("/workspace/output", exist_ok=True)

def prepare_live_briefing(topic=""):
    cmd = [sys.executable, "/workspace/tools/scrape_hn.py", "--comments"]
    if topic:
        cmd.append(topic)
    subprocess.run(cmd, check=False)

    stories_path = "/workspace/output/stories.json"
    stories = []
    if os.path.exists(stories_path):
        with open(stories_path, "r", encoding="utf-8") as f:
            stories = json.load(f)

    lines = [
        "# Live Q&A Briefing: Top 5 Hacker News Stories",
        "",
    ]
    for idx, s in enumerate(stories[:5], 1):
        lines.append(f"## Story {idx}: {s.get('title', '')}")
        lines.append(f"- **Score:** {s.get('score', 0)} points")
        lines.append(f"- **Comments:** {s.get('comments', 0)} comments")
        lines.append(f"- **Author:** {s.get('by', 'reader')}")
        lines.append(f"- **Article URL:** {s.get('url', '')}")
        lines.append(f"- **Hacker News Discussion:** {s.get('hn_url', '')}")
        top_comms = s.get("top_comments") or []
        if top_comms:
            lines.append("- **What readers are saying in the comments:**")
            for c in top_comms:
                lines.append(f"  - *{c.get('by', 'reader')}*: \"{c.get('text', '')}\"")
        lines.append("")

    briefing_md = "\\n".join(lines)
    out_md = "/workspace/output/live_briefing.md"
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(briefing_md)
    print(f"Saved live Q&A briefing notes ({len(briefing_md)} chars) to {out_md}")

if __name__ == "__main__":
    topic = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    prepare_live_briefing(topic)
"""


def build_podcast_prompt(topic: str, podcast_options: Optional[Dict[str, str]] = None) -> str:
    opts = podcast_options or {}
    style_key = opts.get("style", "energetic")
    voice_alex = opts.get("voice_alex", "Kore")
    voice_sam = opts.get("voice_sam", "Puck")
    style_cfg = PODCAST_STYLE_CONFIGS.get(style_key, PODCAST_STYLE_CONFIGS["energetic"])

    return (
        f"Use your `podcast-producer` skill: first run `python3 /workspace/tools/scrape_hn.py \"{topic}\"`. "
        f"Then write a 6-line dialogue between `Alex:` and `Sam:` to `/workspace/output/transcript.md` "
        f"following this style instruction: {style_cfg['script_instruction']} "
        f"(Each line must start with `Alex:` or `Sam:` and contain plain spoken words only, with no stage directions or asterisks). "
        f"Then run `python3 /workspace/tools/generate_podcast.py` to synthesize `/workspace/output/podcast.wav` "
        f"using voices {voice_alex} and {voice_sam}. Confirm the files are saved."
    )


# ---------------------------------------------------------------------------
# Role Definitions for the 4 Parallel Managed Agents
# ---------------------------------------------------------------------------
AGENT_SPECS: Dict[str, Dict[str, Any]] = {
    "infographic": {
        "role_id": "infographic",
        "title": "VM #1: Infographic Designer Agent",
        "subtitle": "Summarizes top 3 takeaways + generates 4 infographic styles in parallel",
        "skill_path": ".agents/skills/infographic-designer/SKILL.md",
        "skill_content": (
            "---\n"
            "name: infographic-designer\n"
            "description: Scrape live tech news, summarize the top 3 takeaways, and generate 4 infographics in distinct visual styles\n"
            "---\n"
            "# Infographic Designer Skill\n"
            "1. Run `python3 /workspace/tools/scrape_hn.py` to scrape the top live stories into `/workspace/output/stories.json`.\n"
            "2. Summarize the top 3 takeaways from the headlines into `/workspace/output/takeaways.md` (preserving story titles, point scores, and comment counts).\n"
            "3. Run `python3 /workspace/tools/generate_infographic.py` to generate 4 infographics in 4 styles:\n"
            "   - `/workspace/output/infographic_style1_minimal.png` (Style 1: Minimal / Monochrome / Line Art)\n"
            "   - `/workspace/output/infographic_style2_space_retro.png` (Style 2: Space / Retro)\n"
            "   - `/workspace/output/infographic_style3_pop_art.png` (Style 3: Pop Art / Dots / Comic)\n"
            "   - `/workspace/output/infographic_style4_watercolor.png` (Style 4: Sketch / Watercolor / Travel)\n"
            "4. Verify all 4 PNG files exist in `/workspace/output/` and output the top 3 takeaways in 3 clear bullet points."
        ),
        "helper_path": "/workspace/tools/generate_infographic.py",
        "helper_content": SANDBOX_INFOGRAPHIC_SCRIPT,
        "build_prompt": lambda topic, _opts=None: (
            f"Use your `infographic-designer` skill: first run `python3 /workspace/tools/scrape_hn.py \"{topic}\"`. "
            f"Write the top 3 takeaways from the scraped headlines into `/workspace/output/takeaways.md` "
            f"(preserving exact titles, point scores, and comment counts), then run "
            f"`python3 /workspace/tools/generate_infographic.py` to generate the 4 styled infographics "
            f"(`infographic_style1_minimal.png`, `infographic_style2_space_retro.png`, "
            f"`infographic_style3_pop_art.png`, `infographic_style4_watercolor.png`). "
            f"Confirm all 4 PNG files are saved and state the top 3 takeaways clearly."
        ),
    },
    "video": {
        "role_id": "video",
        "title": "VM #2: Broadcast Video Producer Agent",
        "subtitle": "Scrapes live news + produces 16:9 MP4 broadcast reel in Linux VM",
        "skill_path": ".agents/skills/video-producer/SKILL.md",
        "skill_content": (
            "---\n"
            "name: video-producer\n"
            "description: Scrape live tech news and produce a 16:9 MP4 broadcast video reel\n"
            "---\n"
            "# Video Producer Skill\n"
            "1. Run `python3 /workspace/tools/scrape_hn.py` to fetch the top breaking tech stories into `/workspace/output/stories.json`.\n"
            "2. Run `python3 /workspace/tools/generate_video.py` to produce `/workspace/output/news_video.mp4` using Gemini Omni 1.1 Flash (`gemini-omni-1.1-flash`) or FFmpeg broadcast synthesis.\n"
            "3. Verify `/workspace/output/news_video.mp4` exists and output the broadcast storyboard summary."
        ),
        "helper_path": "/workspace/tools/generate_video.py",
        "helper_content": SANDBOX_VIDEO_SCRIPT,
        "build_prompt": lambda topic, _opts=None: (
            f"Use your `video-producer` skill to scrape today's top tech stories for '{topic}' "
            f"(run `python3 /workspace/tools/scrape_hn.py \"{topic}\"`), then run "
            f"`python3 /workspace/tools/generate_video.py` to generate `/workspace/output/news_video.mp4`. "
            f"Verify the MP4 file size in `/workspace/output/` and share the broadcast storyboard."
        ),
    },
    "podcast": {
        "role_id": "podcast",
        "title": "VM #3: Two-Host Podcast Producer Agent",
        "subtitle": "Scrapes live news + writes script & synthesizes 2-voice WAV with Gemini TTS",
        "skill_path": ".agents/skills/podcast-producer/SKILL.md",
        "skill_content": (
            "---\n"
            "name: podcast-producer\n"
            "description: Scrape live tech news, write a 2-host podcast script (Alex & Sam), and synthesize multi-speaker audio\n"
            "---\n"
            "# Podcast Producer Skill\n"
            "1. Run `python3 /workspace/tools/scrape_hn.py` to scrape the top live stories into `/workspace/output/stories.json`.\n"
            "2. Write an engaging 6-line dialogue between `Alex:` and `Sam:` discussing the top 3 stories to `/workspace/output/transcript.md`.\n"
            "3. Run `python3 /workspace/tools/generate_podcast.py` to synthesize the two-speaker podcast audio into `/workspace/output/podcast.wav`.\n"
            "4. Verify `/workspace/output/podcast.wav` exists and print the show notes."
        ),
        "helper_path": "/workspace/tools/generate_podcast.py",
        "helper_content": SANDBOX_PODCAST_SCRIPT,
        "build_prompt": build_podcast_prompt,
    },
    "live_qa": {
        "role_id": "live_qa",
        "title": "VM #4: Live Q&A Correspondent Agent",
        "subtitle": "Scrapes top 5 Hacker News stories & reader comments for live voice and text Q&A",
        "skill_path": ".agents/skills/live-correspondent/SKILL.md",
        "skill_content": (
            "---\n"
            "name: live-correspondent\n"
            "description: Scrape the top 5 Hacker News stories and top reader comments into a live briefing for interactive Q&A\n"
            "---\n"
            "# Live Correspondent Skill\n"
            "1. Run `python3 /workspace/tools/prepare_live_qa.py` to scrape the top 5 Hacker News stories and their top reader comments into `/workspace/output/stories.json` and `/workspace/output/live_briefing.md`.\n"
            "2. Read `/workspace/output/live_briefing.md` and write a clear 5-bullet overview of the top 5 stories so the reader can ask live questions."
        ),
        "helper_path": "/workspace/tools/prepare_live_qa.py",
        "helper_content": SANDBOX_LIVE_QA_SCRIPT,
        "build_prompt": lambda topic, _opts=None: (
            f"Use your `live-correspondent` skill: run `python3 /workspace/tools/prepare_live_qa.py \"{topic}\"` "
            f"to scrape the top 5 Hacker News stories and top reader comments into `/workspace/output/stories.json` "
            f"and `/workspace/output/live_briefing.md`. Then provide a concise, plain-English 1-sentence summary "
            f"of each of the 5 stories so the user is ready to ask questions live."
        ),
    },
}


def build_sandbox_environment(
    role_id: str,
    api_key: str,
    existing_env_id: Optional[str] = None,
    podcast_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Builds the `environment` config for a Gemini Managed Agent VM.
    """
    spec = AGENT_SPECS[role_id]
    env_cfg: Dict[str, Any] = {
        "type": "remote",
        "network": {
            "allowlist": [
                {
                    "domain": "generativelanguage.googleapis.com",
                    "transform": {
                        "x-goog-api-key": api_key,
                    },
                },
                {"domain": "*"},
            ]
        },
        "env": {
            "GEMINI_API_KEY": {"value": api_key},
        },
    }
    if existing_env_id:
        env_cfg["environment_id"] = existing_env_id
    else:
        sources = [
            {
                "type": "inline",
                "target": spec["skill_path"],
                "content": spec["skill_content"],
            },
            {
                "type": "inline",
                "target": "/workspace/tools/scrape_hn.py",
                "content": SANDBOX_SCRAPER_SCRIPT,
            },
            {
                "type": "inline",
                "target": spec["helper_path"],
                "content": spec["helper_content"],
            },
        ]
        if role_id == "infographic":
            sources.append(
                {
                    "type": "inline",
                    "target": "/workspace/tools/infographic_styles.json",
                    "content": json.dumps(INFOGRAPHIC_STYLES, indent=2),
                }
            )
        elif role_id == "podcast":
            opts = podcast_options or {}
            style_key = opts.get("style", "energetic")
            style_cfg = PODCAST_STYLE_CONFIGS.get(style_key, PODCAST_STYLE_CONFIGS["energetic"])
            podcast_cfg_json = json.dumps(
                {
                    "voice_alex": opts.get("voice_alex", "Kore"),
                    "voice_sam": opts.get("voice_sam", "Puck"),
                    "style": style_key,
                    "tts_director_note": style_cfg["tts_director_note"],
                },
                indent=2,
            )
            sources.append(
                {
                    "type": "inline",
                    "target": "/workspace/tools/podcast_config.json",
                    "content": podcast_cfg_json,
                }
            )
        env_cfg["sources"] = sources
    return env_cfg


def ensure_four_style_infographics(
    client: genai.Client,
    api_key: str,
    role_output_dir: Path,
    run_id: str,
    stories_data: List[Dict[str, Any]],
    takeaways_text: str,
    downloaded_files: List[Dict[str, Any]],
    emit_sync: Callable[[Dict[str, Any]], None],
) -> List[Dict[str, Any]]:
    """
    Ensures all 4 styled infographic PNGs (`infographic_style1_minimal.png`,
    `infographic_style2_space_retro.png`, `infographic_style3_pop_art.png`,
    `infographic_style4_watercolor.png`) exist in `role_output_dir`.
    Generates missing styles in parallel using `gemini-3.1-flash-image`.
    Emits an `artifact_progress` SSE event as EACH image file finishes, so they
    appear in the UI immediately without queuing.
    """
    import concurrent.futures

    role_output_dir.mkdir(parents=True, exist_ok=True)
    if not takeaways_text.strip() and stories_data:
        lines = ["Top 3 Takeaways from Today's Technology Headlines:"]
        for i, s in enumerate(stories_data[:3], 1):
            lines.append(
                f"{i}. {s.get('title', 'Story')} — {s.get('score', 0)} points, {s.get('comments', 0)} comments."
            )
        takeaways_text = "\n".join(lines)

    # Emit any already-existing styles immediately
    for st in INFOGRAPHIC_STYLES:
        fname = st["filename"]
        target = role_output_dir / fname
        if target.exists() and target.stat().st_size > 500:
            emit_sync(
                {
                    "role": "infographic",
                    "type": "artifact_progress",
                    "subtype": "infographic_ready",
                    "file": {
                        "name": fname,
                        "style_id": st["id"],
                        "style_label": st["label"],
                        "remote_path": f"/workspace/output/{fname}",
                        "size_bytes": target.stat().st_size,
                        "url": f"/outputs/{run_id}/infographic/{fname}?t={int(time.time())}",
                    },
                    "text": f"Ready: {st['label']}",
                }
            )

    missing_styles = [
        st for st in INFOGRAPHIC_STYLES if not (role_output_dir / st["filename"]).exists()
    ]

    if missing_styles:
        emit_sync(
            {
                "role": "infographic",
                "type": "step_start",
                "step_type": "image_generation",
                "detail": f"Generating {len(missing_styles)} styled infographics with gemini-nano-banana-2.1 / gemini-3.1-flash-image...",
            }
        )

        def _gen_one(st: Dict[str, str]) -> None:
            target = role_output_dir / st["filename"]
            full_prompt = (
                f"{st['prompt']}\n\n"
                f"## SUPPLIED SOURCE MATERIAL (Top 3 Takeaways from Today's Headlines):\n"
                f"{takeaways_text}"
            )
            img_b64 = None
            for img_model in IMAGE_MODELS:
                try:
                    inter = client.interactions.create(
                        model=img_model,
                        input=full_prompt,
                        response_format={"type": "image", "aspect_ratio": "4:3"},
                    )
                    if getattr(inter, "output_image", None) and getattr(inter.output_image, "data", None):
                        img_b64 = inter.output_image.data
                    else:
                        for step in getattr(inter, "steps", []) or []:
                            if getattr(step, "type", "") == "model_output":
                                for part in getattr(step, "content", []) or []:
                                    if getattr(part, "type", "") == "image" and getattr(part, "data", None):
                                        img_b64 = part.data
                                        break
                    if img_b64:
                        target.write_bytes(base64.b64decode(img_b64))
                        break
                except Exception:
                    pass

            if not target.exists() or target.stat().st_size < 500:
                for img_model in IMAGE_MODELS:
                    try:
                        gen_res = client.models.generate_content(
                            model=img_model,
                            contents=full_prompt,
                            config=types.GenerateContentConfig(response_modalities=["IMAGE"]),
                        )
                        for cand in getattr(gen_res, "candidates", []) or []:
                            content = getattr(cand, "content", None)
                            for part in getattr(content, "parts", []) or []:
                                inline = getattr(part, "inline_data", None)
                                if inline and getattr(inline, "data", None):
                                    raw_bytes = (
                                        inline.data
                                        if isinstance(inline.data, bytes)
                                        else base64.b64decode(inline.data)
                                    )
                                    target.write_bytes(raw_bytes)
                                    break
                        if target.exists() and target.stat().st_size > 500:
                            break
                    except Exception:
                        pass

            if target.exists() and target.stat().st_size > 500:
                emit_sync(
                    {
                        "role": "infographic",
                        "type": "artifact_progress",
                        "subtype": "infographic_ready",
                        "file": {
                            "name": st["filename"],
                            "style_id": st["id"],
                            "style_label": st["label"],
                            "remote_path": f"/workspace/output/{st['filename']}",
                            "size_bytes": target.stat().st_size,
                            "url": f"/outputs/{run_id}/infographic/{st['filename']}?t={int(time.time())}",
                        },
                        "text": f"Completed {st['label']}",
                    }
                )

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(_gen_one, missing_styles))

    existing_by_name = {f["name"]: f for f in downloaded_files}
    for st in INFOGRAPHIC_STYLES:
        fname = st["filename"]
        fpath = role_output_dir / fname
        if fpath.exists():
            existing_by_name[fname] = {
                "name": fname,
                "style_id": st["id"],
                "style_label": st["label"],
                "remote_path": f"/workspace/output/{fname}",
                "size_bytes": fpath.stat().st_size,
                "url": f"/outputs/{run_id}/infographic/{fname}?t={int(time.time())}",
            }

    ordered: List[Dict[str, Any]] = []
    for st in INFOGRAPHIC_STYLES:
        if st["filename"] in existing_by_name:
            entry = existing_by_name.pop(st["filename"])
            entry["style_id"] = st["id"]
            entry["style_label"] = st["label"]
            ordered.append(entry)
    ordered.extend(existing_by_name.values())
    return ordered


def synthesize_podcast_audio_wav(
    client: genai.Client,
    api_key: str,
    transcript_text: str,
    out_wav_path: Path,
    voice_alex: str = "Kore",
    voice_sam: str = "Puck",
    style_key: str = "energetic",
    single_speaker_voice: Optional[str] = None,
) -> bool:
    """
    Synthesizes spoken multi-speaker WAV audio from `transcript_text` using `gemini-3.8-flash-tts`
    (with automatic fallback to `gemini-3.1-flash-tts-preview`), saving 24kHz 16-bit mono WAV
    to `out_wav_path`.
    """
    style_cfg = PODCAST_STYLE_CONFIGS.get(style_key, PODCAST_STYLE_CONFIGS["energetic"])
    director_note = style_cfg["tts_director_note"]
    tts_prompt = f"{director_note}\n\n{transcript_text.strip()}"

    def write_audio_bytes(raw_bytes: bytes) -> None:
        out_wav_path.parent.mkdir(parents=True, exist_ok=True)
        if raw_bytes[:4] == b"RIFF":
            out_wav_path.write_bytes(raw_bytes)
        else:
            with wave.open(str(out_wav_path), "wb") as wf:
                wf.setnchannels(1)
                wf.setsampwidth(2)
                wf.setframerate(24000)
                wf.writeframes(raw_bytes)

    # 1. First choice: gemini-3.8-flash-tts via REST with speechMetadata turns
    try:
        parts = []
        for line in transcript_text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("Alex:"):
                parts.append({"text": line[5:].strip(), "speechMetadata": {"speaker": "Alex"}})
            elif line.startswith("Sam:"):
                parts.append({"text": line[4:].strip(), "speechMetadata": {"speaker": "Sam"}})
            else:
                parts.append({"text": line, "speechMetadata": {"speaker": "Alex"}})

        if parts:
            url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.8-flash-tts:generateContent"
            payload = {
                "contents": [{"parts": parts}],
                "generationConfig": {
                    "responseModalities": ["AUDIO"],
                    "speechConfig": {
                        "multiSpeakerVoiceConfig": {
                            "speakerVoiceConfigs": [
                                {"speaker": "Alex", "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice_alex}}},
                                {"speaker": "Sam", "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice_sam}}},
                            ]
                        }
                    },
                },
            }
            resp = requests.post(
                url,
                headers={"x-goog-api-key": api_key, "Content-Type": "application/json"},
                json=payload,
                timeout=75,
            )
            if resp.status_code == 200:
                body = resp.json()
                for cand in body.get("candidates") or []:
                    for p in (cand.get("content") or {}).get("parts") or []:
                        inline = p.get("inlineData") or {}
                        if inline.get("data"):
                            raw = base64.b64decode(inline["data"])
                            if len(raw) > 1000:
                                write_audio_bytes(raw)
                                return True
    except Exception:
        pass

    # 2. Second choice: gemini-3.1-flash-tts-preview via Interactions API
    speech_cfg_list = [
        {"speaker": "Alex", "voice": voice_alex},
        {"speaker": "Sam", "voice": voice_sam},
    ]
    conv_prompt = f"TTS the following conversation between Alex and Sam:\n{transcript_text.strip()}"

    for model_name in TTS_MODELS:
        try:
            interaction = client.interactions.create(
                model=model_name,
                input=conv_prompt,
                response_format={"type": "audio"},
                generation_config={"speech_config": speech_cfg_list},
            )
            audio_b64 = None
            if getattr(interaction, "output_audio", None) and getattr(interaction.output_audio, "data", None):
                audio_b64 = interaction.output_audio.data
            else:
                for step in getattr(interaction, "steps", []) or []:
                    if getattr(step, "type", "") == "model_output":
                        for part in getattr(step, "content", []) or []:
                            if getattr(part, "type", "") == "audio" and getattr(part, "data", None):
                                audio_b64 = part.data
                                break
            if audio_b64:
                raw_bytes = base64.b64decode(audio_b64)
                if len(raw_bytes) > 1000:
                    write_audio_bytes(raw_bytes)
                    return True
        except Exception:
            pass

    return False


def generate_podcast_script_for_style(
    client: genai.Client,
    api_key: str,
    stories: List[Dict[str, Any]],
    style_cfg: Dict[str, str],
) -> str:
    """
    Generates a unique 6-line multi-turn podcast script tailored specifically
    to the given style instruction and stories.
    """
    stories_summary = "\n".join(
        [
            f"{i}. {s.get('title', 'Tech Story')} ({s.get('score', 100)} points, {s.get('comments', 20)} comments)"
            for i, s in enumerate(stories[:5], 1)
        ]
    )
    prompt = (
        f"{style_cfg['script_instruction']}\n\n"
        f"## TOP STORIES:\n{stories_summary}\n\n"
        "## SCRIPT RULES:\n"
        "1. Exactly 6 lines total, strictly alternating between Alex: and Sam: (Alex speaks 3 times, Sam speaks 3 times).\n"
        "2. Every line must start with 'Alex: ' or 'Sam: '.\n"
        "3. Do not include sound effect notes, music cues, parentheticals, or bold formatting on speaker names.\n"
        "4. Focus on the actual stories with genuine character and tone for this style."
    )

    for model_name in SUPPORTED_AGENT_MODELS:
        try:
            res = client.models.generate_content(model=model_name, contents=prompt)
            if res and res.text and len(res.text.strip()) > 80:
                lines = [l.strip() for l in res.text.strip().split("\n") if l.strip()]
                parsed = [l for l in lines if l.startswith("Alex:") or l.startswith("Sam:")]
                if len(parsed) >= 4:
                    return "\n".join(parsed[:6])
        except Exception:
            pass

    s1 = stories[0]["title"] if len(stories) > 0 else "Sandboxed AI Agents"
    s2 = stories[1]["title"] if len(stories) > 1 else "High-Speed Inference Engine"
    s3 = stories[2]["title"] if len(stories) > 2 else "Solid-State Battery Breakthrough"

    if style_cfg["id"] == "energetic":
        return (
            f"Alex: Good morning tech world! We have incredible stories surging up Hacker News today!\n"
            f"Sam: Leading the board with massive enthusiasm is {s1}!\n"
            f"Alex: Developers are raving about the performance gains and real-time execution speeds.\n"
            f"Sam: And right behind it is {s2}, sparking hundreds of comments in just a few hours!\n"
            f"Alex: We also have {s3} showing game-changing hardware benchmarks.\n"
            f"Sam: Grab your coffee and stay tuned for the next briefing!"
        )
    elif style_cfg["id"] == "bedtime":
        return (
            f"Alex: Welcome to tonight's quiet reflection on the world of technology.\n"
            f"Sam: Today, thousands of engineers spent their hours pondering {s1}.\n"
            f"Alex: In another corner of open-source science, {s2} was quietly shared with the community.\n"
            f"Sam: New ideas like {s3} remind us that progress continues while we rest.\n"
            f"Alex: Let the screens dim and let your mind unwind from the code.\n"
            f"Sam: Goodnight, and rest well for tomorrow's builds."
        )
    else:  # skeptical
        return (
            f"Alex: Another day, another batch of breathless headlines on Hacker News.\n"
            f"Sam: First up is {s1}, but let's see how it actually performs outside of synthetic benchmarks.\n"
            f"Alex: Exactly. And people are already hyping {s2} before anyone has tested it in production.\n"
            f"Sam: Don't get me started on {s3} until we see independent lab verification.\n"
            f"Alex: Question the marketing claims, read the actual source code, and run your own tests.\n"
            f"Sam: We'll believe the hype when the PRs merge cleanly."
        )


def ensure_three_podcast_styles(
    client: genai.Client,
    api_key: str,
    role_output_dir: Path,
    run_id: str,
    stories_data: List[Dict[str, Any]],
    emit_sync: Callable[[Dict[str, Any]], None],
) -> List[Dict[str, Any]]:
    """
    Generates 3 distinct podcast styles (Energetic morning show, Bedtime story, Skeptical engineers)
    with different bespoke scripts and voice pairs, saving them as separate files:
    - podcast_energetic.wav & transcript_energetic.md
    - podcast_bedtime.wav & transcript_bedtime.md
    - podcast_skeptical.wav & transcript_skeptical.md
    Emits an `artifact_progress` SSE event as each file completes so they appear without queuing.
    """
    role_output_dir.mkdir(parents=True, exist_ok=True)
    all_files: List[Dict[str, Any]] = []

    for st in THREE_PODCAST_STYLES:
        st_id = st["id"]
        wav_path = role_output_dir / st["wav_file"]
        script_path = role_output_dir / st["script_file"]

        # 1. Script generation
        script_text = ""
        if script_path.exists():
            script_text = script_path.read_text(encoding="utf-8", errors="replace").strip()
        if not script_text:
            script_text = generate_podcast_script_for_style(client, api_key, stories_data, st)
            script_path.write_text(script_text, encoding="utf-8")

        # 2. Audio synthesis with gemini-3.8-flash-tts
        need_tts = not wav_path.exists() or wav_path.stat().st_size < 40000
        if need_tts:
            emit_sync(
                {
                    "role": "podcast",
                    "type": "step_start",
                    "step_type": "tts_synthesis",
                    "detail": f"Synthesizing {st['label']} audio with gemini-3.8-flash-tts ({st['voice_alex']} & {st['voice_sam']})...",
                }
            )
            synthesize_podcast_audio_wav(
                client=client,
                api_key=api_key,
                transcript_text=script_text,
                out_wav_path=wav_path,
                voice_alex=st["voice_alex"],
                voice_sam=st["voice_sam"],
                style_key=st_id,
            )

        wav_size = wav_path.stat().st_size if wav_path.exists() else 0
        script_size = script_path.stat().st_size if script_path.exists() else 0

        wav_entry = {
            "name": st["wav_file"],
            "style_id": st_id,
            "style_label": st["label"],
            "remote_path": f"/workspace/output/{st['wav_file']}",
            "size_bytes": wav_size,
            "url": f"/outputs/{run_id}/podcast/{st['wav_file']}?t={int(time.time())}",
        }
        script_entry = {
            "name": st["script_file"],
            "style_id": st_id,
            "style_label": st["label"],
            "remote_path": f"/workspace/output/{st['script_file']}",
            "size_bytes": script_size,
            "url": f"/outputs/{run_id}/podcast/{st['script_file']}?t={int(time.time())}",
        }
        all_files.extend([wav_entry, script_entry])

        emit_sync(
            {
                "role": "podcast",
                "type": "artifact_progress",
                "subtype": "podcast_ready",
                "style": st_id,
                "label": st["label"],
                "voices": f"Alex ({st['voice_alex']}) & Sam ({st['voice_sam']})",
                "wav_file": wav_entry,
                "script_file": script_entry,
                "transcript": script_text,
                "text": f"Completed podcast: {st['label']} ({st['voice_alex']} & {st['voice_sam']})",
            }
        )

    # Maintain default podcast.wav & transcript.md for backwards compatibility
    def_wav = role_output_dir / "podcast.wav"
    def_script = role_output_dir / "transcript.md"
    first_wav = role_output_dir / THREE_PODCAST_STYLES[0]["wav_file"]
    first_script = role_output_dir / THREE_PODCAST_STYLES[0]["script_file"]
    if first_wav.exists() and (not def_wav.exists() or def_wav.stat().st_size < 40000):
        shutil.copyfile(first_wav, def_wav)
    if first_script.exists() and not def_script.exists():
        shutil.copyfile(first_script, def_script)

    if def_wav.exists():
        all_files.insert(
            0,
            {
                "name": "podcast.wav",
                "remote_path": "/workspace/output/podcast.wav",
                "size_bytes": def_wav.stat().st_size,
                "url": f"/outputs/{run_id}/podcast/podcast.wav?t={int(time.time())}",
            },
        )
    return all_files


def generate_lyria_chill_house_music(
    client: genai.Client,
    api_key: str,
    out_mp3_path: Path,
    emit_sync: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> bool:
    """
    Generates subtle instrumental chill house elevator-style background music using Lyria 3.5 (`lyria-3.5`)
    and saves it to `out_mp3_path` (`chill_house_bgm.mp3`).
    """
    out_mp3_path.parent.mkdir(parents=True, exist_ok=True)
    if out_mp3_path.exists() and out_mp3_path.stat().st_size > 50000:
        return True

    if emit_sync:
        emit_sync(
            {
                "role": "video",
                "type": "step_start",
                "step_type": "music_generation",
                "detail": "Generating subtle chill house elevator background music with lyria-3.5...",
            }
        )

    lyria_prompt = (
        "Subtle instrumental chill house elevator style background music, warm Rhodes electric piano chords, "
        "soft deep sub bassline, gentle brushed hi-hats, smooth 112 BPM lounge groove, sophisticated tech newsroom "
        "underscore, no vocals, clean warm stereo mix"
    )

    for model_name in ("lyria-3.5", "lyria-3-clip-preview"):
        try:
            inter = client.interactions.create(
                model=model_name,
                input=lyria_prompt,
            )
            aud_b64 = None
            if getattr(inter, "output_audio", None) and getattr(inter.output_audio, "data", None):
                aud_b64 = inter.output_audio.data
            else:
                for step in getattr(inter, "steps", []) or []:
                    if getattr(step, "type", "") == "model_output":
                        for part in getattr(step, "content", []) or []:
                            if getattr(part, "type", "") == "audio" and getattr(part, "data", None):
                                aud_b64 = part.data
                                break
            if aud_b64:
                raw_bytes = base64.b64decode(aud_b64)
                if len(raw_bytes) > 20000:
                    out_mp3_path.write_bytes(raw_bytes)
                    return True
        except Exception:
            pass

    # Fallback warm Rhodes-style chill house synth loop via FFmpeg if network/API is unreachable
    ffmpeg_bin = shutil.which("ffmpeg")
    if not ffmpeg_bin:
        try:
            import imageio_ffmpeg
            ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            pass
    if ffmpeg_bin:
        try:
            cmd = [
                ffmpeg_bin, "-y",
                "-f", "lavfi", "-i", "sine=frequency=220:sample_rate=44100",
                "-f", "lavfi", "-i", "sine=frequency=261.63:sample_rate=44100",
                "-f", "lavfi", "-i", "sine=frequency=329.63:sample_rate=44100",
                "-f", "lavfi", "-i", "sine=frequency=392.00:sample_rate=44100",
                "-filter_complex",
                "[0:a]volume=0.18[a0];[1:a]volume=0.14[a1];[2:a]volume=0.12[a2];[3:a]volume=0.10[a3];"
                "[a0][a1][a2][a3]amix=inputs=4:duration=first,tremolo=f=1.86:d=0.45,lowpass=f=1200",
                "-t", "30",
                "-c:a", "libmp3lame", "-b:a", "192k",
                str(out_mp3_path),
            ]
            subprocess.run(cmd, capture_output=True, check=False)
            return out_mp3_path.exists()
        except Exception:
            pass
    return False


def find_or_create_infographic_reference(
    client: genai.Client,
    api_key: str,
    base_output_dir: Path,
    run_id: str,
    role_output_dir: Path,
    stories_data: List[Dict[str, Any]],
    emit_sync: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Path]:
    """
    Locates the infographic PNG(s) generated by the Infographic Agent (`base_output_dir / run_id / 'infographic'`)
    so the Video Producer Agent can use the infographic image as the video reference image (`<IMAGE_REF_0>`).
    If the Infographic Agent is currently running in parallel, polls briefly until its first PNG appears.
    Copies the primary reference image to `role_output_dir / 'reference_infographic.png'`.
    """
    role_output_dir.mkdir(parents=True, exist_ok=True)
    ref_copy_path = role_output_dir / "reference_infographic.png"
    infographic_dir = base_output_dir / run_id / "infographic"

    preferred_names = [
        "infographic_style2_space_retro.png",
        "infographic_style1_minimal.png",
        "infographic_style3_pop_art.png",
        "infographic_style4_watercolor.png",
    ]

    def _collect_existing(folder: Path) -> List[Path]:
        found: List[Path] = []
        if not folder.exists():
            return found
        for name in preferred_names:
            p = folder / name
            if p.exists() and p.stat().st_size > 5000:
                found.append(p)
        for p in sorted(folder.glob("*.png")):
            if p not in found and p.stat().st_size > 5000:
                found.append(p)
        return found

    candidates = _collect_existing(infographic_dir)
    if not candidates:
        # Poll up to 24 seconds in case VM #1 (Infographic Agent) is generating in parallel right now
        for _ in range(12):
            time.sleep(2.0)
            candidates = _collect_existing(infographic_dir)
            if candidates:
                break

    if not candidates and ref_copy_path.exists() and ref_copy_path.stat().st_size > 5000:
        candidates = [ref_copy_path]

    # Check most recent completed run directory if this run only launched `video`
    if not candidates and base_output_dir.exists():
        for d in sorted(base_output_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            if d.is_dir() and d.name.startswith("run_"):
                c = _collect_existing(d / "infographic")
                if c:
                    candidates = c
                    break

    # Final fallback: generate a reference infographic image directly
    if not candidates:
        try:
            top3 = stories_data[:3] if stories_data else [
                {"title": "Using any C++ library in Godot", "score": 120, "comments": 25},
                {"title": "Phyllotaxis: An audio-reactive LED display", "score": 240, "comments": 45},
                {"title": "The systems that no one will test", "score": 180, "comments": 38},
            ]
            takeaways_lines = ["Top 3 Takeaways from Today's Technology Headlines:"]
            for i, s in enumerate(top3, 1):
                takeaways_lines.append(f"{i}. {s.get('title', 'Story')} ({s.get('score', 100)} pts)")
            prompt_text = f"{INFOGRAPHIC_STYLES[1]['prompt']}\n\n" + "\n".join(takeaways_lines)
            for img_model in IMAGE_MODELS:
                try:
                    inter = client.interactions.create(
                        model=img_model,
                        input=prompt_text,
                        response_format={"type": "image", "aspect_ratio": "4:3"},
                    )
                    if getattr(inter, "output_image", None) and getattr(inter.output_image, "data", None):
                        ref_copy_path.write_bytes(base64.b64decode(inter.output_image.data))
                        candidates = [ref_copy_path]
                        break
                except Exception:
                    pass
        except Exception:
            pass

    if candidates:
        primary = candidates[0]
        if primary.resolve() != ref_copy_path.resolve():
            try:
                shutil.copyfile(primary, ref_copy_path)
            except Exception:
                pass
    return candidates


def synthesize_aligned_scene_narration_wav(
    client: genai.Client,
    api_key: str,
    scenes: List[Dict[str, Any]],
    full_script: str,
    out_wav_path: Path,
    total_duration: float = 25.0,
    voice_name: str = "Puck",
) -> bool:
    """
    Synthesizes spoken narration using `gemini-3.8-flash-tts` and aligns each scene's spoken words
    to the exact timestamp window (`start_sec` .. `end_sec`) of what the viewer sees in the video.
    Saves sample-accurate 24kHz 16-bit mono WAV to `out_wav_path`.
    """
    import concurrent.futures
    import struct

    out_wav_path.parent.mkdir(parents=True, exist_ok=True)
    sample_rate = 24000
    total_samples = int(total_duration * sample_rate)
    num_scenes = max(1, len(scenes))

    def _synth_scene(idx_and_scene: Any) -> Optional[bytes]:
        idx, sc = idx_and_scene
        text = (sc.get("narration") or "").strip()
        if not text:
            return None
        tmp_wav = out_wav_path.parent / f".tmp_scene_{idx}.wav"
        try:
            ok = synthesize_podcast_audio_wav(
                client=client,
                api_key=api_key,
                transcript_text=f"Alex: {text}",
                out_wav_path=tmp_wav,
                voice_alex=voice_name,
                voice_sam="Charon",
                style_key="energetic",
            )
            if ok and tmp_wav.exists() and tmp_wav.stat().st_size > 1000:
                with wave.open(str(tmp_wav), "rb") as wf:
                    pcm = wf.readframes(wf.getnframes())
                try:
                    tmp_wav.unlink()
                except Exception:
                    pass
                return pcm
        except Exception:
            pass
        return None

    scene_pcms: List[Optional[bytes]] = []
    if scenes:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            scene_pcms = list(pool.map(_synth_scene, enumerate(scenes[:4])))

    if scene_pcms and any(scene_pcms):
        master_buf = bytearray(total_samples * 2)  # 16-bit mono silence
        ffmpeg_bin = shutil.which("ffmpeg")
        if not ffmpeg_bin:
            try:
                import imageio_ffmpeg
                ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
            except Exception:
                pass

        for idx, sc in enumerate(scenes[:4]):
            pcm = scene_pcms[idx] if idx < len(scene_pcms) else None
            if not pcm:
                continue
            start_sec = float(sc.get("start_sec", idx * (total_duration / num_scenes)))
            end_sec = float(sc.get("end_sec", (idx + 1) * (total_duration / num_scenes)))
            slot_sec = max(2.0, end_sec - start_sec - 0.12)
            cur_sec = (len(pcm) // 2) / float(sample_rate)

            # If the spoken line is longer than the visual scene window, gently speed it up via ffmpeg atempo
            if cur_sec > slot_sec and ffmpeg_bin:
                tempo = min(1.45, max(1.05, cur_sec / slot_sec))
                in_tmp = out_wav_path.parent / f".tmp_in_{idx}.wav"
                out_tmp = out_wav_path.parent / f".tmp_out_{idx}.wav"
                try:
                    with wave.open(str(in_tmp), "wb") as wf:
                        wf.setnchannels(1)
                        wf.setsampwidth(2)
                        wf.setframerate(sample_rate)
                        wf.writeframes(pcm)
                    subprocess.run(
                        [
                            ffmpeg_bin, "-y", "-i", str(in_tmp),
                            "-filter:a", f"atempo={tempo:.3f}",
                            "-ar", str(sample_rate), "-ac", "1",
                            str(out_tmp),
                        ],
                        capture_output=True,
                        check=False,
                    )
                    if out_tmp.exists() and out_tmp.stat().st_size > 500:
                        with wave.open(str(out_tmp), "rb") as wf:
                            pcm = wf.readframes(wf.getnframes())
                except Exception:
                    pass
                for p in (in_tmp, out_tmp):
                    try:
                        if p.exists():
                            p.unlink()
                    except Exception:
                        pass

            start_byte = int(start_sec * sample_rate) * 2
            max_bytes = min(len(pcm), int((end_sec - start_sec) * sample_rate) * 2, len(master_buf) - start_byte)
            if max_bytes > 0 and start_byte < len(master_buf):
                master_buf[start_byte : start_byte + max_bytes] = pcm[:max_bytes]

        with wave.open(str(out_wav_path), "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(sample_rate)
            wf.writeframes(bytes(master_buf))
        return out_wav_path.exists() and out_wav_path.stat().st_size > 10000

    # Fallback: single-pass full script synthesis with gemini-3.8-flash-tts
    return synthesize_podcast_audio_wav(
        client=client,
        api_key=api_key,
        transcript_text=f"Alex: {full_script}",
        out_wav_path=out_wav_path,
        voice_alex=voice_name,
        voice_sam="Charon",
        style_key="energetic",
    )


def generate_engaging_video_prompt_and_narration(
    client: genai.Client,
    api_key: str,
    stories: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Generates a scene-synchronized 25-second video production plan that:
    1. Uses the infographic image (`<IMAGE_REF_0>`) as the reference image for `gemini-omni-1.1-flash`
       and animates its visual elements in 3D space as each story's text appears across 25 seconds.
    2. Breaks the timeline into 4 explicit scenes (`[00:00-00:06]`, `[00:06-00:12]`, `[00:12-00:19]`, `[00:19-00:25]`)
       so the spoken word from `gemini-3.8-flash-tts` aligns strictly with what the viewer sees on screen.
    3. Produces crisp 1-liner concise summaries for each story.
    """
    items = stories[:5] if stories else []
    if not items:
        items = [
            {"title": "Using any C++ library in Godot", "score": 120, "comments": 25, "url": "https://blog.conan.io"},
            {"title": "Phyllotaxis: An audio-reactive LED display", "score": 240, "comments": 45, "url": "https://jagi.studio"},
            {"title": "The systems that no one will test", "score": 180, "comments": 38, "url": "https://blog.christianperone.com"},
            {"title": "Booted up in 1993, this server still runs", "score": 350, "comments": 90, "url": "https://computerworld.com"},
        ]

    stories_text = "\n".join([
        f'{i}. "{s.get("title", f"Story {i}")}" (Score: {s.get("score", 100)}, Comments: {s.get("comments", 20)})'
        for i, s in enumerate(items[:4], 1)
    ])

    meta_prompt = f"""You are an award-winning motion graphics director creating a 25-second animated video briefing for Gemini Omni 1.1 Flash (`gemini-omni-1.1-flash`) that uses the session's infographic image (`<IMAGE_REF_0>`) as the visual reference image and animates it as text appears on screen, paired with scene-aligned `gemini-3.8-flash-tts` spoken narration and subtle `lyria-3.5` chill house background music.

CRITICAL REQUIREMENTS:
1. NEVER create static slides! The video must continuously animate the infographic reference image `<IMAGE_REF_0>` in 3D space (camera pan, parallax depth, orbiting holographic diagrams, glowing data rings, and animated kinetic text callouts that materialize as each story is introduced).
2. Structure the 25-second video into 4 timed scenes that align the visual animation, appearing text, and spoken narration:
   - Scene 1 (`0.0` to `6.25`s, `time_range`: `"00:00 - 00:06"`): Story #1
   - Scene 2 (`6.25` to `12.5`s, `time_range`: `"00:06 - 00:12"`): Story #2
   - Scene 3 (`12.5` to `18.75`s, `time_range`: `"00:12 - 00:19"`): Story #3
   - Scene 4 (`18.75` to `25.0`s, `time_range`: `"00:19 - 00:25"`): Story #4 + sign-off
3. For each of the 4 scenes, provide:
   - "title": Short punchy story title (under 45 chars)
   - "summary": Crisp 1-liner takeaway (under 14 words)
   - "visual": How `<IMAGE_REF_0>` animates in 3D space as this story's text appears
   - "narration": Exact spoken voiceover sentence for this scene (10 to 14 words so it speaks cleanly in ~5 seconds and aligns 1-to-1 with what the viewer sees in this scene).

TOP STORIES:
{stories_text}

Return a JSON object with:
- "scenes": Array of 4 scene objects, each with "time_range", "start_sec", "end_sec", "title", "summary", "visual", "narration".
- "omni_turn1_prompt": Prompt for Turn 1 (0-10s) starting with `[# References <IMAGE_REF_0>@Image1]` animating `<IMAGE_REF_0>` as Scene 1 (`[00:00 - 00:05]`) and Scene 2 (`[00:05 - 00:10]`) text and graphics appear.
- "omni_turn2_prompt": Prompt for Turn 2 (+10s = 10-20s) continuing the animated infographic motion as Scene 3 (`[00:10 - 00:15]`) and Scene 4 (`[00:15 - 00:20]`) text and graphics appear.
- "omni_turn3_prompt": Prompt for Turn 3 (+5s = 20-25s) zooming out across the living animated infographic dashboard (`[00:20 - 00:25]`).
"""

    for model_name in SUPPORTED_AGENT_MODELS:
        try:
            res = client.models.generate_content(
                model=model_name,
                contents=meta_prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )
            if res and res.text:
                data = json.loads(res.text)
                raw_scenes = data.get("scenes") or []
                t1 = (data.get("omni_turn1_prompt") or "").strip()
                t2 = (data.get("omni_turn2_prompt") or "").strip()
                t3 = (data.get("omni_turn3_prompt") or "").strip()
                if len(raw_scenes) >= 4 and t1:
                    if "<IMAGE_REF_0>" not in t1:
                        t1 = f"[# References <IMAGE_REF_0>@Image1] {t1}"
                    slot_times = [(0.0, 6.25, "00:00 - 00:06"), (6.25, 12.5, "00:06 - 00:12"), (12.5, 18.75, "00:12 - 00:19"), (18.75, 25.0, "00:19 - 00:25")]
                    scenes_clean = []
                    summaries_clean = []
                    for idx in range(4):
                        sc = raw_scenes[idx]
                        st_item = items[idx] if idx < len(items) else {}
                        s_start, s_end, t_range = slot_times[idx]
                        title_val = st_item.get("title") or sc.get("title") or f"Story {idx+1}"
                        sum_val = sc.get("summary") or "Key engineering takeaway and architectural insight."
                        narr_val = sc.get("narration") or f"In story {idx+1}, engineers explore {title_val}."
                        vis_val = sc.get("visual") or f"Infographic elements animate in 3D as kinetic text appears for {title_val}."
                        scenes_clean.append({
                            "idx": idx + 1,
                            "time_range": t_range,
                            "start_sec": s_start,
                            "end_sec": s_end,
                            "title": title_val,
                            "summary": sum_val,
                            "visual": vis_val,
                            "narration": narr_val,
                            "score": st_item.get("score", 100),
                            "comments": st_item.get("comments", 20),
                        })
                        summaries_clean.append({
                            "title": title_val,
                            "summary": sum_val,
                            "score": st_item.get("score", 100),
                            "comments": st_item.get("comments", 20),
                            "narration": narr_val,
                            "time_range": t_range,
                        })
                    for extra_s in items[4:]:
                        summaries_clean.append({
                            "title": extra_s.get("title", "Tech Story"),
                            "summary": "Community discussion and engineering benchmarks.",
                            "score": extra_s.get("score", 100),
                            "comments": extra_s.get("comments", 20),
                        })
                    full_narr = " ".join(sc["narration"] for sc in scenes_clean)
                    combined_prompt = (
                        f"Turn 1 (0-10s, with Infographic Reference <IMAGE_REF_0>):\n{t1}\n\n"
                        f"Turn 2 (10-20s Extension):\n{t2}\n\n"
                        f"Turn 3 (20-25s Extension):\n{t3}"
                    )
                    return {
                        "scenes": scenes_clean,
                        "omni_turn1_prompt": t1,
                        "omni_turn2_prompt": t2,
                        "omni_turn3_prompt": t3,
                        "omni_prompt": combined_prompt,
                        "narration_script": full_narr,
                        "summaries": summaries_clean,
                    }
        except Exception:
            pass

    # Deterministic scene-aligned fallback grounded in the current stories
    s1 = items[0].get("title", "Using any C++ library in Godot") if len(items) > 0 else "Using any C++ library in Godot"
    s2 = items[1].get("title", "Phyllotaxis: An audio-reactive LED display") if len(items) > 1 else "Phyllotaxis: An audio-reactive LED display"
    s3 = items[2].get("title", "The systems that no one will test") if len(items) > 2 else "The systems that no one will test"
    s4 = items[3].get("title", "Booted up in 1993, this server still runs") if len(items) > 3 else "Booted up in 1993, this server still runs"

    fallback_scenes = [
        {
            "idx": 1,
            "time_range": "00:00 - 00:06",
            "start_sec": 0.0,
            "end_sec": 6.25,
            "title": s1,
            "summary": "Link external C++ libraries directly into Godot with modern build tooling.",
            "visual": f"The infographic reference image <IMAGE_REF_0> awakens in 3D space with parallax camera push-in as glowing turquoise hologram panels and animated kinetic text reveal '{s1[:38]}'.",
            "narration": f"Look at the top holographic terminal: developers are now linking any C++ library directly into Godot!",
            "score": items[0].get("score", 100) if len(items) > 0 else 100,
            "comments": items[0].get("comments", 20) if len(items) > 0 else 20,
        },
        {
            "idx": 2,
            "time_range": "00:06 - 00:12",
            "start_sec": 6.25,
            "end_sec": 12.50,
            "title": s2,
            "summary": "Golden-ratio botanical geometry drives a mesmerizing audio-reactive LED spiral.",
            "visual": f"Camera pans across the living infographic collage as golden Fibonacci spirals spin and glowing waveform text animates on screen for '{s2[:38]}'.",
            "narration": f"Next, watch those spinning golden spirals ignite: Phyllotaxis turns botanical math into an audio-reactive LED display.",
            "score": items[1].get("score", 100) if len(items) > 1 else 100,
            "comments": items[1].get("comments", 20) if len(items) > 1 else 20,
        },
        {
            "idx": 3,
            "time_range": "00:12 - 00:19",
            "start_sec": 12.50,
            "end_sec": 18.75,
            "title": s3,
            "summary": "Mission-critical infrastructure often runs in production without end-to-end chaos testing.",
            "visual": f"Vibrant orange telemetry frames and 3D server racks emerge from the infographic as bold kinetic typography highlights '{s3[:38]}'.",
            "narration": f"Now our spotlight shifts to the glowing server monoliths, exposing the critical systems that no one will test.",
            "score": items[2].get("score", 100) if len(items) > 2 else 100,
            "comments": items[2].get("comments", 20) if len(items) > 2 else 20,
        },
        {
            "idx": 4,
            "time_range": "00:19 - 00:25",
            "start_sec": 18.75,
            "end_sec": 25.00,
            "title": s4,
            "summary": "A legendary 1993 server demonstrates decades of uninterrupted hardware resilience.",
            "visual": f"Vintage CRT monitors and the full animated infographic dashboard zoom out in 3D space with kinetic text celebrating '{s4[:38]}'.",
            "narration": f"And on the retro terminal below, salute a legendary 1993 server still running strong after decades!",
            "score": items[3].get("score", 100) if len(items) > 3 else 100,
            "comments": items[3].get("comments", 20) if len(items) > 3 else 20,
        },
    ]

    t1 = (
        "[# References <IMAGE_REF_0>@Image1] Bring this infographic <IMAGE_REF_0> to life as a dynamic, high-motion 3D animated technology broadcast! "
        f"[00:00 - 00:05] Smooth 3D camera push-in with parallax depth: the central figure and diagram motifs from <IMAGE_REF_0> animate in space while glowing turquoise holographic panels and bold animated text '{s1[:32].upper()}' materialize with particle trails. "
        f"[00:05 - 00:10] Camera sweeps across the animated infographic composition as golden Fibonacci spirals spin and kinetic typography '{s2[:32].upper()}' animates in with pulsing light waves."
    )
    t2 = (
        "Extend this animated infographic video for 10 more seconds with continuous 3D camera choreography and kinetic text reveals: "
        f"[00:10 - 00:15] Glowing orange HUD brackets and translucent 3D server racks emerge from the infographic background as bold animated text '{s3[:32].upper()}' slides into focus. "
        f"[00:15 - 00:20] Vintage CRT monitors and orbital rings from the infographic glow with turquoise and amber telemetry as animated text '{s4[:32].upper()}' appears in 3D space."
    )
    t3 = (
        "Extend this video by 5 seconds: smooth wide camera pull-back revealing the entire living 3D animated infographic dashboard with glowing summary cards and kinetic title 'HACKER NEWS BRIEFING'."
    )

    combined_prompt = (
        f"Turn 1 (0-10s, with Infographic Reference <IMAGE_REF_0>):\n{t1}\n\n"
        f"Turn 2 (10-20s Extension):\n{t2}\n\n"
        f"Turn 3 (20-25s Extension):\n{t3}"
    )
    full_narr = " ".join(sc["narration"] for sc in fallback_scenes)
    summaries_list = [
        {
            "title": sc["title"],
            "summary": sc["summary"],
            "score": sc["score"],
            "comments": sc["comments"],
            "narration": sc["narration"],
            "time_range": sc["time_range"],
        }
        for sc in fallback_scenes
    ]

    return {
        "scenes": fallback_scenes,
        "omni_turn1_prompt": t1,
        "omni_turn2_prompt": t2,
        "omni_turn3_prompt": t3,
        "omni_prompt": combined_prompt,
        "narration_script": full_narr,
        "summaries": summaries_list,
    }


def generate_one_liner_summaries(
    client: genai.Client,
    api_key: str,
    stories: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Produces 1-liner concise summaries for each story."""
    meta = generate_engaging_video_prompt_and_narration(client, api_key, stories)
    return meta.get("summaries", [])


def _load_pil_font(size: int, bold: bool = False) -> Any:
    from PIL import ImageFont

    candidates = (
        [
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ]
        if bold
        else [
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ]
    )
    for c in candidates:
        if os.path.exists(c):
            try:
                return ImageFont.truetype(c, size=size)
            except Exception:
                pass
    return ImageFont.load_default()


def render_scene_hud_overlay_pngs(
    scenes: List[Dict[str, Any]],
    out_dir: Path,
) -> List[Path]:
    """
    Renders 4 crisp 1280x720 RGBA transparent broadcast HUD lower-third overlay PNGs using Pillow,
    one for each timed scene so FFmpeg can animate them sliding into frame in exact lockstep with
    the `gemini-3.8-flash-tts` narration and `gemini-omni-1.1-flash` visuals.
    """
    from PIL import Image, ImageDraw

    out_dir.mkdir(parents=True, exist_ok=True)
    font_badge = _load_pil_font(16, bold=True)
    font_title = _load_pil_font(26, bold=True)
    font_summary = _load_pil_font(18, bold=False)
    font_caption = _load_pil_font(16, bold=False)

    overlay_paths: List[Path] = []
    for idx, sc in enumerate(scenes[:4]):
        img = Image.new("RGBA", (1280, 720), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        # Top-left live pill badge
        draw.rounded_rectangle([36, 24, 510, 62], radius=8, fill=(8, 12, 22, 210), outline=(64, 224, 208, 220), width=2)
        draw.ellipse([52, 37, 64, 49], fill=(255, 165, 0, 255))
        draw.text(
            (74, 34),
            f"INFOGRAPHIC MOTION REEL  •  SCENE 0{idx+1} ({sc.get('time_range', '')})",
            font=font_badge,
            fill=(255, 255, 255, 250),
        )

        # Lower-third glassmorphic broadcast card (y: 538 .. 692)
        card_x1, card_y1, card_x2, card_y2 = 36, 538, 1244, 692
        draw.rounded_rectangle(
            [card_x1, card_y1, card_x2, card_y2],
            radius=12,
            fill=(8, 13, 24, 222),
            outline=(64, 224, 208, 230) if idx % 2 == 0 else (255, 165, 0, 230),
            width=2,
        )
        # Left accent bar
        accent_rgb = (64, 224, 208, 255) if idx % 2 == 0 else (255, 165, 0, 255)
        draw.rounded_rectangle([card_x1, card_y1, card_x1 + 10, card_y2], radius=4, fill=accent_rgb)

        # Story badge & score
        score_txt = f"STORY #{idx+1}   •   ▲ {sc.get('score', 100)} PTS   •   {sc.get('comments', 20)} COMMENTS"
        draw.text((card_x1 + 26, card_y1 + 14), score_txt, font=font_badge, fill=accent_rgb)

        # Story Title
        title_str = (sc.get("title") or f"Tech Story {idx+1}")[:68]
        draw.text((card_x1 + 26, card_y1 + 40), title_str, font=font_title, fill=(255, 255, 255, 255))

        # 1-Liner Takeaway
        sum_str = "1-Liner: " + (sc.get("summary") or "")[:95]
        draw.text((card_x1 + 26, card_y1 + 78), sum_str, font=font_summary, fill=(186, 230, 253, 250))

        # Aligned Spoken Narration Caption
        narr_str = 'VOICEOVER: "' + (sc.get("narration") or "")[:104] + '"'
        draw.text((card_x1 + 26, card_y1 + 112), narr_str, font=font_caption, fill=(253, 224, 71, 245))

        # Scene progress bar along bottom edge of card
        prog_frac = float(idx + 1) / 4.0
        bar_w = int((card_x2 - card_x1 - 52) * prog_frac)
        draw.rectangle([card_x1 + 26, card_y2 - 8, card_x2 - 26, card_y2 - 4], fill=(30, 41, 59, 220))
        draw.rectangle([card_x1 + 26, card_y2 - 8, card_x1 + 26 + bar_w, card_y2 - 4], fill=accent_rgb)

        out_p = out_dir / f"hud_scene_{idx}.png"
        img.save(out_p)
        overlay_paths.append(out_p)

    return overlay_paths


def composite_synced_hud_and_mix_audio(
    raw_video_path: Path,
    out_mp4_path: Path,
    scenes: List[Dict[str, Any]],
    narration_wav: Optional[Path],
    bgm_mp3: Optional[Path],
    total_duration: float = 25.0,
) -> bool:
    """
    Takes the `gemini-omni-1.1-flash` video (`raw_video_path`), composites the 4 animated
    scene-synced lower-third text HUD overlays so the appearing text aligns 1-to-1 with
    each scene, and mixes:
      - Track 1: Scene-aligned `gemini-3.8-flash-tts` spoken narration (`volume=1.55`)
      - Track 2: Subtle `lyria-3.5` chill house elevator background music (`volume=0.18`)
      - Track 3: Subtle Omni ambient sound effects (`volume=0.08`, if present)
    """
    ffmpeg_bin = shutil.which("ffmpeg")
    if not ffmpeg_bin:
        try:
            import imageio_ffmpeg
            ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            return False

    hud_dir = out_mp4_path.parent / ".hud_overlays"
    overlay_pngs = render_scene_hud_overlay_pngs(scenes, hud_dir)

    inputs = ["-i", str(raw_video_path)]
    for p in overlay_pngs:
        inputs.extend(["-loop", "1", "-t", str(total_duration), "-i", str(p)])

    narr_idx = None
    if narration_wav and narration_wav.exists() and narration_wav.stat().st_size > 1000:
        narr_idx = 1 + len(overlay_pngs)
        inputs.extend(["-i", str(narration_wav)])

    bgm_idx = None
    if bgm_mp3 and bgm_mp3.exists() and bgm_mp3.stat().st_size > 1000:
        bgm_idx = 1 + len(overlay_pngs) + (1 if narr_idx is not None else 0)
        inputs.extend(["-stream_loop", "-1", "-t", str(total_duration), "-i", str(bgm_mp3)])

    # Build video filter chain with smooth vertical slide-in for each scene's HUD card
    v_filters = ["[0:v]scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1[v0]"]
    last_v = "[v0]"
    for idx, sc in enumerate(scenes[: len(overlay_pngs)]):
        in_idx = idx + 1
        s_start = float(sc.get("start_sec", idx * (total_duration / 4.0)))
        s_end = float(sc.get("end_sec", (idx + 1) * (total_duration / 4.0)))
        next_v = f"[v{idx+1}]"
        # Smooth 0.35s slide-up entrance at the start of each scene
        y_expr = f"if(lt(t-{s_start:.2f},0.35), (0.35-(t-{s_start:.2f}))/0.35*140, 0)"
        v_filters.append(
            f"{last_v}[{in_idx}:v]overlay=x=0:y='{y_expr}':enable='between(t,{s_start:.2f},{s_end:.2f})'{next_v}"
        )
        last_v = next_v

    # Build audio filter chain mixing Gemini 3.8 Flash TTS + Lyria 3.5 chill house BGM
    fade_out_st = max(1.0, total_duration - 1.8)
    a_filters = []
    mix_inputs = []
    if narr_idx is not None:
        a_filters.append(f"[{narr_idx}:a]volume=1.55,apad=whole_dur={total_duration:.1f}[voice]")
        mix_inputs.append("[voice]")
    if bgm_idx is not None:
        a_filters.append(
            f"[{bgm_idx}:a]volume=0.18,lowpass=f=3800,afade=t=in:st=0:d=1.2,afade=t=out:st={fade_out_st:.1f}:d=1.8[bgm]"
        )
        mix_inputs.append("[bgm]")

    if len(mix_inputs) == 2:
        a_filters.append(
            f"{''.join(mix_inputs)}amix=inputs=2:duration=first:dropout_transition=2:normalize=0,alimiter=limit=0.95[aout]"
        )
    elif len(mix_inputs) == 1:
        a_filters.append(f"{mix_inputs[0]}alimiter=limit=0.95[aout]")
    else:
        inputs.extend(["-f", "lavfi", "-i", "sine=frequency=220:sample_rate=44100"])
        synth_idx = 1 + len(overlay_pngs)
        a_filters.append(f"[{synth_idx}:a]volume=0.03,afade=t=out:st={fade_out_st:.1f}:d=1.5[aout]")

    filter_complex = "; ".join(v_filters + a_filters)
    cmd = [
        ffmpeg_bin, "-y",
        *inputs,
        "-filter_complex", filter_complex,
        "-map", last_v,
        "-map", "[aout]",
        "-c:v", "libx264", "-preset", "fast", "-crf", "20", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k",
        "-t", f"{total_duration:.2f}",
        str(out_mp4_path),
    ]
    res = subprocess.run(cmd, capture_output=True)
    return res.returncode == 0 and out_mp4_path.exists() and out_mp4_path.stat().st_size > 100000


def build_sleek_motion_reel(
    stories_summaries: List[Dict[str, Any]],
    out_path: Path,
    narration_wav: Optional[Path] = None,
    bgm_mp3: Optional[Path] = None,
    ref_images: Optional[List[Path]] = None,
    scenes: Optional[List[Dict[str, Any]]] = None,
    total_duration: float = 24.0,
) -> bool:
    """
    High-frame-rate (24 fps, 24s) animated infographic motion fallback using Pillow + FFmpeg.
    Animates the actual infographic reference PNG image(s) with continuous 3D-style Ken Burns
    pan/zoom, parallax floating infographic panels, glowing orbital particles, and synchronized
    animated scene text overlays—never static slides!
    """
    try:
        from PIL import Image, ImageDraw, ImageEnhance
    except Exception:
        return False

    ffmpeg_bin = shutil.which("ffmpeg")
    if not ffmpeg_bin:
        try:
            import imageio_ffmpeg
            ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            return False

    out_path.parent.mkdir(parents=True, exist_ok=True)
    frames_dir = Path(f"/tmp/infographic_motion_frames_{int(time.time())}")
    frames_dir.mkdir(parents=True, exist_ok=True)

    # Load infographic reference image(s)
    loaded_refs: List[Image.Image] = []
    for p in (ref_images or []):
        if p and p.exists():
            try:
                loaded_refs.append(Image.open(p).convert("RGB"))
            except Exception:
                pass
    if not loaded_refs:
        # Create a dark starry space canvas if no PNG is on disk yet
        loaded_refs = [Image.new("RGB", (1280, 960), (10, 14, 23))]

    fps = 24
    total_frames = int(total_duration * fps)
    active_scenes = scenes or []
    if not active_scenes:
        for idx, s in enumerate(stories_summaries[:4]):
            active_scenes.append({
                "idx": idx + 1,
                "time_range": f"00:{idx*6:02d} - 00:{(idx+1)*6:02d}",
                "start_sec": idx * 6.0,
                "end_sec": (idx + 1) * 6.0,
                "title": s.get("title", f"Story {idx+1}"),
                "summary": s.get("summary", ""),
                "narration": s.get("narration", s.get("summary", "")),
                "score": s.get("score", 100),
                "comments": s.get("comments", 20),
            })

    # Pre-scale reference images for fast cropping/panning
    scaled_refs = [im.resize((1600, 1200), Image.Resampling.BICUBIC) for im in loaded_refs]
    card_thumbs = [im.resize((420, 315), Image.Resampling.BICUBIC) for im in loaded_refs]

    for f_idx in range(total_frames):
        t_sec = f_idx / float(fps)
        scene_idx = min(3, int(t_sec / (total_duration / 4.0)))
        local_prog = (t_sec - scene_idx * (total_duration / 4.0)) / (total_duration / 4.0)

        base_im = scaled_refs[scene_idx % len(scaled_refs)]
        # Smooth continuous pan + zoom across the infographic artwork
        zoom = 1.0 + 0.16 * math.sin(local_prog * math.pi * 0.5)
        crop_w = int(1280 / zoom)
        crop_h = int(720 / zoom)
        pan_x = int((1600 - crop_w) * (0.25 + 0.5 * math.sin(t_sec * 0.35 + scene_idx)))
        pan_y = int((1200 - crop_h) * (0.20 + 0.5 * math.cos(t_sec * 0.28 + scene_idx)))
        pan_x = max(0, min(1600 - crop_w, pan_x))
        pan_y = max(0, min(1200 - crop_h, pan_y))

        cropped = base_im.crop((pan_x, pan_y, pan_x + crop_w, pan_y + crop_h)).resize((1280, 720), Image.Resampling.BILINEAR)
        # Slightly darken background edge for contrast while keeping vibrant infographic center
        frame = ImageEnhance.Brightness(cropped).enhance(0.82).convert("RGBA")

        # Floating parallax inset of the infographic on the right with subtle bobbing motion
        thumb = card_thumbs[scene_idx % len(card_thumbs)]
        bob_y = int(85 + 14 * math.sin(t_sec * 2.1))
        bob_x = int(815 + 10 * math.cos(t_sec * 1.6))
        draw = ImageDraw.Draw(frame)
        draw.rounded_rectangle(
            [bob_x - 6, bob_y - 6, bob_x + 426, bob_y + 321],
            radius=10,
            fill=(8, 12, 22, 200),
            outline=(64, 224, 208, 235) if scene_idx % 2 == 0 else (255, 165, 0, 235),
            width=3,
        )
        frame.paste(thumb.convert("RGBA"), (bob_x, bob_y))

        frame.convert("RGB").save(frames_dir / f"frame_{f_idx:04d}.jpg", quality=90)

    raw_temp = out_path.parent / "temp_motion_raw.mp4"
    subprocess.run(
        [
            ffmpeg_bin, "-y",
            "-framerate", str(fps),
            "-i", str(frames_dir / "frame_%04d.jpg"),
            "-c:v", "libx264", "-preset", "fast", "-crf", "20", "-pix_fmt", "yuv420p",
            "-t", f"{total_duration:.2f}",
            str(raw_temp),
        ],
        capture_output=True,
        check=False,
    )
    shutil.rmtree(frames_dir, ignore_errors=True)

    if raw_temp.exists():
        ok = composite_synced_hud_and_mix_audio(
            raw_video_path=raw_temp,
            out_mp4_path=out_path,
            scenes=active_scenes,
            narration_wav=narration_wav,
            bgm_mp3=bgm_mp3,
            total_duration=total_duration,
        )
        if ok:
            return True
        shutil.copyfile(raw_temp, out_path)
        return out_path.exists()
    return False


def ensure_omni_20s_video(
    client: genai.Client,
    api_key: str,
    role_output_dir: Path,
    run_id: str,
    stories_data: List[Dict[str, Any]],
    custom_prompt: str,
    emit_sync: Callable[[Dict[str, Any]], None],
) -> List[Dict[str, Any]]:
    """
    Produces a 20-30s (25s) animated video (`news_video.mp4`) that:
    1. Uses the infographic image from VM #1 (`reference_infographic.png` -> `<IMAGE_REF_0>`) as the
       video reference image for `gemini-omni-1.1-flash`, animating it with 3D motion & kinetic text.
    2. Narrates the video using `gemini-3.8-flash-tts` (`narration.wav`), with spoken words sample-aligned
       to each visual scene (`[00:00-00:06]`, `[00:06-00:12]`, `[00:12-00:19]`, `[00:19-00:25]`).
    3. Generates subtle chill house elevator-style background music using `lyria-3.5` (`chill_house_bgm.mp3`)
       and mixes it smoothly underneath the aligned voiceover narration.
    """
    import concurrent.futures

    role_output_dir.mkdir(parents=True, exist_ok=True)
    base_output_dir = role_output_dir.parent.parent
    out_mp4 = role_output_dir / "news_video.mp4"
    sb_path = role_output_dir / "storyboard.md"
    prompt_path = role_output_dir / "omni_prompt.txt"
    script_path = role_output_dir / "narration_script.md"
    narration_wav = role_output_dir / "narration.wav"
    bgm_mp3 = role_output_dir / "chill_house_bgm.mp3"
    ref_img_path = role_output_dir / "reference_infographic.png"
    marker_file = role_output_dir / "omni_ref_v2.json"

    # 1. Locate (or wait briefly for) the infographic image from the Infographic Agent
    ref_images = find_or_create_infographic_reference(
        client=client,
        api_key=api_key,
        base_output_dir=base_output_dir,
        run_id=run_id,
        role_output_dir=role_output_dir,
        stories_data=stories_data,
        emit_sync=emit_sync,
    )

    # 2. Generate scene-aligned Omni prompts, spoken narration script, and 1-liner summaries
    meta = generate_engaging_video_prompt_and_narration(client, api_key, stories_data)
    scenes = meta["scenes"]
    omni_turn1_prompt = custom_prompt.strip() if custom_prompt.strip() else meta["omni_turn1_prompt"]
    if "<IMAGE_REF_0>" not in omni_turn1_prompt:
        omni_turn1_prompt = f"[# References <IMAGE_REF_0>@Image1] {omni_turn1_prompt}"
    omni_turn2_prompt = meta["omni_turn2_prompt"]
    omni_turn3_prompt = meta["omni_turn3_prompt"]
    omni_prompt = meta["omni_prompt"] if not custom_prompt.strip() else omni_turn1_prompt
    narration_script = meta["narration_script"]
    summaries = meta["summaries"]

    prompt_path.write_text(omni_prompt, encoding="utf-8")

    # Format scene-aligned narration script with timestamps
    script_lines = []
    for sc in scenes:
        script_lines.append(f"[{sc['time_range']}] {sc['narration']}")
    formatted_script = "\n".join(script_lines) if script_lines else narration_script
    script_path.write_text(formatted_script, encoding="utf-8")

    # 3. Build comprehensive storyboard markdown showing scene-by-scene alignment
    sb_lines = [
        "# Top Hacker News Stories — 25s Infographic-Referenced Video Storyboard",
        "",
        "Animated with **Gemini Omni 1.1 Flash (`gemini-omni-1.1-flash`)** using the session's infographic (`<IMAGE_REF_0>`) as the visual reference image, scene-aligned **Gemini 3.8 Flash TTS (`gemini-3.8-flash-tts`)** narration, and subtle **Lyria 3.5 (`lyria-3.5`)** chill house elevator background music.",
        "",
        "## 🎙️ Scene-Aligned Spoken Narration Script (`gemini-3.8-flash-tts`)",
    ]
    for sc in scenes:
        sb_lines.append(f"> **[{sc['time_range']}]** {sc['narration']}")
    sb_lines.extend([
        "",
        "## 🎵 Background Music (`lyria-3.5`)",
        "> Subtle instrumental chill house elevator-style groove (warm Rhodes chords, soft sub bassline, brushed hi-hats @ 112 BPM)",
        "",
        "## 🎬 Gemini Omni 1.1 Flash Prompt (with Infographic `<IMAGE_REF_0>` Reference)",
        "```text",
        omni_prompt,
        "```",
        "",
        "## 📌 Top Stories — Scene Timeline & 1-Liner Concise Summaries",
        "",
    ])
    for idx, sc in enumerate(scenes, 1):
        sb_lines.append(f"{idx}. **{sc['title']}** (▲ {sc.get('score', 100)} pts · {sc.get('comments', 20)} comments · `{sc['time_range']}`)")
        sb_lines.append(f"   - *1-Liner Takeaway:* {sc['summary']}")
        sb_lines.append(f"   - *Aligned Voiceover:* \"{sc['narration']}\"")
        sb_lines.append("")
    sb_text = "\n".join(sb_lines)
    sb_path.write_text(sb_text, encoding="utf-8")

    # Determine if we need to regenerate (overwrite any small/static VM fallback MP4 < 2.5 MB or missing marker)
    need_video = (
        not out_mp4.exists()
        or out_mp4.stat().st_size < 2500000
        or not marker_file.exists()
        or bool(custom_prompt.strip())
    )

    if need_video:
        emit_sync(
            {
                "role": "video",
                "type": "step_start",
                "step_type": "video_generation",
                "detail": "Animating infographic reference (<IMAGE_REF_0>) with gemini-omni-1.1-flash (25s), gemini-3.8-flash-tts aligned narration, and lyria-3.5 chill house music...",
            }
        )

        def _task_tts() -> bool:
            emit_sync(
                {
                    "role": "video",
                    "type": "step_start",
                    "step_type": "tts_synthesis",
                    "detail": "Synthesizing scene-aligned narration with gemini-3.8-flash-tts (Puck)...",
                }
            )
            return synthesize_aligned_scene_narration_wav(
                client=client,
                api_key=api_key,
                scenes=scenes,
                full_script=narration_script,
                out_wav_path=narration_wav,
                total_duration=25.0,
                voice_name="Puck",
            )

        def _task_bgm() -> bool:
            return generate_lyria_chill_house_music(
                client=client,
                api_key=api_key,
                out_mp3_path=bgm_mp3,
                emit_sync=emit_sync,
            )

        def _task_omni() -> Optional[Tuple[Path, float]]:
            try:
                omni_client = genai.Client(
                    api_key=api_key,
                    http_options=types.HttpOptions(timeout=600000),
                )
                input_payload: Any = omni_turn1_prompt
                primary_ref = ref_img_path if ref_img_path.exists() else (ref_images[0] if ref_images else None)
                if primary_ref and primary_ref.exists():
                    up_file = omni_client.files.upload(file=str(primary_ref))
                    input_payload = [
                        {"type": "image", "uri": up_file.uri, "mime_type": "image/png"},
                        {"type": "text", "text": omni_turn1_prompt},
                    ]

                inter1 = omni_client.interactions.create(
                    model="gemini-omni-1.1-flash",
                    input=input_payload,
                    response_format={"type": "video", "aspect_ratio": "16:9"},
                )
                vid_b64 = None
                achieved_dur = 10.0
                if getattr(inter1, "output_video", None) and getattr(inter1.output_video, "data", None):
                    vid_b64 = inter1.output_video.data

                if getattr(inter1, "id", None):
                    try:
                        emit_sync(
                            {
                                "role": "video",
                                "type": "step_start",
                                "step_type": "video_generation",
                                "detail": "Extending animated infographic video to 20s with gemini-omni-1.1-flash...",
                            }
                        )
                        inter2 = omni_client.interactions.create(
                            model="gemini-omni-1.1-flash",
                            previous_interaction_id=inter1.id,
                            input=omni_turn2_prompt,
                            response_format={"type": "video", "aspect_ratio": "16:9"},
                        )
                        if getattr(inter2, "output_video", None) and getattr(inter2.output_video, "data", None):
                            vid_b64 = inter2.output_video.data
                            achieved_dur = 20.0

                        if getattr(inter2, "id", None):
                            try:
                                emit_sync(
                                    {
                                        "role": "video",
                                        "type": "step_start",
                                        "step_type": "video_generation",
                                        "detail": "Extending animated infographic video to 25s finale with gemini-omni-1.1-flash...",
                                    }
                                )
                                inter3 = omni_client.interactions.create(
                                    model="gemini-omni-1.1-flash",
                                    previous_interaction_id=inter2.id,
                                    input=omni_turn3_prompt,
                                    response_format={"type": "video", "aspect_ratio": "16:9", "duration": "5s"},
                                )
                                if getattr(inter3, "output_video", None) and getattr(inter3.output_video, "data", None):
                                    vid_b64 = inter3.output_video.data
                                    achieved_dur = 25.0
                            except Exception:
                                pass
                    except Exception:
                        pass

                if vid_b64:
                    temp_omni = role_output_dir / "temp_omni_raw.mp4"
                    temp_omni.write_bytes(base64.b64decode(vid_b64))
                    return temp_omni, achieved_dur
            except Exception:
                pass
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            fut_tts = pool.submit(_task_tts)
            fut_bgm = pool.submit(_task_bgm)
            fut_omni = pool.submit(_task_omni)

            fut_tts.result()
            fut_bgm.result()
            omni_res = fut_omni.result()

        video_ok = False
        if omni_res is not None:
            temp_omni_path, achieved_dur = omni_res
            # If Omni returned 20s instead of 25s, re-align scenes & narration to 20s
            if abs(achieved_dur - 25.0) > 1.0:
                for idx, sc in enumerate(scenes[:4]):
                    sc["start_sec"] = idx * (achieved_dur / 4.0)
                    sc["end_sec"] = (idx + 1) * (achieved_dur / 4.0)
                synthesize_aligned_scene_narration_wav(
                    client=client,
                    api_key=api_key,
                    scenes=scenes,
                    full_script=narration_script,
                    out_wav_path=narration_wav,
                    total_duration=achieved_dur,
                    voice_name="Puck",
                )
            video_ok = composite_synced_hud_and_mix_audio(
                raw_video_path=temp_omni_path,
                out_mp4_path=out_mp4,
                scenes=scenes,
                narration_wav=narration_wav,
                bgm_mp3=bgm_mp3,
                total_duration=achieved_dur,
            )
            if not video_ok and temp_omni_path.exists():
                shutil.copyfile(temp_omni_path, out_mp4)
                video_ok = True

        if not video_ok:
            build_sleek_motion_reel(
                stories_summaries=summaries,
                out_path=out_mp4,
                narration_wav=narration_wav,
                bgm_mp3=bgm_mp3,
                ref_images=ref_images,
                scenes=scenes,
                total_duration=24.0,
            )

        if out_mp4.exists() and out_mp4.stat().st_size > 100000:
            marker_file.write_text(
                json.dumps({"completed_at": int(time.time()), "size_bytes": out_mp4.stat().st_size}),
                encoding="utf-8",
            )

    files_list = []
    if out_mp4.exists():
        ts = int(time.time())
        vid_entry = {
            "name": "news_video.mp4",
            "remote_path": "/workspace/output/news_video.mp4",
            "size_bytes": out_mp4.stat().st_size,
            "url": f"/outputs/{run_id}/video/news_video.mp4?t={ts}",
        }
        sb_entry = {
            "name": "storyboard.md",
            "remote_path": "/workspace/output/storyboard.md",
            "size_bytes": sb_path.stat().st_size if sb_path.exists() else 0,
            "url": f"/outputs/{run_id}/video/storyboard.md?t={ts}",
        }
        narr_entry = {
            "name": "narration.wav",
            "remote_path": "/workspace/output/narration.wav",
            "size_bytes": narration_wav.stat().st_size if narration_wav.exists() else 0,
            "url": f"/outputs/{run_id}/video/narration.wav?t={ts}",
        }
        bgm_entry = {
            "name": "chill_house_bgm.mp3",
            "remote_path": "/workspace/output/chill_house_bgm.mp3",
            "size_bytes": bgm_mp3.stat().st_size if bgm_mp3.exists() else 0,
            "url": f"/outputs/{run_id}/video/chill_house_bgm.mp3?t={ts}",
        }
        ref_entry = {
            "name": "reference_infographic.png",
            "remote_path": "/workspace/output/reference_infographic.png",
            "size_bytes": ref_img_path.stat().st_size if ref_img_path.exists() else 0,
            "url": f"/outputs/{run_id}/video/reference_infographic.png?t={ts}",
        }
        script_entry = {
            "name": "narration_script.md",
            "remote_path": "/workspace/output/narration_script.md",
            "size_bytes": script_path.stat().st_size if script_path.exists() else 0,
            "url": f"/outputs/{run_id}/video/narration_script.md?t={ts}",
        }
        prompt_entry = {
            "name": "omni_prompt.txt",
            "remote_path": "/workspace/output/omni_prompt.txt",
            "size_bytes": prompt_path.stat().st_size if prompt_path.exists() else 0,
            "url": f"/outputs/{run_id}/video/omni_prompt.txt?t={ts}",
        }
        files_list = [vid_entry, sb_entry, narr_entry, bgm_entry, ref_entry, script_entry, prompt_entry]

        emit_sync(
            {
                "role": "video",
                "type": "artifact_progress",
                "subtype": "video_ready",
                "file": vid_entry,
                "narration_file": narr_entry,
                "bgm_file": bgm_entry,
                "ref_image_file": ref_entry,
                "storyboard_file": sb_entry,
                "script_file": script_entry,
                "prompt_file": prompt_entry,
                "storyboard": sb_text,
                "narration_script": formatted_script,
                "omni_prompt": omni_prompt,
                "summaries": summaries,
                "scenes": scenes,
                "text": "Completed 25s infographic-referenced Omni video with aligned Gemini 3.8 Flash TTS narration & Lyria 3.5 chill house music",
            }
        )
    return files_list




def download_sandbox_outputs(
    client: genai.Client,
    api_key: str,
    environment_id: str,
    role_id: str,
    output_dir: Path,
) -> List[Dict[str, Any]]:
    """
    Downloads generated files from `/workspace/output` inside the remote Linux VM
    using the Environments Files API (`client.environments.files`).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded_files: List[Dict[str, Any]] = []

    candidate_paths = [
        "workspace/output/infographic_style1_minimal.png",
        "workspace/output/infographic_style2_space_retro.png",
        "workspace/output/infographic_style3_pop_art.png",
        "workspace/output/infographic_style4_watercolor.png",
        "workspace/output/takeaways.md",
        "workspace/output/infographic.png",
        "workspace/output/data_chart.png",
        "workspace/output/news_video.mp4",
        "workspace/output/storyboard.md",
        "workspace/output/podcast.wav",
        "workspace/output/transcript.md",
        "workspace/output/podcast_energetic.wav",
        "workspace/output/transcript_energetic.md",
        "workspace/output/podcast_bedtime.wav",
        "workspace/output/transcript_bedtime.md",
        "workspace/output/podcast_skeptical.wav",
        "workspace/output/transcript_skeptical.md",
        "workspace/output/live_briefing.md",
        "workspace/output/stories.json",
    ]


    discovered_paths: List[str] = []
    try:
        listing = client.environments.files.list(
            environment=environment_id,
            path="workspace/output",
            recursive=True,
        )
        for f_entry in getattr(listing, "files", []) or []:
            if getattr(f_entry, "type", "FILE") == "FILE" and getattr(f_entry, "path", None):
                discovered_paths.append(f_entry.path)
    except Exception:
        pass

    paths_to_try = discovered_paths if discovered_paths else candidate_paths

    for remote_path in paths_to_try:
        filename = os.path.basename(remote_path)
        local_file = output_dir / filename
        try:
            raw_bytes = client.environments.files.download(
                environment=environment_id,
                path=remote_path,
            )
            if raw_bytes:
                local_file.write_bytes(raw_bytes)
                downloaded_files.append(
                    {
                        "name": filename,
                        "remote_path": f"/{remote_path.lstrip('/')}",
                        "size_bytes": len(raw_bytes),
                        "url": f"/outputs/{output_dir.parent.name}/{role_id}/{filename}?t={int(time.time())}",
                    }
                )
        except Exception:
            try:
                url = f"https://generativelanguage.googleapis.com/v1beta/environments/{environment_id}/files/{remote_path.lstrip('/')}?alt=media"
                resp = requests.get(url, headers={"x-goog-api-key": api_key}, timeout=30)
                if resp.status_code == 200 and resp.content:
                    local_file.write_bytes(resp.content)
                    downloaded_files.append(
                        {
                            "name": filename,
                            "remote_path": f"/{remote_path.lstrip('/')}",
                            "size_bytes": len(resp.content),
                            "url": f"/outputs/{output_dir.parent.name}/{role_id}/{filename}?t={int(time.time())}",
                        }
                    )
            except Exception:
                continue

    return downloaded_files


def _run_single_agent_sync(
    role_id: str,
    prompt: str,
    api_key: str,
    run_id: str,
    base_output_dir: Path,
    emit_sync: Callable[[Dict[str, Any]], None],
    previous_interaction_id: Optional[str] = None,
    existing_environment_id: Optional[str] = None,
    podcast_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Runs a single Managed Agent interaction with `stream=True` inside a worker thread,
    pushing real-time SSE events back to the asyncio queue and downloading the VM artifacts when done.
    """
    client = genai.Client(api_key=api_key)
    spec = AGENT_SPECS[role_id]

    if existing_environment_id:
        # Sync latest helper scripts/configs into the existing VM so regeneration uses current options
        try:
            client.environments.files.upload(
                environment=existing_environment_id,
                path=spec["helper_path"].lstrip("/"),
                file=spec["helper_content"].encode("utf-8"),
            )
            if role_id == "infographic":
                client.environments.files.upload(
                    environment=existing_environment_id,
                    path="workspace/tools/infographic_styles.json",
                    file=json.dumps(INFOGRAPHIC_STYLES, indent=2).encode("utf-8"),
                )
            elif role_id == "podcast":
                opts = podcast_options or {}
                style_key = opts.get("style", "energetic")
                style_cfg = PODCAST_STYLE_CONFIGS.get(style_key, PODCAST_STYLE_CONFIGS["energetic"])
                podcast_cfg_json = json.dumps(
                    {
                        "voice_alex": opts.get("voice_alex", "Kore"),
                        "voice_sam": opts.get("voice_sam", "Puck"),
                        "style": style_key,
                        "tts_director_note": style_cfg["tts_director_note"],
                    },
                    indent=2,
                )
                client.environments.files.upload(
                    environment=existing_environment_id,
                    path="workspace/tools/podcast_config.json",
                    file=podcast_cfg_json.encode("utf-8"),
                )
        except Exception:
            pass

    env_config = build_sandbox_environment(
        role_id=role_id,
        api_key=api_key,
        existing_env_id=existing_environment_id,
        podcast_options=podcast_options,
    )

    emit_sync(
        {
            "role": role_id,
            "type": "vm_boot",
            "message": (
                f"Reusing existing Linux VM ({existing_environment_id})..."
                if existing_environment_id
                else "Provisioning isolated 4-vCPU / 16GB Ubuntu Linux VM & mounting SKILL.md..."
            ),
        }
    )

    create_kwargs: Dict[str, Any] = {
        "agent": MANAGED_AGENT_ID,
        "input": prompt,
        "environment": env_config,
        "stream": True,
    }
    if previous_interaction_id:
        create_kwargs["previous_interaction_id"] = previous_interaction_id

    interaction_id: Optional[str] = None
    environment_id: Optional[str] = existing_environment_id
    current_step_type: Optional[str] = None
    final_text_parts: List[str] = []
    total_tokens: int = 0

    try:
        try:
            stream = client.interactions.create(**create_kwargs)
        except Exception:
            if existing_environment_id:
                emit_sync(
                    {
                        "role": role_id,
                        "type": "vm_boot",
                        "message": "Previous VM expired or unavailable; provisioning fresh Linux VM...",
                    }
                )
                environment_id = None
                create_kwargs["environment"] = build_sandbox_environment(
                    role_id=role_id,
                    api_key=api_key,
                    existing_env_id=None,
                    podcast_options=podcast_options,
                )
                create_kwargs.pop("previous_interaction_id", None)
                stream = client.interactions.create(**create_kwargs)
            else:
                raise

        for event in stream:
            ev_type = getattr(event, "event_type", "")

            if ev_type == "interaction.created":
                inter = getattr(event, "interaction", None)
                if inter:
                    interaction_id = getattr(inter, "id", None) or interaction_id
                    environment_id = getattr(inter, "environment_id", None) or environment_id
                emit_sync(
                    {
                        "role": role_id,
                        "type": "interaction_created",
                        "interaction_id": interaction_id,
                        "environment_id": environment_id,
                    }
                )

            elif ev_type == "step.start":
                step = getattr(event, "step", None)
                current_step_type = getattr(step, "type", "unknown") if step else "unknown"
                step_detail = ""
                if step:
                    if hasattr(step, "name") and getattr(step, "name", None):
                        step_detail = f"{step.name}"
                    elif hasattr(step, "arguments") and getattr(step, "arguments", None):
                        step_detail = str(step.arguments)[:160]
                emit_sync(
                    {
                        "role": role_id,
                        "type": "step_start",
                        "step_type": current_step_type,
                        "detail": step_detail,
                    }
                )

            elif ev_type == "step.delta":
                delta = getattr(event, "delta", None)
                if not delta:
                    continue
                d_type = getattr(delta, "type", "")
                if d_type == "thought_summary" or (current_step_type == "thought" and hasattr(delta, "text")):
                    text_val = getattr(delta, "text", None) or getattr(delta, "summary", None) or ""
                    if text_val:
                        emit_sync(
                            {
                                "role": role_id,
                                "type": "thought_delta",
                                "text": text_val,
                            }
                        )
                elif d_type == "text" or hasattr(delta, "text"):
                    text_val = getattr(delta, "text", "") or ""
                    if text_val:
                        if current_step_type == "model_output":
                            final_text_parts.append(text_val)
                        emit_sync(
                            {
                                "role": role_id,
                                "type": "text_delta",
                                "step_type": current_step_type or "model_output",
                                "text": text_val,
                            }
                        )
                else:
                    raw_dump = str(delta)
                    if len(raw_dump) > 3:
                        emit_sync(
                            {
                                "role": role_id,
                                "type": "tool_delta",
                                "step_type": current_step_type or d_type,
                                "text": raw_dump[:300],
                            }
                        )

            elif ev_type == "step.stop":
                usage = getattr(event, "usage", None)
                if usage and getattr(usage, "total_tokens", None):
                    total_tokens = usage.total_tokens

            elif ev_type == "interaction.completed":
                inter = getattr(event, "interaction", None)
                if inter:
                    interaction_id = getattr(inter, "id", None) or interaction_id
                    environment_id = getattr(inter, "environment_id", None) or environment_id
                    if getattr(inter, "output_text", None) and not final_text_parts:
                        final_text_parts.append(inter.output_text)
                    usage = getattr(inter, "usage", None)
                    if usage and getattr(usage, "total_tokens", None):
                        total_tokens = usage.total_tokens

        if interaction_id and not environment_id:
            try:
                full_inter = client.interactions.get(id=interaction_id)
                environment_id = getattr(full_inter, "environment_id", None)
                if getattr(full_inter, "output_text", None) and not final_text_parts:
                    final_text_parts.append(full_inter.output_text)
            except Exception:
                pass

        role_output_dir = base_output_dir / run_id / role_id
        downloaded_files: List[Dict[str, Any]] = []
        if environment_id:
            emit_sync(
                {
                    "role": role_id,
                    "type": "downloading_files",
                    "environment_id": environment_id,
                    "message": f"Downloading generated artifacts from VM {environment_id} (/workspace/output)...",
                }
            )
            downloaded_files = download_sandbox_outputs(
                client=client,
                api_key=api_key,
                environment_id=environment_id,
                role_id=role_id,
                output_dir=role_output_dir,
            )

        transcript_text = ""
        takeaways_text = ""
        live_briefing_text = ""
        stories_data = []
        for f_info in downloaded_files:
            f_path = role_output_dir / f_info["name"]
            if f_info["name"] == "transcript.md" and f_path.exists():
                transcript_text = f_path.read_text(encoding="utf-8", errors="replace")
            elif f_info["name"] == "takeaways.md" and f_path.exists():
                takeaways_text = f_path.read_text(encoding="utf-8", errors="replace")
            elif f_info["name"] == "live_briefing.md" and f_path.exists():
                live_briefing_text = f_path.read_text(encoding="utf-8", errors="replace")
            elif f_info["name"] == "stories.json" and f_path.exists():
                try:
                    stories_data = json.loads(f_path.read_text(encoding="utf-8"))
                except Exception:
                    pass

        if role_id == "infographic":
            downloaded_files = ensure_four_style_infographics(
                client=client,
                api_key=api_key,
                role_output_dir=role_output_dir,
                run_id=run_id,
                stories_data=stories_data,
                takeaways_text=takeaways_text or "".join(final_text_parts).strip(),
                downloaded_files=downloaded_files,
                emit_sync=emit_sync,
            )

        storyboard_text = ""
        narration_script_text = ""
        omni_prompt_text = ""
        if role_id == "video":
            video_files = ensure_omni_20s_video(
                client=client,
                api_key=api_key,
                role_output_dir=role_output_dir,
                run_id=run_id,
                stories_data=stories_data,
                custom_prompt="",
                emit_sync=emit_sync,
            )
            downloaded_files = video_files if video_files else downloaded_files
            sb_p = role_output_dir / "storyboard.md"
            if sb_p.exists():
                storyboard_text = sb_p.read_text(encoding="utf-8", errors="replace").strip()
            ns_p = role_output_dir / "narration_script.md"
            if ns_p.exists():
                narration_script_text = ns_p.read_text(encoding="utf-8", errors="replace").strip()
            op_p = role_output_dir / "omni_prompt.txt"
            if op_p.exists():
                omni_prompt_text = op_p.read_text(encoding="utf-8", errors="replace").strip()

        podcast_styles_dict = {}
        if role_id == "podcast":
            podcast_files = ensure_three_podcast_styles(
                client=client,
                api_key=api_key,
                role_output_dir=role_output_dir,
                run_id=run_id,
                stories_data=stories_data,
                emit_sync=emit_sync,
            )
            downloaded_files = podcast_files if podcast_files else downloaded_files
            for st in THREE_PODCAST_STYLES:
                w_p = role_output_dir / st["wav_file"]
                s_p = role_output_dir / st["script_file"]
                if w_p.exists():
                    podcast_styles_dict[st["id"]] = {
                        "id": st["id"],
                        "label": st["label"],
                        "voices": f"Alex ({st['voice_alex']}) & Sam ({st['voice_sam']})",
                        "wav_file": {
                            "name": st["wav_file"],
                            "style_id": st["id"],
                            "style_label": st["label"],
                            "remote_path": f"/workspace/output/{st['wav_file']}",
                            "size_bytes": w_p.stat().st_size,
                            "url": f"/outputs/{run_id}/podcast/{st['wav_file']}?t={int(time.time())}",
                        },
                        "script_file": {
                            "name": st["script_file"],
                            "style_id": st["id"],
                            "style_label": st["label"],
                            "remote_path": f"/workspace/output/{st['script_file']}",
                            "size_bytes": s_p.stat().st_size if s_p.exists() else 0,
                            "url": f"/outputs/{run_id}/podcast/{st['script_file']}?t={int(time.time())}",
                        },
                        "transcript": s_p.read_text(encoding="utf-8", errors="replace").strip() if s_p.exists() else "",
                    }

        result_payload = {
            "role": role_id,
            "type": "agent_completed",
            "interaction_id": interaction_id,
            "environment_id": environment_id,
            "output_text": "".join(final_text_parts).strip(),
            "takeaways": takeaways_text.strip(),
            "storyboard": storyboard_text,
            "narration_script": narration_script_text,
            "omni_prompt": omni_prompt_text,
            "podcast_styles": podcast_styles_dict,
            "total_tokens": total_tokens,
            "files": downloaded_files,
            "transcript": transcript_text,
            "live_briefing": live_briefing_text,
            "stories": stories_data,
        }
        emit_sync(result_payload)
        return result_payload

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
        emit_sync(
            {
                "role": role_id,
                "type": "agent_error",
                "error": err_msg,
                "interaction_id": interaction_id,
                "environment_id": environment_id,
            }
        )
        return {"role": role_id, "type": "agent_error", "error": err_msg}


async def run_parallel_newsroom(
    topic: str,
    api_key: str,
    run_id: str,
    base_output_dir: Path,
    event_callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
    roles: Optional[List[str]] = None,
    podcast_options: Optional[Dict[str, str]] = None,
    existing_environments: Optional[Dict[str, Optional[str]]] = None,
    previous_interactions: Optional[Dict[str, Optional[str]]] = None,
) -> Dict[str, Any]:
    """
    Launches Managed Agents (`infographic`, `video`, `podcast`, `live_qa`) in parallel
    using `asyncio.to_thread` and streams multiplexed SSE events back to the caller.
    Can optionally reuse `existing_environments` and `previous_interactions` when regenerating.
    """
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue()
    target_roles = roles or ["infographic", "video", "podcast", "live_qa"]
    env_map = existing_environments or {}
    inter_map = previous_interactions or {}

    def make_emitter() -> Callable[[Dict[str, Any]], None]:
        def _emit(item: Dict[str, Any]) -> None:
            loop.call_soon_threadsafe(queue.put_nowait, item)
        return _emit

    async def forward_events() -> None:
        while True:
            item = await queue.get()
            if item is None:
                break
            await event_callback(item)

    forward_task = asyncio.create_task(forward_events())

    tasks = [
        asyncio.to_thread(
            _run_single_agent_sync,
            role_id,
            AGENT_SPECS[role_id]["build_prompt"](topic, podcast_options),
            api_key,
            run_id,
            base_output_dir,
            make_emitter(),
            inter_map.get(role_id),
            env_map.get(role_id),
            podcast_options if role_id == "podcast" else None,
        )
        for role_id in target_roles
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    await queue.put(None)
    await forward_task

    return {
        "run_id": run_id,
        "topic": topic,
        "results": [r if isinstance(r, dict) else {"error": str(r)} for r in results],
    }


async def run_followup_in_vm(
    role_id: str,
    followup_prompt: str,
    previous_interaction_id: Optional[str],
    environment_id: Optional[str],
    api_key: str,
    run_id: str,
    base_output_dir: Path,
    event_callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
    podcast_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Continues a multi-turn conversation inside the SAME Linux VM (`environment_id`)
    and conversation history (`previous_interaction_id`).
    """
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue()

    def _emit(item: Dict[str, Any]) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, item)

    async def forward_events() -> None:
        while True:
            item = await queue.get()
            if item is None:
                break
            await event_callback(item)

    forward_task = asyncio.create_task(forward_events())
    res = await asyncio.to_thread(
        _run_single_agent_sync,
        role_id,
        followup_prompt,
        api_key,
        run_id,
        base_output_dir,
        _emit,
        previous_interaction_id,
        environment_id,
        podcast_options,
    )
    await queue.put(None)
    await forward_task
    return res


def reconstruct_session_from_disk(base_output_dir: Path, run_id: str) -> Optional[Dict[str, Any]]:
    """
    Loads `session.json` for `run_id`, or reconstructs a full session object from any existing
    files in `static/outputs/<run_id>/` so refreshing the browser always restores the session.
    """
    run_dir = base_output_dir / run_id
    if not run_dir.exists() or not run_dir.is_dir():
        return None

    session_file = run_dir / "session.json"
    session_data: Dict[str, Any] = {
        "run_id": run_id,
        "created_at": int(run_dir.stat().st_mtime),
        "topic": "Top Hacker News",
        "podcast_options": {
            "style": "energetic",
            "voice_alex": "Kore",
            "voice_sam": "Puck",
        },
        "agents": {},
        "stories": [],
        "qa_history": [],
        "last_qa_interaction_id": None,
    }
    if session_file.exists():
        try:
            loaded = json.loads(session_file.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                session_data.update(loaded)
        except Exception:
            pass

    style_meta = {s["filename"]: (s["id"], s["label"]) for s in INFOGRAPHIC_STYLES}
    session_stories: List[Dict[str, Any]] = list(session_data.get("stories") or [])

    for role_id in ("infographic", "video", "podcast", "live_qa"):
        role_dir = run_dir / role_id
        agent_state = session_data["agents"].get(role_id, {})
        files_list: List[Dict[str, Any]] = []

        if role_dir.exists() and role_dir.is_dir():
            for fpath in sorted(role_dir.iterdir()):
                if (
                    fpath.is_file()
                    and not fpath.name.startswith(".")
                    and not fpath.name.startswith("temp_")
                    and not fpath.name.startswith("omni_ref_")
                ):
                    f_entry: Dict[str, Any] = {
                        "name": fpath.name,
                        "remote_path": f"/workspace/output/{fpath.name}",
                        "size_bytes": fpath.stat().st_size,
                        "url": f"/outputs/{run_id}/{role_id}/{fpath.name}?t={int(fpath.stat().st_mtime)}",
                    }
                    if fpath.name in style_meta:
                        f_entry["style_id"], f_entry["style_label"] = style_meta[fpath.name]
                    files_list.append(f_entry)

        # Sort infographic style files in 1..4 order if present
        if role_id == "infographic" and files_list:
            by_name = {f["name"]: f for f in files_list}
            ordered = []
            for st in INFOGRAPHIC_STYLES:
                if st["filename"] in by_name:
                    ordered.append(by_name.pop(st["filename"]))
            ordered.extend(by_name.values())
            files_list = ordered

        takeaways_text = agent_state.get("takeaways", "")
        transcript_text = agent_state.get("transcript", "")
        output_text = agent_state.get("output_text", "")
        stories_data = agent_state.get("stories", [])

        if role_dir.exists():
            if not takeaways_text and (role_dir / "takeaways.md").exists():
                takeaways_text = (role_dir / "takeaways.md").read_text(encoding="utf-8", errors="replace").strip()
            if not transcript_text and (role_dir / "transcript.md").exists():
                transcript_text = (role_dir / "transcript.md").read_text(encoding="utf-8", errors="replace").strip()
            if not stories_data and (role_dir / "stories.json").exists():
                try:
                    stories_data = json.loads((role_dir / "stories.json").read_text(encoding="utf-8"))
                except Exception:
                    pass
            if stories_data and len(stories_data) > len(session_stories):
                session_stories = stories_data[:5]
            if role_id == "live_qa" and not output_text and (role_dir / "live_briefing.md").exists():
                output_text = (role_dir / "live_briefing.md").read_text(encoding="utf-8", errors="replace").strip()
            if role_id == "infographic" and not takeaways_text and stories_data:
                lines = []
                for idx, s in enumerate(stories_data[:3], 1):
                    lines.append(f"{idx}. {s.get('title', '')} ({s.get('score', 0)} points · {s.get('comments', 0)} comments)")
                takeaways_text = "\n".join(lines)

        storyboard_text = agent_state.get("storyboard", "")
        narration_script = agent_state.get("narration_script", "")
        omni_prompt = agent_state.get("omni_prompt", "")
        podcast_styles = agent_state.get("podcast_styles") or {}
        if role_dir.exists():
            if role_id == "video":
                if not storyboard_text and (role_dir / "storyboard.md").exists():
                    storyboard_text = (role_dir / "storyboard.md").read_text(encoding="utf-8", errors="replace").strip()
                if not narration_script and (role_dir / "narration_script.md").exists():
                    narration_script = (role_dir / "narration_script.md").read_text(encoding="utf-8", errors="replace").strip()
                if not omni_prompt and (role_dir / "omni_prompt.txt").exists():
                    omni_prompt = (role_dir / "omni_prompt.txt").read_text(encoding="utf-8", errors="replace").strip()
            if role_id == "podcast":
                for st in THREE_PODCAST_STYLES:
                    wav_p = role_dir / st["wav_file"]
                    script_p = role_dir / st["script_file"]
                    if wav_p.exists():
                        s_text = script_p.read_text(encoding="utf-8", errors="replace").strip() if script_p.exists() else ""
                        podcast_styles[st["id"]] = {
                            "id": st["id"],
                            "label": st["label"],
                            "voices": f"Alex ({st['voice_alex']}) & Sam ({st['voice_sam']})",
                            "wav_file": {
                                "name": st["wav_file"],
                                "style_id": st["id"],
                                "style_label": st["label"],
                                "remote_path": f"/workspace/output/{st['wav_file']}",
                                "size_bytes": wav_p.stat().st_size,
                                "url": f"/outputs/{run_id}/podcast/{st['wav_file']}?t={int(wav_p.stat().st_mtime)}",
                            },
                            "script_file": {
                                "name": st["script_file"],
                                "style_id": st["id"],
                                "style_label": st["label"],
                                "remote_path": f"/workspace/output/{st['script_file']}",
                                "size_bytes": script_p.stat().st_size if script_p.exists() else 0,
                                "url": f"/outputs/{run_id}/podcast/{st['script_file']}?t={int(script_p.stat().st_mtime if script_p.exists() else 0)}",
                            },
                            "transcript": s_text,
                        }

        has_artifacts = bool(files_list or output_text or takeaways_text or transcript_text or storyboard_text or narration_script or omni_prompt or podcast_styles)
        status = agent_state.get("status") or ("Finished" if has_artifacts else "Not started")
        logs = agent_state.get("logs") or (
            f"Restored saved outputs from {run_id}/{role_id}." if has_artifacts else "Waiting for briefing to start…"
        )

        session_data["agents"][role_id] = {
            "role": role_id,
            "status": status,
            "interaction_id": agent_state.get("interaction_id"),
            "environment_id": agent_state.get("environment_id"),
            "output_text": output_text,
            "takeaways": takeaways_text,
            "storyboard": storyboard_text,
            "narration_script": narration_script,
            "omni_prompt": omni_prompt,
            "podcast_styles": podcast_styles,
            "transcript": transcript_text,
            "files": files_list if files_list else agent_state.get("files", []),
            "logs": logs,
        }

    session_data["stories"] = [
        {
            "id": s.get("id"),
            "title": s.get("title", ""),
            "url": s.get("url", ""),
            "hn_url": s.get("hn_url", f"https://news.ycombinator.com/item?id={s.get('id', '')}"),
            "score": s.get("score", 0),
            "comments": s.get("comments", 0),
            "by": s.get("by", ""),
        }
        for s in session_stories[:5]
    ]

    return session_data


def load_latest_hn_context(base_output_dir: Path, run_id: Optional[str] = None) -> str:
    """
    Loads the `live_briefing.md` or `stories.json` produced by the 4th Managed Agent (`live_qa`),
    or fetches the current top 5 Hacker News stories if no run has completed yet.
    """
    candidate_runs: List[str] = []
    if run_id:
        candidate_runs.append(run_id)
    if base_output_dir.exists():
        for d in sorted(base_output_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            if d.is_dir() and d.name.startswith("run_") and d.name not in candidate_runs:
                candidate_runs.append(d.name)

    for rid in candidate_runs:
        briefing_file = base_output_dir / rid / "live_qa" / "live_briefing.md"
        if briefing_file.exists():
            return briefing_file.read_text(encoding="utf-8", errors="replace")
        for role in ("live_qa", "podcast", "infographic", "video"):
            stories_file = base_output_dir / rid / role / "stories.json"
            if stories_file.exists():
                try:
                    stories = json.loads(stories_file.read_text(encoding="utf-8"))
                    return "\n".join(
                        [
                            f"{i+1}. {s.get('title')} ({s.get('score')} points, {s.get('comments')} comments) - {s.get('url')}"
                            for i, s in enumerate(stories[:5])
                        ]
                    )
                except Exception:
                    pass

    # Fallback: fetch live top 5 stories from Hacker News API right now
    try:
        url = "https://hacker-news.firebaseio.com/v0/topstories.json"
        req = urllib.request.Request(url, headers={"User-Agent": "GeminiManagedAgent/1.0"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            ids = json.loads(resp.read().decode("utf-8"))[:6]
        lines = []
        for idx, sid in enumerate(ids[:5], 1):
            item_url = f"https://hacker-news.firebaseio.com/v0/item/{sid}.json"
            with urllib.request.urlopen(item_url, timeout=4) as r:
                item = json.loads(r.read().decode("utf-8"))
            if item and "title" in item:
                lines.append(
                    f"{idx}. {item['title']} ({item.get('score', 0)} points, {item.get('descendants', 0)} comments) - "
                    f"{item.get('url', f'https://news.ycombinator.com/item?id={sid}')}"
                )
        return "\n".join(lines)
    except Exception as e:
        return f"Top 5 Hacker News stories (fallback): {e}"

