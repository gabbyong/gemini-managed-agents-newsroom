# Technology News Briefing — Parallel Gemini Managed Agents Demo

A live, interactive multi-agent newsroom built with **Gemini Managed Agents (`antigravity-preview-09-2026`)**, **Gemini 4 Argon (`gemini-4-argon`)**, **Gemini 3.8 Flash (`gemini-3.8-flash`)**, **Gemini Omni 1.1 Flash (`gemini-omni-1.1-flash`)**, **Gemini 3.8 Flash TTS (`gemini-3.8-flash-tts`)**, **Lyria 3.5 (`lyria-3.5`)**, **Nano Banana 2.1 / 2 (`gemini-nano-banana-2.1` / `gemini-3.1-flash-image`)**, and the **Gemini 3.8 Live API (`gemini-3.8-live` / `gemini-3.8-live-extended-thinking`)**.

With **one click**, the app spins up **4 isolated Ubuntu Linux VMs in parallel** (each with 4 vCPUs, 16 GB RAM, Python 3.12, and `google-genai`) to scrape live Hacker News stories and simultaneously produce:

1. **VM #1 — 4-Style Infographic Designer Agent:**
   - Summarizes the top 3 takeaways and generates **4 distinct visual infographic styles in parallel** (`infographic_style1_minimal.png`, `infographic_style2_space_retro.png`, `infographic_style3_pop_art.png`, `infographic_style4_watercolor.png`) using `gemini-nano-banana-2.1` / `gemini-3.1-flash-image`, streaming each image to the UI as soon as it finishes.
2. **VM #2 — 25s Infographic-Referenced Video Producer Agent:**
   - Uses `gemini-4-argon` / `gemini-3.8-flash` to plan a 4-scene storyboard and uses the session's infographic image (`<IMAGE_REF_0>`) as the reference image for **Gemini Omni 1.1 Flash (`gemini-omni-1.1-flash`)** to animate a 25-second 3D motion graphics reel as story headlines and 1-liner takeaways appear on screen.
   - Synthesizes scene-aligned spoken narration using **Gemini 3.8 Flash TTS (`gemini-3.8-flash-tts`)** locked to each visual scene (`00:00–00:06`, `00:06–00:12`, `00:12–00:19`, `00:19–00:25`).
   - Generates subtle chill house elevator-style background music with **Lyria 3.5 (`lyria-3.5`)** and mixes it underneath the voiceover.
3. **VM #3 — 3-Style Two-Host Podcast Producer Agent:**
   - Writes 3 bespoke two-host (`Alex` & `Sam`) scripts with `gemini-4-argon` / `gemini-3.8-flash` and synthesizes 3 separate multi-speaker podcast episodes in parallel using **Gemini 3.8 Flash TTS (`gemini-3.8-flash-tts`)**:
     - ⚡ **Energetic morning show** (`Kore` & `Puck`)
     - 🌙 **Bedtime story** (`Sulafat` & `Enceladus`)
     - 🧐 **Skeptical engineers** (`Fenrir` & `Zephyr`)
4. **VM #4 — Live Voice Q&A Correspondent Agent:**
   - Scrapes the top 5 Hacker News stories and top reader comments into `/workspace/output/live_briefing.md` to ground a real-time, full-duplex **Gemini 3.8 Live (`gemini-3.8-live` / `gemini-3.8-live-extended-thinking`)** voice correspondent that you can speak with and interrupt at any time.

---

## Official Gemini API Models Used

Verified against [`https://ai.google.dev/gemini-api/docs/models/gemini#model-variations`](https://ai.google.dev/gemini-api/docs/models/gemini#model-variations):

| Capability | Model ID(s) | Release Status |
|---|---|---|
| **Managed Agent (Remote VM)** | `antigravity-preview-09-2026` | Public Preview |
| **Frontier Reasoning & Planning** | `gemini-4-argon`, `gemini-3.8-flash`, `gemini-3.6-flash`, `gemini-3.5-flash-lite` | Stable |
| **Image Generation (Infographics)** | `gemini-nano-banana-2.1`, `gemini-3.1-flash-image` | Stable |
| **Video Generation & Editing** | `gemini-omni-1.1-flash` | Stable |
| **Multi-Speaker & Scene TTS** | `gemini-3.8-flash-tts`, `gemini-3.8-flash-lite-tts` | Stable |
| **Background Music Generation** | `lyria-3.5`, `lyria-3-clip-preview` | Stable / Preview |
| **Real-Time Bidirectional Voice** | `gemini-3.8-live`, `gemini-3.8-live-extended-thinking` | Stable |

---

## Quickstart

```bash
git clone https://github.com/gabbyong/gemini-managed-agents-newsroom.git
cd gemini-managed-agents-newsroom

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Start the server
uvicorn app:app --reload --port 8080
```

Open **`http://127.0.0.1:8080`** in your browser and paste your **Gemini API key** directly into the **Gemini API key** field at the top of the page (or set `export GEMINI_API_KEY="your-key"` in your terminal).

---

## Key Managed Agents Capabilities Demonstrated

1. **Zero-Infra Parallel Sandboxes (`environment={"type": "remote"}`):**
   Provision 4 independent Linux VMs concurrently with a single `client.interactions.create(agent="antigravity-preview-09-2026", ...)` call per role.
2. **Declarative Skill & Tool Mounting (`environment.sources`):**
   Each VM boots with its own `.agents/skills/<role>/SKILL.md` and Python helper scripts mounted directly into `/workspace`.
3. **Zero-Leak Egress Proxy Credentials (`network.allowlist.transform`):**
   When code inside a sandboxed VM calls `generativelanguage.googleapis.com`, the `x-goog-api-key` header is injected by the egress proxy.
4. **Sandbox File Extraction & 7-Day Resumable VMs:**
   Generated media artifacts are downloaded via `client.environments.files.download()`, and follow-up prompts reuse the existing `environment_id` and `previous_interaction_id`.
