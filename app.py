"""
FastAPI Web Server for the Parallel Gemini Managed Agents Tech Newsroom Demo.

Run with:
    .venv/bin/uvicorn app:app --reload --port 8080
"""

import asyncio
import base64
import json
import os
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from google import genai
from google.genai import types

from newsroom_agents import (
    AGENT_SPECS,
    MANAGED_AGENT_ID,
    load_latest_hn_context,
    reconstruct_session_from_disk,
    run_followup_in_vm,
    run_parallel_newsroom,
    synthesize_podcast_audio_wav,
)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
OUTPUTS_DIR = STATIC_DIR / "outputs"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Technology News Briefing — Parallel Gemini Managed Agents Demo")

app.mount("/outputs", StaticFiles(directory=str(OUTPUTS_DIR)), name="outputs")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# In-memory cache of session manifests backed by `static/outputs/<run_id>/session.json`
SESSIONS_CACHE: Dict[str, Dict[str, Any]] = {}
CACHED_API_KEY_FILE = BASE_DIR / ".cached_api_key"


def resolve_api_key(provided_key: Optional[str] = None) -> str:
    """
    Resolves the Gemini API key from the request, environment, or local cache file.
    Whenever a valid key is provided, caches it locally so page refreshes and live voice
    sessions always have the key ready.
    """
    key = (provided_key or "").strip()
    if key:
        os.environ["GEMINI_API_KEY"] = key
        try:
            CACHED_API_KEY_FILE.write_text(key, encoding="utf-8")
        except Exception:
            pass
        return key

    env_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    if env_key:
        return env_key

    if CACHED_API_KEY_FILE.exists():
        try:
            cached = CACHED_API_KEY_FILE.read_text(encoding="utf-8").strip()
            if cached:
                os.environ["GEMINI_API_KEY"] = cached
                return cached
        except Exception:
            pass
    return ""


def get_or_load_session(run_id: str) -> Optional[Dict[str, Any]]:
    if run_id in SESSIONS_CACHE and SESSIONS_CACHE[run_id].get("stories"):
        return SESSIONS_CACHE[run_id]
    reconstructed = reconstruct_session_from_disk(OUTPUTS_DIR, run_id)
    if reconstructed:
        SESSIONS_CACHE[run_id] = reconstructed
        save_session(run_id, reconstructed)
    return reconstructed


def save_session(run_id: str, session_data: Dict[str, Any]) -> None:
    SESSIONS_CACHE[run_id] = session_data
    run_dir = OUTPUTS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    session_file = run_dir / "session.json"
    try:
        session_file.write_text(json.dumps(session_data, indent=2), encoding="utf-8")
    except Exception:
        pass


def init_new_session(
    run_id: str,
    topic: str,
    podcast_options: Dict[str, str],
) -> Dict[str, Any]:
    session_data: Dict[str, Any] = {
        "run_id": run_id,
        "created_at": int(time.time()),
        "topic": topic,
        "podcast_options": podcast_options,
        "stories": [],
        "agents": {
            role_id: {
                "role": role_id,
                "status": "Starting virtual machine…",
                "interaction_id": None,
                "environment_id": None,
                "output_text": "",
                "takeaways": "",
                "transcript": "",
                "files": [],
                "logs": "",
            }
            for role_id in ("infographic", "video", "podcast", "live_qa")
        },
        "qa_history": [],
        "last_qa_interaction_id": None,
    }
    save_session(run_id, session_data)
    return session_data


def update_session_from_event(run_id: str, ev: Dict[str, Any]) -> None:
    session_data = get_or_load_session(run_id)
    if not session_data:
        session_data = init_new_session(
            run_id=run_id,
            topic=ev.get("topic", "Top Hacker News"),
            podcast_options=ev.get("podcast_options") or {
                "style": "energetic",
                "voice_alex": "Kore",
                "voice_sam": "Puck",
            },
        )

    ev_type = ev.get("type")
    if ev_type == "qa_answer":
        session_data.setdefault("qa_history", []).append(
            {
                "question": ev.get("question", ""),
                "answer": ev.get("answer", ""),
                "audio_url": ev.get("audio_url"),
                "thinking_level": ev.get("thinking_level", "medium"),
            }
        )
        if ev.get("qa_interaction_id"):
            session_data["last_qa_interaction_id"] = ev["qa_interaction_id"]
        save_session(run_id, session_data)
        return

    role = ev.get("role")
    if not role or role not in ("infographic", "video", "podcast", "live_qa"):
        return

    agent_entry = session_data.setdefault("agents", {}).setdefault(
        role,
        {
            "role": role,
            "status": "Working…",
            "interaction_id": None,
            "environment_id": None,
            "output_text": "",
            "takeaways": "",
            "transcript": "",
            "files": [],
            "logs": "",
        },
    )

    def _append_log(line: str) -> None:
        if not line:
            return
        cur = agent_entry.get("logs") or ""
        if cur == "Waiting for briefing to start…":
            cur = ""
        agent_entry["logs"] = (cur + line + "\n")[-16000:]

    if ev_type == "vm_boot":
        agent_entry["status"] = "Starting virtual machine…"
        _append_log(ev.get("message", ""))
    elif ev_type == "interaction_created":
        if ev.get("interaction_id"):
            agent_entry["interaction_id"] = ev["interaction_id"]
        if ev.get("environment_id"):
            agent_entry["environment_id"] = ev["environment_id"]
        agent_entry["status"] = "Working…"
    elif ev_type == "step_start":
        step_type = ev.get("step_type", "")
        detail = ev.get("detail", "")
        readable = {
            "thought": "Planning next step…",
            "code_execution_call": "Running Python in virtual machine…",
            "google_search_call": "Searching the web…",
            "url_context_call": "Reading web page…",
            "function_call": "Running tool in virtual machine…",
            "image_generation": "Generating 4 styled infographics…",
            "video_generation": "Animating infographic with gemini-omni-1.1-flash (25s)…",
            "music_generation": "Generating chill house BGM with lyria-3.5…",
            "tts_synthesis": "Synthesizing scene-aligned audio with gemini-3.8-flash-tts…",
            "model_output": "Writing summary…",
        }
        agent_entry["status"] = readable.get(step_type, "Working…")
        if detail:
            _append_log(f"[{step_type}] {detail}")
    elif ev_type in ("thought_delta", "text_delta"):
        _append_log(ev.get("text", ""))
    elif ev_type == "downloading_files":
        if ev.get("environment_id"):
            agent_entry["environment_id"] = ev["environment_id"]
        agent_entry["status"] = "Downloading files from virtual machine…"
        _append_log(ev.get("message", ""))
    elif ev_type == "artifact_progress":
        subtype = ev.get("subtype")
        if subtype == "infographic_ready":
            f = ev.get("file")
            if f:
                existing = agent_entry.setdefault("files", [])
                updated = False
                for idx, item in enumerate(existing):
                    if item.get("name") == f.get("name"):
                        existing[idx] = f
                        updated = True
                        break
                if not updated:
                    existing.append(f)
        elif subtype == "video_ready":
            for k in ("file", "storyboard_file", "narration_file", "bgm_file", "ref_image_file", "script_file", "prompt_file"):
                f = ev.get(k)
                if f:
                    existing = agent_entry.setdefault("files", [])
                    updated = False
                    for idx, item in enumerate(existing):
                        if item.get("name") == f.get("name"):
                            existing[idx] = f
                            updated = True
                            break
                    if not updated:
                        existing.append(f)
            if ev.get("storyboard"):
                agent_entry["storyboard"] = ev["storyboard"]
            if ev.get("narration_script"):
                agent_entry["narration_script"] = ev["narration_script"]
            if ev.get("omni_prompt"):
                agent_entry["omni_prompt"] = ev["omni_prompt"]
        elif subtype == "podcast_ready":
            st_key = ev.get("style", "energetic")
            pod_styles = agent_entry.setdefault("podcast_styles", {})
            pod_styles[st_key] = {
                "id": st_key,
                "label": ev.get("label", st_key),
                "voices": ev.get("voices", ""),
                "wav_file": ev.get("wav_file"),
                "script_file": ev.get("script_file"),
                "transcript": ev.get("transcript", ""),
            }
            for k in ("wav_file", "script_file"):
                f = ev.get(k)
                if f:
                    existing = agent_entry.setdefault("files", [])
                    updated = False
                    for idx, item in enumerate(existing):
                        if item.get("name") == f.get("name"):
                            existing[idx] = f
                            updated = True
                            break
                    if not updated:
                        existing.append(f)
        if ev.get("text"):
            _append_log(f"[{subtype or 'artifact'}] {ev['text']}")
    elif ev_type == "agent_completed":
        if ev.get("interaction_id"):
            agent_entry["interaction_id"] = ev["interaction_id"]
        if ev.get("environment_id"):
            agent_entry["environment_id"] = ev["environment_id"]
        agent_entry["status"] = "Ready for questions" if role == "live_qa" else "Finished"
        agent_entry["output_text"] = ev.get("output_text", "")
        agent_entry["takeaways"] = ev.get("takeaways", "")
        agent_entry["transcript"] = ev.get("transcript", "")
        if ev.get("storyboard"):
            agent_entry["storyboard"] = ev["storyboard"]
        if ev.get("narration_script"):
            agent_entry["narration_script"] = ev["narration_script"]
        if ev.get("omni_prompt"):
            agent_entry["omni_prompt"] = ev["omni_prompt"]
        if ev.get("podcast_styles"):
            agent_entry["podcast_styles"] = ev["podcast_styles"]
        if ev.get("files"):
            agent_entry["files"] = ev.get("files", [])
        raw_stories = ev.get("stories") or []
        if raw_stories and len(raw_stories) >= len(session_data.get("stories") or []):
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
                for s in raw_stories[:5]
            ]
    elif ev_type == "agent_error":
        agent_entry["status"] = "Failed"
        _append_log(f"Error: {ev.get('error', '')}")

    save_session(run_id, session_data)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/config")
async def get_config() -> JSONResponse:
    has_env_key = bool(resolve_api_key())
    return JSONResponse(
        {
            "has_env_key": has_env_key,
            "managed_agent_id": MANAGED_AGENT_ID,
            "agents": [
                {
                    "role_id": k,
                    "title": v["title"],
                    "subtitle": v["subtitle"],
                    "skill_path": v["skill_path"],
                    "skill_content": v["skill_content"],
                }
                for k, v in AGENT_SPECS.items()
            ],
        }
    )


@app.post("/api/save-key")
async def save_key_endpoint(payload: Dict[str, Any]) -> JSONResponse:
    key = resolve_api_key(payload.get("api_key"))
    return JSONResponse({"saved": bool(key)})


@app.get("/api/sessions")
async def list_sessions() -> JSONResponse:
    """
    Lists all saved briefing sessions on disk (`static/outputs/run_*`) and returns the
    most recent session with artifacts so the page can automatically restore after a refresh.
    """
    run_dirs: List[Path] = []
    if OUTPUTS_DIR.exists():
        for d in OUTPUTS_DIR.iterdir():
            if d.is_dir() and d.name.startswith("run_"):
                run_dirs.append(d)
    run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    sessions_summary: List[Dict[str, Any]] = []
    best_session: Optional[Dict[str, Any]] = None

    for d in run_dirs[:15]:
        sess = get_or_load_session(d.name)
        if not sess:
            continue
        agents = sess.get("agents", {})
        completed_count = sum(
            1
            for r in ("infographic", "video", "podcast", "live_qa")
            if agents.get(r, {}).get("files") or agents.get(r, {}).get("output_text")
        )
        sessions_summary.append(
            {
                "run_id": sess["run_id"],
                "created_at": sess.get("created_at", int(d.stat().st_mtime)),
                "topic": sess.get("topic", "Top Hacker News"),
                "completed_count": completed_count,
                "stories": sess.get("stories", []),
            }
        )
        # Prefer the newest session that has all 4 roles completed
        if best_session is None:
            best_session = sess
        elif (
            sum(
                1
                for r in ("infographic", "video", "podcast", "live_qa")
                if best_session.get("agents", {}).get(r, {}).get("files")
                or best_session.get("agents", {}).get(r, {}).get("output_text")
            )
            < 4
            and completed_count == 4
        ):
            best_session = sess

    return JSONResponse(
        {
            "sessions": sessions_summary,
            "latest_session": best_session,
        }
    )


@app.get("/api/session/{run_id}")
async def get_session_by_id(run_id: str) -> JSONResponse:
    sess = get_or_load_session(run_id)
    if not sess:
        return JSONResponse({"error": f"Session {run_id} not found"}, status_code=404)
    return JSONResponse({"session": sess})


@app.get("/api/hn-preview")
async def hn_preview() -> JSONResponse:
    """Fetches a quick preview of the current top 5 Hacker News stories for the page."""
    try:
        url = "https://hacker-news.firebaseio.com/v0/topstories.json"
        req = urllib.request.Request(url, headers={"User-Agent": "GeminiManagedAgentDemo/1.0"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            ids = json.loads(resp.read().decode("utf-8"))[:6]
        stories = []
        for sid in ids:
            item_url = f"https://hacker-news.firebaseio.com/v0/item/{sid}.json"
            with urllib.request.urlopen(item_url, timeout=4) as r:
                item = json.loads(r.read().decode("utf-8"))
            if item and "title" in item:
                stories.append(
                    {
                        "id": sid,
                        "title": item["title"],
                        "score": item.get("score", 0),
                        "comments": item.get("descendants", 0),
                        "url": item.get("url", f"https://news.ycombinator.com/item?id={sid}"),
                    }
                )
        return JSONResponse({"stories": stories[:5]})
    except Exception as e:
        return JSONResponse({"stories": [], "error": str(e)})


@app.websocket("/ws/newsroom")
async def newsroom_ws(ws: WebSocket) -> None:
    await ws.accept()
    try:
        while True:
            raw = await ws.receive_text()
            payload = json.loads(raw)
            action = payload.get("action")
            api_key = resolve_api_key(payload.get("api_key"))

            if not api_key:
                await ws.send_json(
                    {
                        "type": "global_error",
                        "error": "Enter your Gemini API key above or set GEMINI_API_KEY in your terminal.",
                    }
                )
                continue

            if action == "launch_parallel":
                topic = (payload.get("topic") or "Top Hacker News").strip()
                roles = payload.get("roles") or ["infographic", "video", "podcast", "live_qa"]
                podcast_options = payload.get("podcast_options") or {
                    "style": "energetic",
                    "voice_alex": "Kore",
                    "voice_sam": "Puck",
                }
                run_id = payload.get("run_id") or f"run_{int(time.time())}"
                reuse_vms = bool(payload.get("reuse_vms", False))

                existing_envs: Dict[str, Optional[str]] = {}
                prev_inters: Dict[str, Optional[str]] = {}
                if reuse_vms:
                    existing_sess = get_or_load_session(run_id)
                    if existing_sess:
                        for r in roles:
                            ag = existing_sess.get("agents", {}).get(r, {})
                            if ag.get("environment_id"):
                                existing_envs[r] = ag["environment_id"]
                            if ag.get("interaction_id"):
                                prev_inters[r] = ag["interaction_id"]
                    else:
                        init_new_session(run_id, topic, podcast_options)
                else:
                    init_new_session(run_id, topic, podcast_options)

                async def send_and_record(ev: Dict[str, Any]) -> None:
                    update_session_from_event(run_id, ev)
                    try:
                        await ws.send_json(ev)
                    except Exception:
                        pass

                await ws.send_json(
                    {
                        "type": "run_started",
                        "run_id": run_id,
                        "topic": topic,
                        "roles": roles,
                        "podcast_options": podcast_options,
                    }
                )
                await run_parallel_newsroom(
                    topic=topic,
                    api_key=api_key,
                    run_id=run_id,
                    base_output_dir=OUTPUTS_DIR,
                    event_callback=send_and_record,
                    roles=roles,
                    podcast_options=podcast_options,
                    existing_environments=existing_envs if reuse_vms else None,
                    previous_interactions=prev_inters if reuse_vms else None,
                )
                await ws.send_json({"type": "run_finished", "run_id": run_id})

            elif action == "regenerate_role":
                role_id = payload["role"]
                topic = (payload.get("topic") or "Top Hacker News").strip()
                run_id = payload.get("run_id") or f"run_{int(time.time())}"
                podcast_options = payload.get("podcast_options") or {
                    "style": "energetic",
                    "voice_alex": "Kore",
                    "voice_sam": "Puck",
                }
                env_id = payload.get("environment_id")
                prev_id = payload.get("previous_interaction_id")

                existing_sess = get_or_load_session(run_id)
                if not existing_sess:
                    init_new_session(run_id, topic, podcast_options)
                else:
                    if not env_id:
                        env_id = existing_sess.get("agents", {}).get(role_id, {}).get("environment_id")
                    if not prev_id:
                        prev_id = existing_sess.get("agents", {}).get(role_id, {}).get("interaction_id")

                async def send_and_record_single(ev: Dict[str, Any]) -> None:
                    update_session_from_event(run_id, ev)
                    try:
                        await ws.send_json(ev)
                    except Exception:
                        pass

                await run_parallel_newsroom(
                    topic=topic,
                    api_key=api_key,
                    run_id=run_id,
                    base_output_dir=OUTPUTS_DIR,
                    event_callback=send_and_record_single,
                    roles=[role_id],
                    podcast_options=podcast_options,
                    existing_environments={role_id: env_id} if env_id else None,
                    previous_interactions={role_id: prev_id} if prev_id else None,
                )
                await ws.send_json({"type": "role_regenerated", "run_id": run_id, "role": role_id})

            elif action == "followup":
                role_id = payload["role"]
                prompt = payload["prompt"]
                prev_id = payload.get("previous_interaction_id")
                env_id = payload.get("environment_id")
                run_id = payload.get("run_id") or f"run_{int(time.time())}"
                podcast_options = payload.get("podcast_options")

                async def send_and_record_followup(ev: Dict[str, Any]) -> None:
                    update_session_from_event(run_id, ev)
                    try:
                        await ws.send_json(ev)
                    except Exception:
                        pass

                await run_followup_in_vm(
                    role_id=role_id,
                    followup_prompt=prompt,
                    previous_interaction_id=prev_id,
                    environment_id=env_id,
                    api_key=api_key,
                    run_id=run_id,
                    base_output_dir=OUTPUTS_DIR,
                    event_callback=send_and_record_followup,
                    podcast_options=podcast_options,
                )

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await ws.send_json({"type": "global_error", "error": str(exc)})
        except Exception:
            pass


@app.websocket("/ws/live-voice")
async def live_voice_ws(ws: WebSocket) -> None:
    """
    Persistent real-time bidirectional voice WebSocket powered by the Gemini Live API,
    grounded in the top 5 Hacker News stories scraped by the 4th Managed Agent VM (`live_qa`).
    """
    await ws.accept()
    try:
        init_raw = await ws.receive_text()
        init_msg = json.loads(init_raw)
        api_key = resolve_api_key(init_msg.get("api_key"))
        run_id = init_msg.get("run_id")
        voice_name = init_msg.get("voice") or "Kore"
        thinking_mode = init_msg.get("thinking_mode") or "standard"
        resumption_handle: Optional[str] = init_msg.get("resumption_handle") or None

        if not api_key:
            await ws.send_json(
                {
                    "type": "live_error",
                    "error": "Enter your Gemini API key at the top right to start live voice.",
                    "missing_key": True,
                }
            )
            await ws.close()
            return

        hn_context = await asyncio.to_thread(load_latest_hn_context, OUTPUTS_DIR, run_id)
        system_prompt = (
            "You are a live technology news correspondent answering questions in real time about "
            "today's top 5 Hacker News stories scraped by our Managed Agent virtual machine. "
            "Keep your spoken answers concise, natural, and direct (2 to 4 sentences). "
            "The user can interrupt you at any time. "
            "Here is the live briefing from the virtual machine:\n\n"
            f"{hn_context}"
        )

        is_extended = "extended" in thinking_mode or thinking_mode == "high"
        thinking_level = "high" if is_extended else "low"

        client = genai.Client(api_key=api_key)

        if is_extended:
            candidate_models = (
                "gemini-3.8-live-extended-thinking",
                "gemini-3.8-live",
                "gemini-3.1-flash-live-preview",
            )
        else:
            candidate_models = (
                "gemini-3.8-live",
                "gemini-3.1-flash-live-preview",
            )

        stop_requested = asyncio.Event()
        browser_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()

        async def pump_browser_messages() -> None:
            try:
                while not stop_requested.is_set():
                    msg_raw = await ws.receive_text()
                    data = json.loads(msg_raw)
                    if data.get("type") == "stop":
                        stop_requested.set()
                        await browser_queue.put(data)
                        break
                    await browser_queue.put(data)
            except WebSocketDisconnect:
                stop_requested.set()
                await browser_queue.put({"type": "stop"})
            except Exception:
                stop_requested.set()
                await browser_queue.put({"type": "stop"})

        browser_reader_task = asyncio.create_task(pump_browser_messages())

        first_connect = True
        while not stop_requested.is_set():
            full_live_config = types.LiveConnectConfig(
                response_modalities=[types.Modality.AUDIO],
                speech_config=types.SpeechConfig(
                    voice_config=types.VoiceConfig(
                        prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=voice_name)
                    )
                ),
                thinking_config=types.ThinkingConfig(
                    thinking_level=thinking_level,
                    include_thoughts=is_extended,
                ),
                input_audio_transcription=types.AudioTranscriptionConfig(),
                output_audio_transcription=types.AudioTranscriptionConfig(),
                system_instruction=types.Content(parts=[types.Part(text=system_prompt)]),
            )
            clean_live_config = types.LiveConnectConfig(
                response_modalities=[types.Modality.AUDIO],
                speech_config=types.SpeechConfig(
                    voice_config=types.VoiceConfig(
                        prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=voice_name)
                    )
                ),
                input_audio_transcription=types.AudioTranscriptionConfig(),
                output_audio_transcription=types.AudioTranscriptionConfig(),
                system_instruction=types.Content(parts=[types.Part(text=system_prompt)]),
            )

            session_cm = None
            session = None
            connected_model = candidate_models[0]
            last_err = ""
            for candidate_model in candidate_models:
                for cfg in (full_live_config, clean_live_config):
                    try:
                        cm = client.aio.live.connect(model=candidate_model, config=cfg)
                        session = await cm.__aenter__()
                        session_cm = cm
                        connected_model = candidate_model
                        break
                    except Exception as e:
                        last_err = str(e)
                        continue
                if session_cm and session:
                    break

            if not session_cm or not session:
                await ws.send_json(
                    {
                        "type": "live_error",
                        "error": f"Could not connect to Gemini Live API ({last_err or 'connection failed'}).",
                    }
                )
                break

            if first_connect:
                first_connect = False
                await ws.send_json(
                    {
                        "type": "live_connected",
                        "model": connected_model,
                        "thinking_level": thinking_level,
                        "voice": voice_name,
                    }
                )

            session_closed = asyncio.Event()

            async def forward_to_gemini() -> None:
                try:
                    while not stop_requested.is_set() and not session_closed.is_set():
                        try:
                            data = await asyncio.wait_for(browser_queue.get(), timeout=0.5)
                        except asyncio.TimeoutError:
                            continue
                        mtype = data.get("type")
                        if mtype == "stop":
                            stop_requested.set()
                            break
                        elif mtype == "audio_chunk" and data.get("data"):
                            pcm_bytes = base64.b64decode(data["data"])
                            await session.send_realtime_input(
                                audio=types.Blob(data=pcm_bytes, mime_type="audio/pcm;rate=16000")
                            )
                        elif mtype == "audio_stream_end":
                            await session.send_realtime_input(audio_stream_end=True)
                        elif mtype == "text_question" and data.get("text"):
                            await session.send_realtime_input(text=data["text"])
                except Exception:
                    session_closed.set()

            async def receive_from_gemini() -> None:
                nonlocal resumption_handle
                try:
                    # IMPORTANT: `session.receive()` in `google-genai` yields messages for ONE model turn
                    # and then breaks when `turn_complete` is True. Wrapping it in `while True:` keeps
                    # the session open across unlimited turns and interruptions!
                    while not stop_requested.is_set():
                        got_any = False
                        async for response in session.receive():
                            got_any = True
                            if getattr(response, "session_resumption_update", None):
                                upd = response.session_resumption_update
                                if getattr(upd, "resumable", False) and getattr(upd, "new_handle", None):
                                    resumption_handle = upd.new_handle
                                    await ws.send_json(
                                        {
                                            "type": "live_resumption_handle",
                                            "handle": resumption_handle,
                                        }
                                    )

                            content = getattr(response, "server_content", None)
                            if not content:
                                continue

                            if getattr(content, "interrupted", False):
                                await ws.send_json({"type": "live_interrupted"})

                            if getattr(content, "model_turn", None):
                                for part in getattr(content.model_turn, "parts", []) or []:
                                    if getattr(part, "thought", False) and getattr(part, "text", None):
                                        await ws.send_json(
                                            {"type": "live_thought", "text": part.text}
                                        )
                                    inline = getattr(part, "inline_data", None)
                                    if inline and getattr(inline, "data", None):
                                        raw_aud = inline.data
                                        b64_aud = (
                                            base64.b64encode(raw_aud).decode("ascii")
                                            if isinstance(raw_aud, bytes)
                                            else raw_aud
                                        )
                                        await ws.send_json({"type": "live_audio", "data": b64_aud})

                            if getattr(content, "input_transcription", None):
                                t_in = getattr(content.input_transcription, "text", None)
                                if t_in:
                                    await ws.send_json({"type": "live_input_transcript", "text": t_in})

                            if getattr(content, "output_transcription", None):
                                t_out = getattr(content.output_transcription, "text", None)
                                if t_out:
                                    await ws.send_json({"type": "live_output_transcript", "text": t_out})

                            if getattr(content, "turn_complete", False):
                                await ws.send_json({"type": "live_turn_complete"})

                        if not got_any:
                            # Underlying WebSocket closed cleanly or rotated
                            break
                except Exception:
                    pass
                finally:
                    session_closed.set()

            fwd_task = asyncio.create_task(forward_to_gemini())
            rcv_task = asyncio.create_task(receive_from_gemini())
            await asyncio.wait([fwd_task, rcv_task], return_when=asyncio.FIRST_COMPLETED)
            session_closed.set()
            for t in (fwd_task, rcv_task):
                if not t.done():
                    t.cancel()
            try:
                await session_cm.__aexit__(None, None, None)
            except Exception:
                pass

            if not stop_requested.is_set():
                await asyncio.sleep(0.3)

        browser_reader_task.cancel()

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await ws.send_json({"type": "live_error", "error": str(exc)})
        except Exception:
            pass
