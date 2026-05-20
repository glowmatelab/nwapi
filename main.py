import os
import asyncio
import re
import glob
import time
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import yt_dlp
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
import uvicorn

# ─── Config ────────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("API_KEY", "")          
DOWNLOAD_DIR = Path("downloads")
CLEANUP_AFTER = 60 * 30                          

# Global lock dict to prevent duplicate parallel downloads for same video
download_locks = {}

# ─── Startup ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    cleanup_task = asyncio.create_task(cleanup_loop())
    yield
    cleanup_task.cancel()

app = FastAPI(title="YT Download API", lifespan=lifespan)

# ─── Auth ──────────────────────────────────────────────────────────────────────
def check_auth(request: Request, api_key: Optional[str] = None):
    if not API_KEY:
        return True
    key = api_key or request.headers.get("x-api-key", "")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# ─── Helpers ───────────────────────────────────────────────────────────────────
def extract_video_id(url_or_id: str) -> str:
    """Extracts the 11-char video ID from any YouTube URL or raw ID."""
    pattern = r'(?:v=|\/v\/|youtu\.be\/|\/embed\/|\/shorts\/|^)([a-zA-Z0-9_-]{11})'
    match = re.search(pattern, url_or_id.strip())
    if match:
        return match.group(1)
    raise HTTPException(status_code=400, detail="Invalid YouTube URL or Video ID format")

def find_file(video_id: str, ext: str) -> Optional[Path]:
    pattern = str(DOWNLOAD_DIR / f"{video_id}.{ext}")
    files = glob.glob(pattern)
    if files:
        return Path(files[0])
    return None

# ─── yt-dlp Options ────────────────────────────────────────────────────────────
def get_ydl_opts_audio(video_id: str) -> dict:
    return {
        "format": "bestaudio/best",
        # Keep temporary download simple, postprocessor will turn it into video_id.mp3
        "outtmpl": str(DOWNLOAD_DIR / f"{video_id}.%(ext)s"),
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
        "socket_timeout": 30,
    }

def get_ydl_opts_video(video_id: str) -> dict:
    return {
        "format": "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
        "outtmpl": str(DOWNLOAD_DIR / f"{video_id}.%(ext)s"),
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "merge_output_format": "mp4",
        "socket_timeout": 30,
    }

# ─── Core Logic ────────────────────────────────────────────────────────────────
async def download_yt(video_id: str, file_type: str) -> Path:
    ext = "mp3" if file_type == "audio" else "mp4"
    
    # 1. Check cache first
    cached = find_file(video_id, ext)
    if cached and cached.exists() and cached.stat().st_size > 0:
        return cached

    # 2. Concurrency Lock for this specific video
    if video_id not in download_locks:
        download_locks[video_id] = asyncio.Lock()
        
    async with download_locks[video_id]:
        # Check again inside lock in case another request finished it while waiting
        cached = find_file(video_id, ext)
        if cached and cached.exists() and cached.stat().st_size > 0:
            return cached

        url = f"https://www.youtube.com/watch?v={video_id}"
        opts = get_ydl_opts_audio(video_id) if file_type == "audio" else get_ydl_opts_video(video_id)

        loop = asyncio.get_event_loop()
        
        def _download():
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])

        try:
            await loop.run_in_executor(None, _download)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

        result = find_file(video_id, ext)
        if not result or not result.exists() or result.stat().st_size == 0:
            raise HTTPException(status_code=500, detail="File processing failed after download")

        return result

async def get_live_url(video_id: str) -> str:
    url = f"https://www.youtube.com/watch?v={video_id}"
    opts = {
        "quiet": True,
        "no_warnings": True,
        "format": "best",
        "socket_timeout": 15,
    }
    loop = asyncio.get_event_loop()

    def _extract():
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info.get("url") or info.get("manifest_url", "")

    try:
        return await loop.run_in_executor(None, _extract)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Live extraction failed: {str(e)}")

# ─── Cleanup ───────────────────────────────────────────────────────────────────
async def cleanup_loop():
    while True:
        await asyncio.sleep(600)  # Check every 10 mins
        now = time.time()
        for f in DOWNLOAD_DIR.glob("*"):
            try:
                if f.is_file() and now - f.stat().st_mtime > CLEANUP_AFTER:
                    f.unlink()
            except Exception:
                pass
        # Clear old locks memory safely
        expired_locks = [vid for vid, lock in download_locks.items() if not lock.locked()]
        for vid in expired_locks:
            download_locks.pop(vid, None)

# ─── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "running"}

@app.get("/download")
async def download(
    request: Request,
    url: str = Query(..., description="YouTube URL or Video ID"),
    type: str = Query("audio", description="audio or video"),
    api_key: Optional[str] = Query(None),
):
    check_auth(request, api_key)

    if type not in ("audio", "video"):
        raise HTTPException(status_code=400, detail="type must be 'audio' or 'video'")

    # Clean input and extract safe 11-char ID
    video_id = extract_video_id(url)

    file_path = await download_yt(video_id, type)

    return FileResponse(
        path=str(file_path),
        media_type="audio/mpeg" if type == "audio" else "video/mp4",
        filename=f"{video_id}.{type == 'audio' and 'mp3' or 'mp4'}",
    )

@app.get("/live")
async def live(
    request: Request,
    url: str = Query(..., description="YouTube URL or Video ID"),
    api_key: Optional[str] = Query(None),
):
    check_auth(request, api_key)

    video_id = extract_video_id(url)
    stream_url = await get_live_url(video_id)

    return JSONResponse({"stream_url": stream_url})

if __name__ == "__main__":
    # Standard dynamic port binding for VPS/Render/Heroku deployment
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
