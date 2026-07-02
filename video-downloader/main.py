"""
Video Downloader - FastAPI + HTMX + yt-dlp
=============================================
Flow:
1. User URL paste karta hai -> HTMX POST /analyze (no page reload)
2. yt-dlp se video info nikalta hai (title, thumbnail, formats)
3. Partial HTML return hota hai jisme format options hain
4. User format select karke /download trigger karta hai -> background task
5. HTMX polling se /progress/{job_id} check hota hai every 2s
6. Ready hone par ek download link milta hai jo seedha browser ke
   download manager (Chrome ke "downloads" icon) mein file save karta hai.

IMPORTANT (storage design):
Koi bhi file server par PERMANENTLY save nahi hoti. Har job apni khud ki
OS temp-directory mein download hoti hai (Linux par /tmp, jo aksar RAM-backed
tmpfs hoti hai). Jab user "Save File" click karta hai, file seedha
StreamingResponse se chunks mein browser ko bheji jaati hai, aur stream
khatam hote hi temp file/folder turant delete ho jaate hain. Is se Render
free tier ka disk space kabhi nahi bharta, chahe kitne log demo try karein.
"""

import os
import uuid
import threading
import time
import tempfile
import shutil
from pathlib import Path

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import yt_dlp

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

# Har job ki temp files OS ki system temp directory ke andar ek dedicated
# sub-folder mein rakhte hain, taake easily isolate aur cleanup ho sake.
TEMP_ROOT = Path(tempfile.gettempdir()) / "video-downloader-jobs"
TEMP_ROOT.mkdir(exist_ok=True)

# Safety-net cleanup: agar koi job kisi wajah se apni temp dir clean na kar
# paye (jaise user download click karne ke baad browser band kar de, ya
# kabhi file serve hi na ho), to yeh background loop use eventually hata deta hai.
JOB_MAX_AGE_SECONDS = 30 * 60  # 30 minutes


def _cleanup_loop():
    """Safety net cleanup - normally har job khud apni file delete kar deta
    hai stream complete hone ke turant baad, lekin yeh ek backup hai."""
    while True:
        now = time.time()
        for job_dir in TEMP_ROOT.glob("*"):
            if job_dir.is_dir() and (now - job_dir.stat().st_mtime) > JOB_MAX_AGE_SECONDS:
                shutil.rmtree(job_dir, ignore_errors=True)
        time.sleep(600)  # har 10 minute mein check


app = FastAPI(title="Video Downloader")


@app.on_event("startup")
def start_cleanup_thread():
    threading.Thread(target=_cleanup_loop, daemon=True).start()


app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# In-memory job store. FYP-scale ke liye yeh kaafi hai.
# Production mein iski jagah Redis ya DB use hoga.
# job_id -> { status, percent, error, job_dir, file_path, display_name }
JOBS: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Helper: yt-dlp se video info (title, thumbnail, formats) nikalna
# ---------------------------------------------------------------------------

def extract_video_info(url: str) -> dict:
    """URL se metadata nikalta hai bina download kiye."""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    # Formats ko simplify karte hain - har resolution ke liye sabse behtar
    # bitrate wala format rakhte hain (duplicate codecs mein se best select karte hain).
    best_by_height: dict[int, dict] = {}
    best_audio = None
    best_audio_abr = -1

    for f in info.get("formats", []):
        height = f.get("height")
        vcodec = f.get("vcodec")
        acodec = f.get("acodec")
        ext = f.get("ext")
        tbr = f.get("tbr") or 0

        # Audio-only format - sabse high bitrate wala rakhte hain
        if vcodec == "none" and acodec != "none":
            abr = f.get("abr") or tbr
            if abr > best_audio_abr:
                best_audio_abr = abr
                best_audio = f.get("format_id")
            continue

        # Video formats (DASH video-only streams bhi shamil, high-res ke liye normal)
        if height and vcodec != "none":
            existing = best_by_height.get(height)
            if existing is None or tbr > existing["tbr"]:
                best_by_height[height] = {
                    "format_id": f.get("format_id"),
                    "label": f"{height}p",
                    "ext": ext or "mp4",
                    "type": "video",
                    "tbr": tbr,
                }

    formats = list(best_by_height.values())
    for f in formats:
        f.pop("tbr", None)

    if best_audio:
        formats.append({
            "format_id": best_audio,
            "label": "Audio Only (MP3)",
            "ext": "mp3",
            "type": "audio",
        })

    def sort_key(f):
        if f["type"] == "audio":
            return -1
        return int(f["label"].replace("p", ""))

    formats.sort(key=sort_key, reverse=True)

    return {
        "title": info.get("title", "Unknown Title"),
        "thumbnail": info.get("thumbnail", ""),
        "duration": info.get("duration", 0),
        "uploader": info.get("uploader", "Unknown"),
        "formats": formats,
        "url": url,
    }


# ---------------------------------------------------------------------------
# Helper: background mein actual download chalana + progress update karna
# ---------------------------------------------------------------------------

def run_download(job_id: str, url: str, format_id: str, is_audio: bool):
    JOBS[job_id]["status"] = "downloading"
    JOBS[job_id]["percent"] = 0

    # Har job ki apni dedicated temp directory - isolated, baad mein
    # poori folder hi ek saath delete kar denge.
    job_dir = TEMP_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    JOBS[job_id]["job_dir"] = str(job_dir)

    def progress_hook(d):
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            downloaded = d.get("downloaded_bytes", 0)
            if total:
                JOBS[job_id]["percent"] = int(downloaded / total * 100)
        elif d["status"] == "finished":
            JOBS[job_id]["percent"] = 100
            JOBS[job_id]["status"] = "processing"  # audio conversion/merge ho sakta hai

    output_template = str(job_dir / "video.%(ext)s")

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "outtmpl": output_template,
        "progress_hooks": [progress_hook],
    }

    if is_audio:
        ydl_opts["format"] = "bestaudio/best"
        ydl_opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }]
    else:
        ydl_opts["format"] = f"{format_id}+bestaudio/best"
        ydl_opts["merge_output_format"] = "mp4"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        # yt-dlp kabhi kabhi extra files bhi "video.*" naam se save karta hai
        # (jaise .part intermediate files, .json metadata, .description, thumbnail).
        # Sirf actual media-file extensions allow karte hain, taake galat file
        # pick na ho. Agar multiple match milein, sabse badi file final/merged
        # output hoti hai (intermediate files chhoti hoti hain).
        VALID_MEDIA_EXT = {".mp4", ".mp3", ".webm", ".mkv", ".m4a", ".opus", ".ogg"}

        candidates = [
            f for f in job_dir.glob("video.*")
            if f.suffix.lower() in VALID_MEDIA_EXT and f.is_file()
        ]
        saved_file = max(candidates, key=lambda f: f.stat().st_size, default=None)

        if saved_file and saved_file.stat().st_size > 0:
            # IMPORTANT: HTTP headers sirf ASCII/Latin-1 support karte hain.
            # Python ka str.isalnum() Unicode-aware hai - matlab Urdu/Chinese/
            # emoji jaise characters bhi "alphanumeric" maan kar pass ho jate
            # hain. Agar aisा title Content-Disposition header mein chala jaye
            # to header encoding crash ho jati hai aur poori response corrupt
            # ho jati hai - Chrome ko broken stream milti hai aur wo
            # ".txt / No file" dikhata hai. Is liye sirf ASCII range allow
            # karte hain yahan.
            raw_title = info.get("title") or "video"
            safe_title = "".join(
                c for c in raw_title if c.isascii() and (c.isalnum() or c in " -_")
            ).strip()[:80] or "video"
            display_name = f"{safe_title}{saved_file.suffix}"

            JOBS[job_id]["status"] = "done"
            JOBS[job_id]["file_path"] = str(saved_file)
            JOBS[job_id]["display_name"] = display_name
            JOBS[job_id]["percent"] = 100
        else:
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["error"] = "File save nahi hui, dobara try karein."
            shutil.rmtree(job_dir, ignore_errors=True)

    except Exception as e:
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = str(e)
        shutil.rmtree(job_dir, ignore_errors=True)


def _delayed_cleanup(job_id: str, delay_seconds: int = 8):
    """Stream complete hone ke kuch second baad file delete karta hai (turant
    nahi). Ye isliye zaroori hai kyunki Chrome (aur kuch antivirus/extensions)
    'download' attribute wale links par kabhi kabhi ek se zyada GET request
    bhej dete hain (retry ya preliminary check ke taur par). Agar hum turant
    delete kar dein to dusri request 404 paati hai aur Chrome "File cannot be
    available" dikhata hai. Chhota sa delay is race condition ko khatam karta
    hai, aur background cleanup loop (30 min) phir bhi final safety-net hai."""
    job = JOBS.get(job_id)
    if not job or job.get("cleanup_scheduled"):
        return  # Pehle se hi schedule ho chuka hai, dobara thread nahi banani
    job["cleanup_scheduled"] = True

    def _do_cleanup():
        time.sleep(delay_seconds)
        job = JOBS.pop(job_id, None)
        if job and job.get("job_dir"):
            shutil.rmtree(job["job_dir"], ignore_errors=True)

    threading.Thread(target=_do_cleanup, daemon=True).start()


def stream_and_cleanup(job_id: str, file_path: Path, chunk_size: int = 1024 * 1024):
    """File ko chunks mein read karke browser ko bhejte hain. Stream complete
    hone ke turant baad delete NAHI karte (taake browser ki koi retry/duplicate
    request fail na ho) - iske bajaye chhoti si delay ke baad background mein
    cleanup hota hai."""
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk
    _delayed_cleanup(job_id)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/analyze", response_class=HTMLResponse)
async def analyze(request: Request, url: str = Form(...)):
    """HTMX yahan POST karta hai jab user URL submit karta hai."""
    url = url.strip()

    if not url:
        return templates.TemplateResponse(
            "partials/error.html",
            {"request": request, "message": "Pehle koi URL to daalein."},
        )

    try:
        info = extract_video_info(url)
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.html",
            {"request": request, "message": f"Video info nahi mil saki: {str(e)[:150]}"},
        )

    return templates.TemplateResponse(
        "partials/formats.html",
        {"request": request, "info": info},
    )


@app.post("/download", response_class=HTMLResponse)
async def download(
    request: Request,
    url: str = Form(...),
    format_id: str = Form(...),
    format_type: str = Form(...),
):
    """User ne format select karke download button click kiya.
    Job create karke background thread mein download start karte hain."""
    job_id = str(uuid.uuid4())
    is_audio = format_type == "audio"

    JOBS[job_id] = {"status": "queued", "percent": 0, "error": None, "job_dir": None}

    thread = threading.Thread(
        target=run_download,
        args=(job_id, url, format_id, is_audio),
        daemon=True,
    )
    thread.start()

    return templates.TemplateResponse(
        "partials/progress.html",
        {"request": request, "job_id": job_id},
    )


@app.get("/progress/{job_id}", response_class=HTMLResponse)
async def progress(request: Request, job_id: str):
    """HTMX har 2 second baad poll karta hai (hx-trigger='every 2s')."""
    job = JOBS.get(job_id)

    if not job:
        return templates.TemplateResponse(
            "partials/error.html",
            {"request": request, "message": "Job nahi mili."},
        )

    if job["status"] == "done":
        return templates.TemplateResponse(
            "partials/done.html",
            {"request": request, "job_id": job_id},
        )

    if job["status"] == "error":
        return templates.TemplateResponse(
            "partials/error.html",
            {"request": request, "message": job["error"]},
        )

    return templates.TemplateResponse(
        "partials/progress.html",
        {"request": request, "job_id": job_id, "percent": job["percent"], "status": job["status"]},
    )


@app.get("/file/{job_id}")
async def get_file(job_id: str):
    """File ko seedha stream karta hai browser ke download manager (Chrome
    ke downloads) ko - server ki disk par koi permanent copy nahi banti.
    Streaming complete hote hi temp folder khud-ba-khud delete ho jata hai."""
    job = JOBS.get(job_id)
    if not job or job.get("status") != "done" or not job.get("file_path"):
        return HTMLResponse("File abhi ready nahi hai.", status_code=404)

    file_path = Path(job["file_path"])
    if not file_path.exists():
        return HTMLResponse("File nahi mili.", status_code=404)

    display_name = job.get("display_name") or file_path.name
    file_size = file_path.stat().st_size

    # Extra safety: ASCII-only fallback filename banate hain header ke liye,
    # chahe display_name kuch bhi ho. RFC 5987 ka filename* field UTF-8
    # properly support karta hai agar kabhi future mein Unicode names chahiye hon.
    ascii_fallback = display_name.encode("ascii", errors="ignore").decode("ascii") or "download"
    from urllib.parse import quote
    encoded_name = quote(display_name)

    return StreamingResponse(
        stream_and_cleanup(job_id, file_path),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{ascii_fallback}"; '
                f"filename*=UTF-8''{encoded_name}"
            ),
            "Content-Length": str(file_size),
        },
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)