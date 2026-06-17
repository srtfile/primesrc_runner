#!/usr/bin/env python3
"""
server.py  —  PrimeSrc pipeline server for Render
===================================================
Endpoints
  POST /run          trigger pipeline (requires X-Secret header)
  GET  /status       current run status + last run info
  GET  /results/{f}  download output file by name
  GET  /health       uptime check
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from pipeline import run_pipeline   # our adapted pipeline module

# ── config ───────────────────────────────────────────────────────────────────
SECRET          = os.environ.get("PIPELINE_SECRET", "")
WORK_DIR        = Path("/tmp/primesrc")
OUTPUT_FILES    = [
    "api_url_list.txt",
    "final_stream_urls.txt",
    "pipeline_summary.json",
    "pipeline_summary.gz.json",
]
INPUT_FILE      = WORK_DIR / "multiple_primesrc.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("server")

WORK_DIR.mkdir(parents=True, exist_ok=True)

# ── state ─────────────────────────────────────────────────────────────────────
_state: dict = {
    "running":    False,
    "last_start": None,
    "last_end":   None,
    "last_status": "idle",   # idle | running | success | error
    "last_error":  None,
    "last_log":    [],
}

app = FastAPI(title="PrimeSrc Pipeline", version="1.0")


# ── auth helper ──────────────────────────────────────────────────────────────
def _check_secret(x_secret: str | None) -> None:
    if SECRET and x_secret != SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")


# ── request body ─────────────────────────────────────────────────────────────
class RunRequest(BaseModel):
    media_type:   str  = "movie"
    skip_stage1:  bool = False
    skip_stage2:  bool = False
    batch_size:   int  = 5
    embed_urls:   list[str] | None = None   # optional: pass URLs inline


# ── background task ──────────────────────────────────────────────────────────
async def _do_run(req: RunRequest) -> None:
    _state["running"]    = True
    _state["last_start"] = datetime.now(timezone.utc).isoformat()
    _state["last_end"]   = None
    _state["last_status"] = "running"
    _state["last_error"]  = None
    _state["last_log"]    = []

    try:
        # write embed URLs to input file if provided inline
        if req.embed_urls:
            INPUT_FILE.write_text("\n".join(req.embed_urls) + "\n", encoding="utf-8")
            log.info(f"Wrote {len(req.embed_urls)} embed URLs to {INPUT_FILE}")
        elif not INPUT_FILE.exists():
            raise FileNotFoundError(
                "multiple_primesrc.txt not found on server and no embed_urls provided"
            )

        output_log: list[str] = []
        await run_pipeline(
            input_file   = INPUT_FILE,
            work_dir     = WORK_DIR,
            media_type   = req.media_type,
            skip_stage1  = req.skip_stage1,
            skip_stage2  = req.skip_stage2,
            batch_size   = req.batch_size,
            log_sink     = output_log,
        )
        _state["last_log"]    = output_log
        _state["last_status"] = "success"
        log.info("Pipeline finished successfully")

    except Exception as exc:
        _state["last_status"] = "error"
        _state["last_error"]  = str(exc)
        log.exception("Pipeline failed")

    finally:
        _state["running"]  = False
        _state["last_end"] = datetime.now(timezone.utc).isoformat()


# ── endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health() -> dict:
    return {"ok": True, "time": datetime.now(timezone.utc).isoformat()}


@app.get("/status")
def status() -> dict:
    files = {}
    for name in OUTPUT_FILES:
        p = WORK_DIR / name
        files[name] = {"exists": p.exists(), "size": p.stat().st_size if p.exists() else 0}
    return {**_state, "output_files": files}


@app.post("/run")
async def run(
    req: RunRequest,
    background_tasks: BackgroundTasks,
    x_secret: str | None = Header(default=None),
) -> dict:
    _check_secret(x_secret)

    if _state["running"]:
        raise HTTPException(status_code=409, detail="Pipeline already running")

    background_tasks.add_task(_do_run, req)
    return {"ok": True, "message": "Pipeline started"}


@app.get("/results/{filename}")
def download(
    filename: str,
    x_secret: str | None = Header(default=None),
) -> FileResponse:
    _check_secret(x_secret)

    # only allow known output files
    if filename not in OUTPUT_FILES:
        raise HTTPException(status_code=404, detail="Unknown file")

    path = WORK_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not generated yet")

    return FileResponse(path, filename=filename)


@app.get("/results")
def list_results(x_secret: str | None = Header(default=None)) -> dict:
    _check_secret(x_secret)
    files = {}
    for name in OUTPUT_FILES:
        p = WORK_DIR / name
        files[name] = {
            "exists": p.exists(),
            "size":   p.stat().st_size if p.exists() else 0,
            "url":    f"/results/{name}",
        }
    return {"files": files}
