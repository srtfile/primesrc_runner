# PrimeSrc Pipeline — Render + GitHub Actions Setup

## Architecture

```
GitHub repo
  multiple_primesrc.txt  ←  you edit this
  pipeline_summary.json  ←  auto-committed after each run
  pipeline_summary.gz.json
  api_url_list.txt
  final_stream_urls.txt

GitHub Actions  (.github/workflows/primesrc_pipeline.yml)
  triggered by: push to multiple_primesrc.txt  OR  manual dispatch
  1. POSTs embed URLs to Render /run
  2. Polls /status every 15s until done
  3. Downloads result files from /results/*
  4. Commits them back to repo

Render  (render_app/)
  FastAPI + nodriver + headless Chromium
  /run      → starts pipeline in background
  /status   → check if running / done
  /results/ → download output files
```

---

## Setup

### Step 1 — Deploy to Render

1. Push the `render_app/` folder contents to a GitHub repo (can be the same repo or a separate one).
2. Go to [render.com](https://render.com) → **New Web Service** → connect your repo.
3. Render auto-detects `render.yaml`. Click **Apply**.
4. Once deployed, copy your service URL, e.g. `https://primesrc-pipeline.onrender.com`.

### Step 2 — Set Render environment variables

In the Render dashboard → your service → **Environment**:

| Key | Value |
|-----|-------|
| `PIPELINE_SECRET` | any random string, e.g. `mys3cr3ttoken` |
| `TMDB_API_KEY` | your TMDB v3 API key (optional) |

### Step 3 — Set GitHub repository secrets

In your GitHub repo → **Settings → Secrets → Actions**:

| Secret | Value |
|--------|-------|
| `RENDER_APP_URL` | `https://primesrc-pipeline.onrender.com` (no trailing slash) |
| `PIPELINE_SECRET` | same value you set on Render |

### Step 4 — Add your input file

Put your embed URLs in `multiple_primesrc.txt` at the repo root, one per line:

```
https://primesrc.me/embed/movie?tmdb=12345
https://primesrc.me/embed/movie?tmdb=67890
12345
primesrc.me/embed/movie?tmdb=11111
```

Lines starting with `#` are ignored.

### Step 5 — Run

**Manual:** GitHub → Actions → "PrimeSrc Pipeline" → Run workflow → pick options.

**Auto:** Just push a change to `multiple_primesrc.txt` — the workflow triggers automatically.

---

## Notes

- **Render free tier** spins down after inactivity. First request after sleep takes ~30s to wake up. The workflow handles this (it polls for up to 90 minutes).
- Only one pipeline run at a time — concurrent `/run` calls return HTTP 409.
- Results are committed with `[skip ci]` in the message to avoid re-triggering the workflow.
- `pipeline_summary.json` is cumulative — new sources are merged into existing entries by tmdb_id.
