#!/usr/bin/env python3
"""
pipeline.py  —  PrimeSrc pipeline adapted for Render (headless nodriver)
=========================================================================
Differences from the original primesrc_pipeline.py:
  - No Windows paths / Chrome profile copying
  - nodriver launched with headless=True, browser_executable_path to system Chromium
  - log_sink list captures output instead of printing to console
  - Exposed as async run_pipeline() called by server.py
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import os
import re
import time
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse
from urllib.request import Request, urlopen

warnings.filterwarnings("ignore", category=ResourceWarning)

# ── tunables ──────────────────────────────────────────────────────────────────
STAGE1_REQUEST_TIMEOUT = 20
STAGE2_PAGE_TIMEOUT    = 45
STAGE2_BLANK_TIMEOUT   = 1
STAGE2_RELOADS         = 2
STAGE2_FINAL_RETRIES   = 1

TMDB_ID_RE  = re.compile(r"^\d+$")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "6fad3f86b8452ee232deb7977d7dcf58")

# Chromium binary locations tried in order on Linux
CHROMIUM_CANDIDATES = [
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
]


# ── logger ────────────────────────────────────────────────────────────────────
def _make_logger(sink: list[str]):
    """Returns log functions that append to sink and also print."""
    def _log(level: str, msg: str) -> None:
        line = f"[{level}] {msg}"
        sink.append(line)
        print(line)
    return (
        lambda m: _log("INFO",  m),
        lambda m: _log("OK",    m),
        lambda m: _log("WARN",  m),
        lambda m: _log("ERR",   m),
    )


# ════════════════════════════════════════════════════════════════
# STAGE 1  —  embed URLs → /api/v1/s → api_url_list.txt
# ════════════════════════════════════════════════════════════════

@dataclass
class ServerOption:
    server_name:    str
    key:            str
    api_url:        str
    main_url:       str
    title:          str = ""
    quality:        str = ""
    audio_language: str = ""


def _build_server_api_url(main_url: str) -> str:
    parsed = urlparse(main_url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if parsed.path.startswith("/embed/movie"):
        params.setdefault("type", "movie")
    elif parsed.path.startswith("/embed/tv"):
        params.setdefault("type", "tv")
    base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'primesrc.me'}"
    return f"{base}/api/v1/s?{urlencode(params)}"


def _fetch_json_http(url: str, referer: str) -> Any:
    req = Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":  "application/json, */*",
        "Referer": referer,
    })
    with urlopen(req, timeout=STAGE1_REQUEST_TIMEOUT) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return json.loads(resp.read().decode(charset, errors="replace"))


def _normalise_embed_url(raw: str, media_type: str = "movie") -> str:
    raw = raw.strip()
    if TMDB_ID_RE.fullmatch(raw):
        return f"https://primesrc.me/embed/{media_type}?tmdb={raw}"
    if raw.startswith("primesrc.me/"):
        return "https://" + raw
    if raw.startswith("/embed/"):
        return "https://primesrc.me" + raw
    return raw


def _find_server_lists(obj: Any) -> list[dict[str, Any]]:
    lists: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        servers = obj.get("servers")
        if isinstance(servers, list) and servers:
            if any("key" in item or "file_name" in item for item in servers if isinstance(item, dict)):
                info = obj.get("info") if isinstance(obj.get("info"), dict) else {}
                lists.append({"servers": servers, "info": info})
        for v in obj.values():
            lists.extend(_find_server_lists(v))
    elif isinstance(obj, list):
        for item in obj:
            lists.extend(_find_server_lists(item))
    return lists


def _options_from_server_list(servers: list[dict], main_url: str) -> list[ServerOption]:
    options: list[ServerOption] = []
    for item in servers:
        key  = str(item.get("key")  or "").strip()
        name = str(item.get("name") or "").strip()
        if not key:
            continue
        options.append(ServerOption(
            server_name    = name,
            key            = key,
            api_url        = f"https://primesrc.me/api/v1/l?key={quote(key, safe='')}",
            main_url       = main_url,
            title          = str(item.get("file_name")      or "").strip(),
            quality        = str(item.get("quality")        or "").strip(),
            audio_language = str(item.get("audio_language") or "").strip(),
        ))
    return options


def stage1_fetch_api_keys(
    input_file: Path,
    api_list_file: Path,
    media_type: str,
    log_info, log_ok, log_warn, log_err,
) -> list[ServerOption]:
    raw_lines = [
        l.strip()
        for l in input_file.read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.startswith("#")
    ]
    log_info(f"Stage 1 — {len(raw_lines)} input embed URLs")

    seen_urls: set[str] = set()
    embed_urls: list[str] = []
    for raw in raw_lines:
        url = _normalise_embed_url(raw, media_type)
        if url not in seen_urls:
            seen_urls.add(url)
            embed_urls.append(url)

    all_options:  list[ServerOption]        = []
    errors:       list[tuple[str, str]]     = []

    for idx, embed_url in enumerate(embed_urls, 1):
        label    = f"[{idx:>4}/{len(embed_urls)}]"
        api_url  = _build_server_api_url(embed_url)
        try:
            obj          = _fetch_json_http(api_url, embed_url)
            server_lists = _find_server_lists(obj)
            if not server_lists:
                log_warn(f"{label} no server list  {embed_url}")
                continue
            for sl in server_lists:
                opts = _options_from_server_list(sl.get("servers", []), embed_url)
                all_options.extend(opts)
            count = sum(
                len(_options_from_server_list(sl.get("servers", []), embed_url))
                for sl in server_lists
            )
            log_ok(f"{label} {count} keys  {embed_url}")
        except Exception as exc:
            errors.append((embed_url, str(exc)))
            log_err(f"{label} {exc}  {embed_url}")

    seen_api: set[str] = set()
    unique_options: list[ServerOption] = []
    for opt in all_options:
        if opt.api_url not in seen_api:
            seen_api.add(opt.api_url)
            unique_options.append(opt)

    api_list_file.write_text(
        "\n".join(opt.api_url for opt in unique_options) + "\n",
        encoding="utf-8",
    )
    log_info(f"Stage 1 — total keys: {len(all_options)}  unique: {len(unique_options)}  errors: {len(errors)}")
    return unique_options


# ════════════════════════════════════════════════════════════════
# STAGE 2  —  keys → nodriver headless → stream URLs
# ════════════════════════════════════════════════════════════════

def _get_chromium_exe() -> str:
    for path in CHROMIUM_CANDIDATES:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(
        f"Chromium not found. Tried: {CHROMIUM_CANDIDATES}"
    )


def extract_json(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty page content")
    if text[0] in "{[":
        return json.loads(text)
    s = text.find("{")
    e = text.rfind("}") + 1
    if s == -1 or e <= s:
        raise ValueError("No JSON object found in page")
    return json.loads(text[s:e])


def get_play_url(data: Any) -> str | None:
    if isinstance(data, dict):
        for key in ("link", "url", "file", "src", "stream"):
            v = data.get(key)
            if isinstance(v, str) and v.startswith(("http://", "https://")):
                return v
        for key in ("sources", "tracks", "streams"):
            items = data.get(key)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, str) and item.startswith(("http://", "https://")):
                        return item
                    if isinstance(item, dict):
                        nested = get_play_url(item)
                        if nested:
                            return nested
    elif isinstance(data, list):
        for item in data:
            nested = get_play_url(item)
            if nested:
                return nested
    return None


async def wait_for_json_fast(page: Any, timeout: int = 45, blank_timeout: int = 1) -> str:
    deadline = time.monotonic() + timeout
    started  = time.monotonic()
    last_text = ""
    tick = 0
    while time.monotonic() < deadline:
        await asyncio.sleep(0.1)
        tick += 1
        try:
            text = await page.evaluate("document.body.innerText")
            last_text = (text or "").strip()
            if last_text and last_text[0] in "{[":
                return last_text
            if tick % 10 == 0:
                title = await page.evaluate("document.title")
                if title == "" and time.monotonic() - started >= blank_timeout:
                    raise ValueError("Blank page stalled before JSON")
        except ValueError:
            raise
        except Exception:
            pass
    return last_text


_print_lock: asyncio.Lock | None = None


async def extract_one(
    browser: Any,
    api_url: str,
    timeout: int,
    blank_timeout: int,
    reloads: int,
    sem: asyncio.Semaphore,
    index: int,
    total: int,
    log_ok, log_err,
) -> dict[str, Any]:
    async with sem:
        label = f"[{index:>3}/{total}]"
        try:
            page = await browser.get(api_url, new_tab=True)
        except Exception as e:
            log_err(f"{label} open tab failed: {e}")
            return {"index": index, "api_url": api_url, "error": str(e), "extracted_url": None}

        last_error = None
        try:
            for attempt in range(reloads + 1):
                if attempt:
                    await page.reload(ignore_cache=True)
                    await asyncio.sleep(0.2)
                try:
                    text = await wait_for_json_fast(page, timeout=timeout, blank_timeout=blank_timeout)
                    if not text or text[0] not in "{[":
                        text = await page.evaluate("document.body.innerHTML")
                    data     = extract_json(text)
                    play_url = get_play_url(data)
                    if play_url:
                        log_ok(f"{label} {play_url}")
                        return {"index": index, "api_url": api_url, "data": data, "extracted_url": play_url}
                    last_error = "no URL in response"
                    log_err(f"{label} {last_error}")
                except Exception as e:
                    last_error = str(e)
                    log_err(f"{label} {last_error}")

            return {"index": index, "api_url": api_url, "error": last_error or "failed", "extracted_url": None}
        finally:
            try:
                await page.close()
            except Exception:
                pass


async def process_batch(
    browser: Any,
    indexed_urls: list[tuple[int, str]],
    total: int,
    timeout: int,
    blank_timeout: int,
    reloads: int,
    batch_title: str,
    batch_size: int,
    log_info, log_ok, log_err,
) -> list[dict[str, Any]]:
    log_info(f"{batch_title}: {len(indexed_urls)} URL(s)")
    sem   = asyncio.Semaphore(batch_size)
    tasks = [
        asyncio.create_task(
            extract_one(browser, url, timeout, blank_timeout, reloads, sem, index, total, log_ok, log_err)
        )
        for index, url in indexed_urls
    ]
    return await asyncio.gather(*tasks)


async def stage2_extract_stream_urls(
    api_list_file: Path,
    stream_out_file: Path,
    batch_size: int,
    log_info, log_ok, log_warn, log_err,
) -> list[dict[str, Any]]:
    import nodriver as uc  # type: ignore[import]

    api_urls = [
        l.strip()
        for l in api_list_file.read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.startswith("#")
    ]
    if not api_urls:
        log_warn("api_url_list.txt is empty — nothing to resolve")
        return []

    log_info(f"Stage 2 — {len(api_urls)} keys, batch_size={batch_size}")

    chromium_exe = _get_chromium_exe()
    log_info(f"Chromium: {chromium_exe}")

    # Launch nodriver with headless Chromium — no profile needed
    browser = await uc.start(
        headless=True,
        browser_executable_path=chromium_exe,
        browser_args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-setuid-sandbox",
            "--disable-software-rasterizer",
            "--window-size=1280,800",
        ],
    )

    t_start = time.monotonic()
    results: list[dict[str, Any]] = []

    try:
        # Warm-up: first URL alone
        results.extend(await process_batch(
            browser, [(1, api_urls[0])], len(api_urls),
            STAGE2_PAGE_TIMEOUT, STAGE2_BLANK_TIMEOUT, STAGE2_RELOADS,
            "Warm-up 1/1", 1,
            log_info, log_ok, log_err,
        ))

        # Remaining in batches
        remaining   = list(enumerate(api_urls[1:], 2))
        batch_total = (len(remaining) + batch_size - 1) // batch_size
        for batch_num, start in enumerate(range(0, len(remaining), batch_size), 1):
            batch = remaining[start : start + batch_size]
            results.extend(await process_batch(
                browser, batch, len(api_urls),
                STAGE2_PAGE_TIMEOUT, STAGE2_BLANK_TIMEOUT, STAGE2_RELOADS,
                f"Batch {batch_num}/{batch_total}", batch_size,
                log_info, log_ok, log_err,
            ))

        # Final retry passes
        for attempt in range(1, STAGE2_FINAL_RETRIES + 1):
            failed = [(r["index"], r["api_url"]) for r in results if not r.get("extracted_url")]
            if not failed:
                break
            retry_results  = await process_batch(
                browser, failed, len(api_urls),
                STAGE2_PAGE_TIMEOUT, STAGE2_BLANK_TIMEOUT, 0,
                f"Final retry {attempt}/{STAGE2_FINAL_RETRIES}", batch_size,
                log_info, log_ok, log_err,
            )
            retry_by_index = {r["index"]: r for r in retry_results}
            results = [
                retry_by_index.get(r["index"], r) if not r.get("extracted_url") else r
                for r in results
            ]
    finally:
        try:
            browser.stop()
        except Exception:
            pass

    results.sort(key=lambda r: r.get("index", 0))
    ok    = [r for r in results if r.get("extracted_url")]
    fails = [r for r in results if not r.get("extracted_url")]

    # Write flat stream URL list
    stream_out_file.write_text(
        "\n".join(r["extracted_url"] for r in ok) + "\n",
        encoding="utf-8",
    )

    elapsed = time.monotonic() - t_start
    log_info(f"Stage 2 done in {elapsed:.1f}s — success: {len(ok)}  failed: {len(fails)}")
    return results


# ════════════════════════════════════════════════════════════════
# SUMMARY  —  pipeline_summary.json + .gz.json
# ════════════════════════════════════════════════════════════════

def _tmdb_request(path: str) -> dict:
    base = "https://api.themoviedb.org/3"
    sep  = "&" if "?" in path else "?"
    url  = f"{base}{path}{sep}language=en-US"
    if TMDB_API_KEY:
        url += f"&api_key={TMDB_API_KEY}"
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Accept":     "application/json",
    })
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _fetch_tmdb_info(tmdb_id: str) -> tuple[str, str | None]:
    try:
        data    = _tmdb_request(f"/movie/{tmdb_id}")
        title   = data.get("title") or data.get("original_title") or ""
        imdb_id = data.get("imdb_id") or None
        if not imdb_id:
            ext     = _tmdb_request(f"/movie/{tmdb_id}/external_ids")
            imdb_id = ext.get("imdb_id") or None
        return title, imdb_id
    except Exception:
        return "", None


def _to_gz_b64_json(pretty_path: Path, gz_path: Path) -> None:
    raw  = pretty_path.read_bytes()
    gz   = gzip.compress(raw, compresslevel=9)
    b64  = base64.b64encode(gz).decode("ascii")
    gz_path.write_text(
        json.dumps({"encoding": "gzip+base64", "source_file": pretty_path.name, "compressed": b64}),
        encoding="utf-8",
    )


def _format_summary_json(records: list[dict[str, Any]]) -> str:
    def _jv(v: Any) -> str:
        return json.dumps(v, ensure_ascii=False)

    lines: list[str] = ["["]
    for rec_idx, rec in enumerate(records):
        lines.append("  {")
        header_keys = ["serial", "title", "tmdb_id", "imdb_id", "extracted_at"]
        n_sources   = sum(1 for k in rec if re.fullmatch(r"host-\d+", k))
        all_field_lines: list[str] = []
        for hk in header_keys:
            if hk in rec:
                all_field_lines.append(f'    {_jv(hk)}: {_jv(rec[hk])}')
        for n in range(1, n_sources + 1):
            hkey = f"host-{n}"
            ukey = f"url-{n}"
            all_field_lines.append(
                f'    {_jv(hkey)}: {_jv(rec.get(hkey, ""))}, {_jv(ukey)}: {_jv(rec.get(ukey, ""))}'
            )
        is_last_rec = rec_idx == len(records) - 1
        for fi, fl in enumerate(all_field_lines):
            is_last_field = fi == len(all_field_lines) - 1
            lines.append(fl if is_last_field else fl + ",")
        lines.append("  }" if is_last_rec else "  },")
    lines.append("]")
    return "\n".join(lines) + "\n"


def _write_summary(
    stage1_options: list[ServerOption],
    stage2_results: list[dict[str, Any]],
    json_path: Path,
    log_info, log_ok, log_warn,
) -> None:
    link_map = {r["api_url"]: r.get("extracted_url") or "" for r in stage2_results}

    new_groups:    dict[str, list[dict[str, Any]]] = defaultdict(list)
    tmdb_to_main:  dict[str, str] = {}
    for opt in stage1_options:
        stream_url = link_map.get(opt.api_url, "")
        if not stream_url:
            continue
        qs   = dict(x.split("=", 1) for x in urlparse(opt.main_url).query.split("&") if "=" in x)
        tmdb = qs.get("tmdb", "")
        if not tmdb:
            continue
        new_groups[tmdb].append({"host": urlparse(stream_url).netloc, "url": stream_url})
        tmdb_to_main.setdefault(tmdb, opt.main_url)

    existing: list[dict[str, Any]] = []
    if json_path.exists():
        try:
            existing = json.loads(json_path.read_text(encoding="utf-8"))
            if not isinstance(existing, list):
                existing = []
            log_info(f"Loaded {len(existing)} existing entries")
        except Exception as exc:
            log_warn(f"Could not load existing JSON ({exc}) — starting fresh")

    index: dict[int, dict[str, Any]] = {}
    for e in existing:
        tmdb_int = e["tmdb_id"]
        sources: list[dict[str, str]] = []
        n = 1
        while f"host-{n}" in e:
            sources.append({"host": e[f"host-{n}"], "url": e[f"url-{n}"]})
            n += 1
        index[tmdb_int] = {
            "tmdb_id":      tmdb_int,
            "imdb_id":      e.get("imdb_id"),
            "title":        e.get("title", ""),
            "extracted_at": e["extracted_at"],
            "_sources":     sources,
        }

    extracted_at     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tmdb_meta_cache: dict[int, tuple[str, Any]] = {}

    for tmdb_str, new_sources in new_groups.items():
        tmdb_int = int(tmdb_str)
        if tmdb_int in index:
            entry         = index[tmdb_int]
            existing_urls = {s["url"] for s in entry["_sources"]}
            added         = [s for s in new_sources if s["url"] not in existing_urls]
            entry["_sources"].extend(added)
            entry["extracted_at"] = extracted_at
        else:
            if tmdb_int not in tmdb_meta_cache:
                title, imdb_id = _fetch_tmdb_info(tmdb_str)
                tmdb_meta_cache[tmdb_int] = (title, imdb_id)
            else:
                title, imdb_id = tmdb_meta_cache[tmdb_int]
            index[tmdb_int] = {
                "tmdb_id":      tmdb_int,
                "imdb_id":      imdb_id,
                "title":        title,
                "extracted_at": extracted_at,
                "_sources":     list(new_sources),
            }
            log_ok(f"tmdb={tmdb_int} '{title}' — {len(new_sources)} source(s)")

    sorted_entries = sorted(index.values(), key=lambda x: x["tmdb_id"])
    for i, entry in enumerate(sorted_entries, 1):
        entry["serial"] = i

    output: list[dict[str, Any]] = []
    for e in sorted_entries:
        row: dict[str, Any] = {
            "serial":       e["serial"],
            "title":        e.get("title", ""),
            "tmdb_id":      e["tmdb_id"],
            "imdb_id":      e.get("imdb_id"),
            "extracted_at": e["extracted_at"],
        }
        for n, src in enumerate(e["_sources"], 1):
            row[f"host-{n}"] = src["host"]
            row[f"url-{n}"]  = src["url"]
        output.append(row)

    json_path.write_text(_format_summary_json(output), encoding="utf-8")
    gz_path = json_path.with_suffix("").with_suffix(".gz.json")
    _to_gz_b64_json(json_path, gz_path)

    total_sources = sum(sum(1 for k in row if k.startswith("url-")) for row in output)
    log_ok(f"Summary written — movies: {len(output)}  sources: {total_sources}")
    log_ok(f"JSON  → {json_path}")
    log_ok(f"GZ    → {gz_path}")


# ════════════════════════════════════════════════════════════════
# PUBLIC ENTRY POINT  (called by server.py)
# ════════════════════════════════════════════════════════════════

async def run_pipeline(
    input_file:  Path,
    work_dir:    Path,
    media_type:  str  = "movie",
    skip_stage1: bool = False,
    skip_stage2: bool = False,
    batch_size:  int  = 5,
    log_sink:    list[str] | None = None,
) -> None:
    if log_sink is None:
        log_sink = []
    log_info, log_ok, log_warn, log_err = _make_logger(log_sink)

    api_list_file = work_dir / "api_url_list.txt"
    stream_out    = work_dir / "final_stream_urls.txt"
    json_out      = work_dir / "pipeline_summary.json"

    stage1_options: list[ServerOption]    = []
    stage2_results: list[dict[str, Any]]  = []

    # Stage 1
    if skip_stage1:
        log_info("Stage 1 skipped")
        if api_list_file.exists():
            for line in api_list_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key = line.split("key=")[-1] if "key=" in line else ""
                stage1_options.append(ServerOption("", key, line, ""))
    else:
        stage1_options = stage1_fetch_api_keys(
            input_file, api_list_file, media_type,
            log_info, log_ok, log_warn, log_err,
        )

    # Stage 2
    if skip_stage2:
        log_info("Stage 2 skipped")
    else:
        stage2_results = await stage2_extract_stream_urls(
            api_list_file, stream_out, batch_size,
            log_info, log_ok, log_warn, log_err,
        )

    # Summary
    if stage1_options or stage2_results:
        _write_summary(
            stage1_options, stage2_results, json_out,
            log_info, log_ok, log_warn,
        )
