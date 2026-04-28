"""
YouTube WAV Distributed Downloader — Client
===========================================

Pulls batches of YouTube links from the orchestrator, downloads each as
16 kHz WAV (plus auto-subtitle when available), uploads to S3, and reports
back. Designed to run on many VPSes / GPU boxes in parallel.
"""
from __future__ import annotations

import argparse
import csv
import os
import socket
import sys
import time
import re
import json
import glob
import shutil
import tempfile
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import boto3
import requests
import yt_dlp
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

# ── Args / config ──────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="YouTube WAV distributed downloader client")
    p.add_argument("--api", default=os.getenv("API_BASE_URL", "http://51.102.128.158:8000"),
                   help="Orchestrator base URL")
    p.add_argument("--token", default=os.getenv("API_TOKEN", ""),
                   help="Shared secret for the orchestrator (or env API_TOKEN)")
    p.add_argument("--machine-id", default=os.getenv("MACHINE_ID", socket.gethostname()),
                   help="Unique id for this client")
    p.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "64")),
                   help="How many links to pull per batch")
    p.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "8")),
                   help="Parallel downloads inside a batch")
    p.add_argument("--cookies", default=os.getenv("COOKIES_FILE", ""),
                   help="Path to a Netscape-format cookies.txt for YouTube (optional)")
    p.add_argument("--workdir", default=os.getenv("WORKDIR", ""),
                   help="Working directory (default: a temp directory)")
    p.add_argument("--log-file", default=os.getenv("LOG_FILE", "download_log.csv"),
                   help="Per-video CSV log path (default: ./download_log.csv)")
    p.add_argument("--no-log-file", action="store_true",
                   help="Disable CSV log file")
    p.add_argument("--keep-files", action="store_true",
                   help="Keep downloaded WAV/SRT files after upload (debug)")
    return p.parse_args()


console = Console(log_path=False)


# ── Logging helpers ────────────────────────────────────────────────────────────
LOG_STYLES = {
    "info":    ("ℹ",  ""),
    "batch":   ("▶",  "bold cyan"),
    "start":   ("⤓",  "cyan"),
    "success": ("✓",  "green"),
    "skip":    ("⤼",  "yellow"),
    "error":   ("✗",  "red"),
    "warn":    ("!",  "yellow"),
}


class EventLogger:
    """Prints scrolling status lines above the rich Live dashboard
    and (optionally) appends a CSV row per video result."""

    def __init__(self, live: "Live", log_path: Optional[str]):
        self.live = live
        self.log_path = log_path
        self._csv_lock = threading.Lock()
        self._console_lock = threading.Lock()
        if self.log_path:
            new = not os.path.exists(self.log_path)
            os.makedirs(os.path.dirname(self.log_path) or ".", exist_ok=True)
            with open(self.log_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if new:
                    w.writerow([
                        "timestamp", "machine_id", "batch_no", "type",
                        "video_id", "link", "status", "error_type",
                        "s3_url", "bytes", "elapsed_sec", "error",
                    ])

    def emit(self, kind: str, msg: str) -> None:
        icon, style = LOG_STYLES.get(kind, ("•", ""))
        text = f"{icon} {msg}"
        if style:
            text = f"[{style}]{text}[/]"
        with self._console_lock:
            self.live.console.log(text)

    def csv_row(self, row: List[object]) -> None:
        if not self.log_path:
            return
        with self._csv_lock:
            with open(self.log_path, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(row)


# ── Helpers ────────────────────────────────────────────────────────────────────
YOUTUBE_ID_RE = re.compile(r"(?:v=|youtu\.be/|/shorts/|/embed/)([A-Za-z0-9_-]{11})")
COOKIE_HINTS = (
    "sign in to confirm",
    "this video is private",
    "members-only",
    "members only",
    "age-restricted",
    "age restricted",
    "login required",
    "cookies",
)


class _SilentLogger:
    """Swallow yt-dlp logging so it doesn't trample the rich Live display."""
    def debug(self, msg): pass
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass


def video_id_from_url(url: str) -> Optional[str]:
    m = YOUTUBE_ID_RE.search(url)
    if m:
        return m.group(1)
    try:
        q = parse_qs(urlparse(url).query)
        v = q.get("v", [None])[0]
        if v and len(v) >= 11:
            return v[:11]
    except Exception:
        pass
    return None


def classify_error(exc: BaseException) -> str:
    msg = str(exc).lower()
    if any(h in msg for h in COOKIE_HINTS):
        return "cookie"
    if "http error 4" in msg or "forbidden" in msg or "unauthorized" in msg:
        return "cookie"
    if "network" in msg or "timed out" in msg or "timeout" in msg or "resolve" in msg:
        return "network"
    if "unavailable" in msg or "removed" in msg or "private" in msg or "deleted" in msg:
        return "extract"
    return "other"


# ── Stats / UI state ───────────────────────────────────────────────────────────
@dataclass
class SessionStats:
    started_at: float = field(default_factory=time.time)
    batches: int = 0
    success: int = 0
    skipped: int = 0
    failed: int = 0
    cookie_errors: int = 0
    bytes_uploaded: int = 0
    last_error: str = ""
    server_stats: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, status: str, bytes_up: int = 0, err: str = "", err_type: str = ""):
        with self.lock:
            if status == "success":
                self.success += 1
            elif status == "skipped":
                self.skipped += 1
            else:
                self.failed += 1
                if err_type == "cookie":
                    self.cookie_errors += 1
                if err:
                    self.last_error = err[:120]
            self.bytes_uploaded += bytes_up

    def total(self) -> int:
        return self.success + self.skipped + self.failed


# ── Server API ─────────────────────────────────────────────────────────────────
class ServerAPI:
    def __init__(self, base_url: str, machine_id: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.machine_id = machine_id
        self.s = requests.Session()
        if not token:
            raise SystemExit("API token required: pass --token or set API_TOKEN.")
        self.s.headers.update({"Authorization": f"Bearer {token}"})

    def _retry(self, fn, *, label: str, attempts: int = 5):
        delay = 1.0
        for i in range(1, attempts + 1):
            try:
                return fn()
            except Exception as e:
                if i == attempts:
                    raise
                console.print(f"[yellow]⚠ {label} attempt {i}/{attempts} failed: {e}; retrying in {delay:.1f}s[/]")
                time.sleep(delay)
                delay = min(delay * 2, 30.0)

    def get_config(self) -> dict:
        def _do():
            r = self.s.get(f"{self.base_url}/config", timeout=30)
            r.raise_for_status()
            return r.json()["config"]
        return self._retry(_do, label="get_config")

    def get_batch(self, size: int) -> dict:
        def _do():
            r = self.s.post(
                f"{self.base_url}/batch",
                json={"machine_id": self.machine_id, "size": size},
                timeout=60,
            )
            r.raise_for_status()
            return r.json()
        return self._retry(_do, label="get_batch")

    def report(self, batch_id: str, results: list) -> dict:
        def _do():
            r = self.s.post(
                f"{self.base_url}/report",
                json={
                    "machine_id": self.machine_id,
                    "batch_id": batch_id,
                    "results": results,
                },
                timeout=60,
            )
            r.raise_for_status()
            return r.json()
        return self._retry(_do, label="report", attempts=10)


# ── S3 client ──────────────────────────────────────────────────────────────────
class S3Uploader:
    def __init__(self, aws: dict):
        self.bucket = aws["s3_bucket"]
        self.region = aws.get("region", "us-east-1")
        self._tls = threading.local()
        self._aws = aws

    def _client(self):
        c = getattr(self._tls, "client", None)
        if c is None:
            c = boto3.client(
                "s3",
                aws_access_key_id=self._aws["access_key_id"],
                aws_secret_access_key=self._aws["secret_access_key"],
                region_name=self.region,
            )
            self._tls.client = c
        return c

    def exists(self, key: str) -> bool:
        try:
            self._client().head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def upload(self, path: str, key: str) -> Tuple[str, int]:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            self._client().upload_fileobj(f, self.bucket, key)
        return f"s3://{self.bucket}/{key}", size


# ── Single-video worker ────────────────────────────────────────────────────────
@dataclass
class DLResult:
    link: str
    status: str               # success | skipped | error
    s3_url: str = ""
    error: str = ""
    error_type: str = ""
    bytes_uploaded: int = 0
    video_id: str = ""
    elapsed_sec: float = 0.0


def download_one(
    link: str,
    vtype: str,
    workdir: str,
    s3: S3Uploader,
    cookies_file: str,
    keep_files: bool,
) -> DLResult:
    t0 = time.time()
    vid = video_id_from_url(link)
    if not vid:
        return DLResult(link, "error", error="bad_url", error_type="extract",
                        elapsed_sec=time.time() - t0)

    wav_key = f"{vtype}/{vid}.wav"
    srt_key = f"{vtype}/{vid}.srt"

    # Skip-if-exists: if the WAV is already on S3, mark as skipped.
    if s3.exists(wav_key):
        return DLResult(link, "skipped", s3_url=f"s3://{s3.bucket}/{wav_key}",
                        video_id=vid, elapsed_sec=time.time() - t0)

    # Per-video temp directory so parallel workers don't collide.
    vdir = tempfile.mkdtemp(prefix=f"yt_{vid}_", dir=workdir)
    try:
        out_tmpl = os.path.join(vdir, f"{vid}.%(ext)s")
        wav_path = os.path.join(vdir, f"{vid}.wav")

        common = {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "outtmpl": out_tmpl,
            "retries": 3,
            "fragment_retries": 3,
            "logger": _SilentLogger(),
            "noprogress": True,
        }
        if cookies_file and os.path.exists(cookies_file):
            common["cookiefile"] = cookies_file

        # 1) audio → wav 16 kHz
        ydl_audio = {
            **common,
            "format": "bestaudio/best",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "wav",
                "preferredquality": "192",
            }],
            "postprocessor_args": ["-ar", "16000"],
        }
        try:
            with yt_dlp.YoutubeDL(ydl_audio) as ydl:
                ydl.download([link])
        except Exception as e:
            return DLResult(link, "error", error=str(e)[:300],
                            error_type=classify_error(e),
                            video_id=vid, elapsed_sec=time.time() - t0)

        if not os.path.exists(wav_path):
            # ffmpeg may have emitted with a slightly different name; pick first wav
            cands = glob.glob(os.path.join(vdir, "*.wav"))
            if cands:
                wav_path = cands[0]
            else:
                return DLResult(link, "error", error="wav_missing", error_type="extract",
                                video_id=vid, elapsed_sec=time.time() - t0)

        # 2) try auto-subtitle (best-effort; non-fatal)
        srt_uploaded = ""
        try:
            ydl_sub = {
                **common,
                "skip_download": True,
                "writesubtitles": False,
                "writeautomaticsub": True,
                "subtitleslangs": ["tr", "en"],
                "subtitlesformat": "srt",
            }
            with yt_dlp.YoutubeDL(ydl_sub) as ydl:
                ydl.download([link])
            srt_candidates = sorted(glob.glob(os.path.join(vdir, f"{vid}*.srt")))
            if srt_candidates:
                srt_uploaded, _ = s3.upload(srt_candidates[0], srt_key)
        except Exception:
            pass  # subtitles are optional

        # 3) upload wav
        try:
            wav_url, wav_size = s3.upload(wav_path, wav_key)
        except Exception as e:
            return DLResult(link, "error", error=f"s3:{e}"[:300], error_type="other",
                            video_id=vid, elapsed_sec=time.time() - t0)

        return DLResult(
            link, "success", s3_url=wav_url, bytes_uploaded=wav_size,
            error=("srt:" + srt_uploaded) if srt_uploaded else "",
            video_id=vid, elapsed_sec=time.time() - t0,
        )
    finally:
        if not keep_files:
            try:
                shutil.rmtree(vdir, ignore_errors=True)
            except Exception:
                pass


# ── UI rendering ───────────────────────────────────────────────────────────────
def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def fmt_seconds(s: Optional[float]) -> str:
    if not s or s < 0:
        return "—"
    s = int(s)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"


def build_dashboard(stats: SessionStats, args, current_type: str, batch_progress: Progress) -> Panel:
    elapsed = time.time() - stats.started_at
    total = stats.total()
    rate = total / elapsed if elapsed > 0 else 0.0

    server = stats.server_stats or {}
    s_total = server.get("total", 0)
    s_done = server.get("processed", 0)
    s_pct = (s_done / s_total * 100) if s_total else 0.0

    table = Table.grid(expand=True, padding=(0, 1))
    table.add_column(justify="left", style="bold cyan")
    table.add_column(justify="left")
    table.add_column(justify="left", style="bold cyan")
    table.add_column(justify="left")

    table.add_row(
        "Machine", args.machine_id,
        "Server", args.api,
    )
    table.add_row(
        "Active type", Text(current_type or "—", style="bold magenta"),
        "Batch / concurrency", f"{args.batch_size} / {args.concurrency}",
    )
    table.add_row(
        "Local processed", f"{total}",
        "Local rate", f"{rate:.2f}/s",
    )
    table.add_row(
        Text("✓ success", style="green"), f"{stats.success}",
        Text("⤼ skipped", style="yellow"), f"{stats.skipped}",
    )
    table.add_row(
        Text("✗ failed", style="red"), f"{stats.failed}",
        Text("⚿ cookie-errs", style="yellow"), f"{stats.cookie_errors}",
    )
    table.add_row(
        "Uploaded", fmt_bytes(stats.bytes_uploaded),
        "Elapsed", fmt_seconds(elapsed),
    )
    table.add_row(
        "Server total", f"{s_done:,} / {s_total:,} ({s_pct:.2f}%)",
        "Server ETA", fmt_seconds(server.get("eta_seconds")),
    )
    if server.get("remaining_per_type"):
        rpt = server["remaining_per_type"]
        table.add_row(
            "Remaining (tts/stt/tv)",
            f"{rpt.get('tts', 0):,} / {rpt.get('stt', 0):,} / {rpt.get('tv', 0):,}",
            "Machines online", f"{server.get('machines_seen', 0)}",
        )
    if stats.last_error:
        table.add_row(Text("last err", style="red"), stats.last_error, "", "")

    return Panel(
        Group(table, batch_progress),
        title="🎵 youtube-wav client",
        border_style="cyan",
    )


# ── Main loop ──────────────────────────────────────────────────────────────────
def run():
    args = parse_args()
    workdir = args.workdir or tempfile.mkdtemp(prefix="ytwav_")
    os.makedirs(workdir, exist_ok=True)

    console.print(Panel.fit(
        Text.from_markup(
            f"[bold cyan]youtube-wav client[/]\n"
            f"machine [bold]{args.machine_id}[/]  •  api [bold]{args.api}[/]\n"
            f"batch [bold]{args.batch_size}[/]  •  concurrency [bold]{args.concurrency}[/]\n"
            f"workdir [bold]{workdir}[/]"
        ),
        border_style="cyan",
    ))

    api = ServerAPI(args.api, args.machine_id, args.token)
    config = api.get_config()
    aws = config["aws"]
    s3 = S3Uploader(aws)
    console.print(f"[green]✓ AWS config loaded[/] (bucket=[bold]{aws['s3_bucket']}[/], region={aws.get('region')})")

    stats = SessionStats()
    current_type = "—"

    batch_progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=None),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>5.1f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        expand=True,
    )

    def render() -> Panel:
        return build_dashboard(stats, args, current_type, batch_progress)

    log_path = None if args.no_log_file else os.path.abspath(args.log_file)

    with Live(render(), refresh_per_second=4, console=console) as live:
        logger = EventLogger(live, log_path)
        if log_path:
            logger.emit("info", f"per-video log: [bold]{log_path}[/]")

        while True:
            try:
                resp = api.get_batch(args.batch_size)
            except Exception as e:
                stats.last_error = f"get_batch: {e}"[:120]
                logger.emit("warn", f"get_batch failed: {e}; retry in 5s")
                live.update(render())
                time.sleep(5)
                continue

            if resp.get("status") == "no_more_videos":
                stats.server_stats = resp.get("stats") or stats.server_stats
                live.update(render())
                logger.emit("info", "server reports no more videos")
                console.print("[bold green]🏁 server reports no more videos[/]")
                break

            stats.batches += 1
            batch_id = resp["batch_id"]
            current_type = resp["type"]
            links: List[str] = resp["links"]
            batch_no = stats.batches

            logger.emit(
                "batch",
                f"batch #{batch_no} [{current_type}] pulled {len(links)} links "
                f"(id {batch_id[:8]})",
            )
            batch_t0 = time.time()

            task_id = batch_progress.add_task(
                f"batch #{batch_no} [{current_type}]", total=len(links),
            )
            results: List[DLResult] = []
            done_in_batch = 0

            with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
                futures = {
                    pool.submit(
                        download_one, link, current_type, workdir, s3,
                        args.cookies, args.keep_files,
                    ): link
                    for link in links
                }
                for fut in as_completed(futures):
                    link = futures[fut]
                    try:
                        r = fut.result()
                    except Exception as e:
                        r = DLResult(
                            link, "error",
                            error=f"{type(e).__name__}:{e}"[:300],
                            error_type=classify_error(e),
                            video_id=video_id_from_url(link) or "",
                        )
                    results.append(r)
                    stats.add(
                        r.status, bytes_up=r.bytes_uploaded,
                        err=r.error, err_type=r.error_type,
                    )
                    done_in_batch += 1

                    vid_label = r.video_id or (link[-11:])
                    pos = f"#{batch_no}·{done_in_batch}/{len(links)}"
                    if r.status == "success":
                        logger.emit(
                            "success",
                            f"{pos} [{current_type}] {vid_label}  "
                            f"{fmt_bytes(r.bytes_uploaded)} in {r.elapsed_sec:.1f}s",
                        )
                    elif r.status == "skipped":
                        logger.emit(
                            "skip",
                            f"{pos} [{current_type}] {vid_label}  already on S3",
                        )
                    else:
                        # strip noisy yt-dlp prefix and trim
                        msg = (r.error or "").replace("ERROR: ", "").strip()
                        msg = re.sub(r"\s+", " ", msg)[:80]
                        logger.emit(
                            "error",
                            f"{pos} [{current_type}] {vid_label}  "
                            f"{r.error_type or 'err'}: {msg}",
                        )

                    logger.csv_row([
                        datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        args.machine_id, batch_no, current_type,
                        r.video_id, r.link, r.status, r.error_type,
                        r.s3_url, r.bytes_uploaded,
                        round(r.elapsed_sec, 2), (r.error or "")[:200],
                    ])

                    batch_progress.advance(task_id, 1)
                    live.update(render())

            ok = sum(1 for r in results if r.status == "success")
            sk = sum(1 for r in results if r.status == "skipped")
            er = sum(1 for r in results if r.status == "error")
            dt = time.time() - batch_t0
            logger.emit(
                "batch",
                f"batch #{batch_no} done in {dt:.1f}s — "
                f"[green]{ok} ok[/] / [yellow]{sk} skip[/] / [red]{er} fail[/]",
            )

            payload = [
                {
                    "link": r.link,
                    "status": r.status,
                    "s3_url": r.s3_url,
                    "error": r.error,
                    "error_type": r.error_type,
                }
                for r in results
            ]
            try:
                rep = api.report(batch_id, payload)
                stats.server_stats = rep.get("stats") or stats.server_stats
            except Exception as e:
                stats.last_error = f"report: {e}"[:120]
                logger.emit("warn", f"report failed: {e}")

            batch_progress.update(task_id, visible=False)
            live.update(render())

    console.print(f"[bold]Session done.[/] {stats.success} ok, {stats.skipped} skipped, {stats.failed} failed.")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        console.print("[yellow]interrupted[/]")
        sys.exit(130)
    except Exception:
        console.print_exception()
        sys.exit(1)
