#!/usr/bin/env python3
"""Discord Cloud: a tiny Discord-backed ZIP archive appliance.

@author MermoTEC
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import json
import logging
import math
import mimetypes
import os
import re
import secrets
import shutil
import statistics
import sys
import time
import unicodedata
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import discord
from aiohttp import web


ROOT = Path(__file__).resolve().parent


def load_dotenv(path: Path) -> None:
    """Load the simple KEY=value format used by this project's .env file."""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if value[:1] == value[-1:] and value.startswith(("'", '"')):
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_dotenv(ROOT / ".env")


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing {name}. Copy .env.example to .env and configure it.")
    return value


BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "").strip()
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0") or 0)
PASSWORD = os.getenv("DISCORD_CLOUD_PASSWORD", "")
SESSION_SECRET = os.getenv("DISCORD_CLOUD_SECRET", "")
HOST = os.getenv("DISCORD_CLOUD_HOST", "0.0.0.0")
PORT = int(os.getenv("DISCORD_CLOUD_PORT", "8080"))
TEMP_DIR = Path(os.getenv("DISCORD_CLOUD_TEMP_DIR", "/var/tmp/discord-cloud"))
MAX_UPLOAD_BYTES = int(float(os.getenv("DISCORD_CLOUD_MAX_UPLOAD_GB", "20")) * 1024**3)
HARD_MAX_UPLOAD_BYTES = int(float(os.getenv("DISCORD_CLOUD_HARD_MAX_UPLOAD_GB", "100")) * 1024**3)
HARD_MAX_CHUNK_BYTES = 100 * 1024**2
CHUNK_BYTES = int(os.getenv("DISCORD_CLOUD_CHUNK_BYTES", "9437184"))
READY_TTL = int(os.getenv("DISCORD_CLOUD_DOWNLOAD_TTL_SECONDS", "1800"))
DOWNLOAD_CONCURRENCY = max(1, min(4, int(os.getenv("DISCORD_CLOUD_DOWNLOAD_CONCURRENCY", "3"))))
RETRY_ATTEMPTS = max(1, min(10, int(os.getenv("DISCORD_CLOUD_RETRY_ATTEMPTS", "6"))))
LIBRARY_CACHE_SECONDS = max(5, int(os.getenv("DISCORD_CLOUD_LIBRARY_CACHE_SECONDS", "30")))
DEFAULT_MAX_FILES = int(os.getenv("DISCORD_CLOUD_MAX_FILES", "5000"))
COOKIE_SECURE = os.getenv("DISCORD_CLOUD_COOKIE_SECURE", "false").lower() in {"1", "true", "yes"}
SESSION_COOKIE = "discord_cloud_session"
MANIFEST_SUFFIX = ".discord_cloud.json"
MANIFEST_SCHEMA = 3
SUPPORTED_MANIFEST_SCHEMAS = {1, 2, 3}
VERSION = "3.0.0"
STATE_PATH = ROOT / "state.json"
SUSPICIOUS_EXTENSIONS = {".apk", ".app", ".bat", ".cmd", ".com", ".dll", ".dmg", ".exe", ".jar", ".js", ".msi", ".ps1", ".scr", ".sh", ".vbs"}
STARTED_AT = time.time()

if not 1_000_000 <= CHUNK_BYTES < 10_000_000:
    raise SystemExit("DISCORD_CLOUD_CHUNK_BYTES must be at least 1,000,000 and below 10,000,000.")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("discord-cloud")


class UserError(Exception):
    pass


@dataclass
class Job:
    kind: str
    name: str
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: str = "queued"
    message: str = "Queued"
    progress: int = 0
    created: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)
    started: float = field(default_factory=time.time)
    path: Path | None = None
    channel_id: int | None = None
    manifest_message_id: int | None = None
    error: str | None = None
    claimed: bool = False
    cancel_requested: bool = False
    bytes_total: int = 0
    bytes_done: int = 0
    sha256: str | None = None
    note: str = ""
    file_count: int = 0
    source_bytes: int = 0
    packaged_bytes: int = 0
    compression_ratio: float = 1.0

    def set(self, status: str, message: str, progress: int | None = None) -> None:
        self.status, self.message, self.updated = status, message, time.time()
        if progress is not None:
            self.progress = max(0, min(100, progress))

    def transferred(self, amount: int, total: int) -> None:
        self.bytes_done, self.bytes_total, self.updated = amount, total, time.time()

    def public(self) -> dict:
        elapsed = max(time.time() - self.started, 0.001)
        speed = self.bytes_done / elapsed
        eta = int((self.bytes_total - self.bytes_done) / speed) if speed and self.bytes_total > self.bytes_done else 0
        return {
            "id": self.id,
            "kind": self.kind,
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "progress": self.progress,
            "channel_id": str(self.channel_id) if self.channel_id else None,
            "manifest_message_id": str(self.manifest_message_id) if self.manifest_message_id else None,
            "error": self.error,
            "bytes_done": self.bytes_done,
            "bytes_total": self.bytes_total,
            "speed": int(speed),
            "speed_label": f"{human_bytes(int(speed))}/s" if self.bytes_done else "",
            "eta_seconds": eta,
            "cancelable": self.status in {"queued", "working"},
            "sha256": self.sha256,
            "file_count": self.file_count,
            "source_bytes": self.source_bytes,
            "packaged_bytes": self.packaged_bytes,
            "compression_ratio": round(self.compression_ratio, 3),
        }


jobs: dict[str, Job] = {}
tasks: set[asyncio.Task] = set()
upload_lock = asyncio.Lock()
download_lock = asyncio.Lock()
library_cache: dict = {"expires": 0.0, "data": []}

intents = discord.Intents.none()
intents.guilds = True
client = discord.Client(intents=intents)


def start_task(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    tasks.add(task)
    task.add_done_callback(tasks.discard)
    return task


async def discord_retry(label: str, operation, job: Job | None = None):
    """Retry transient Discord failures; discord.py already waits for normal 429 buckets."""
    last_error = None
    for attempt in range(int(setting("retry_attempts"))):
        try:
            return await operation()
        except discord.RateLimited as exc:
            last_error = exc
            delay = max(float(exc.retry_after), 1.0)
            log.warning("%s hit a Discord rate limit; waiting %.1fs", label, delay)
            if job:
                job.set("working", f"Discord rate limit; resuming {label} in {delay:.0f}s", job.progress)
            await asyncio.sleep(delay)
        except discord.HTTPException as exc:
            last_error = exc
            if exc.status != 429 and not 500 <= exc.status < 600:
                raise
            delay = max(float(getattr(exc, "retry_after", 0) or 0), min(30.0, 2 ** attempt))
            log.warning("%s failed with HTTP %s; retrying in %.1fs", label, exc.status, delay)
            if job:
                job.set("working", f"Discord is busy; retrying {label} in {delay:.0f}s", job.progress)
            await asyncio.sleep(delay)
    raise last_error or UserError(f"{label} failed after retries.")


def guild() -> discord.Guild:
    result = client.get_guild(GUILD_ID)
    if not result:
        raise UserError("The bot is not connected to the configured Discord server.")
    return result


def text_channel(channel_id: int) -> discord.TextChannel:
    channel = client.get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel) or channel.guild.id != GUILD_ID:
        raise UserError("That text channel is not in the configured Discord server.")
    return channel


def safe_filename(name: str) -> str:
    name = Path(name.replace("\\", "/")).name
    name = "".join(c for c in name if c >= " " and c not in '\r\n\0"')
    return (name or "archive.zip")[:180]


def safe_archive_path(name: str, fallback: str) -> str:
    """Create a cross-platform, traversal-safe path without touching file contents."""
    raw_parts = name.replace("\\", "/").split("/")
    if any(part == ".." for part in raw_parts):
        raise UserError("A selected file contains an unsafe parent path.")
    reserved = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}
    parts = []
    for raw in raw_parts:
        if not raw or raw == ".":
            continue
        clean = "".join(char if char >= " " and char not in '<>:"|?*\0' else "_" for char in raw).strip(" .")
        clean = clean[:120] or "file"
        if clean.split(".", 1)[0].casefold() in reserved:
            clean = "_" + clean
        parts.append(clean)
    result = "/".join(parts) or fallback
    return result[:500]


def unique_archive_path(path: str, used: set[str]) -> str:
    candidate = path
    stem, dot, suffix = path.rpartition(".")
    if not stem or "/" in suffix:
        stem, dot, suffix = path, "", ""
    number = 2
    while candidate.casefold() in used:
        candidate = f"{stem} ({number}){dot}{suffix}"
        number += 1
    used.add(candidate.casefold())
    return candidate


def safe_channel_name(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    name = re.sub(r"[^a-z0-9_-]+", "-", name).strip("-_")
    return (name or "archive")[:90]


def part_filename(original: str, part: int, total: int) -> str:
    suffix = f"-part{part}-of{total}"
    return f"{safe_filename(original)[:180 - len(suffix)]}{suffix}"


def choose_attachment(attachments, chunk: dict):
    """Resolve by immutable ID first, then tolerate Discord filename normalization and v1 manifests."""
    attachment_id = int(chunk.get("attachment_id", 0) or 0)
    if attachment_id:
        found = next((item for item in attachments if item.id == attachment_id), None)
        if found:
            return found
    expected = str(chunk.get("filename", ""))
    found = next((item for item in attachments if item.filename == expected), None)
    if found:
        return found
    folded = re.sub(r"[^a-z0-9._-]+", "_", expected.casefold())
    found = next((item for item in attachments if re.sub(r"[^a-z0-9._-]+", "_", item.filename.casefold()) == folded), None)
    return found or (attachments[0] if len(attachments) == 1 else None)


def human_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"


DEFAULT_SETTINGS = {
    "chunk_bytes": CHUNK_BYTES,
    "max_upload_bytes": min(MAX_UPLOAD_BYTES, HARD_MAX_UPLOAD_BYTES),
    "download_concurrency": DOWNLOAD_CONCURRENCY,
    "retry_attempts": RETRY_ATTEMPTS,
    "library_cache_seconds": LIBRARY_CACHE_SECONDS,
    "ready_ttl_seconds": READY_TTL,
    "compression_level": 1,
    "max_files": DEFAULT_MAX_FILES,
}


def load_state() -> dict:
    state = {"settings": dict(DEFAULT_SETTINGS), "history": []}
    try:
        saved = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(saved.get("settings"), dict):
            state["settings"].update({key: saved["settings"][key] for key in DEFAULT_SETTINGS if key in saved["settings"]})
        if isinstance(saved.get("history"), list):
            state["history"] = [item for item in saved["history"] if isinstance(item, dict)][-100:]
        for key, default in DEFAULT_SETTINGS.items():
            try:
                state["settings"][key] = int(state["settings"][key])
            except (TypeError, ValueError):
                state["settings"][key] = default
        state["settings"]["chunk_bytes"] = min(max(state["settings"]["chunk_bytes"], 1_000_000), HARD_MAX_CHUNK_BYTES)
        state["settings"]["max_upload_bytes"] = min(max(state["settings"]["max_upload_bytes"], 1024**2), HARD_MAX_UPLOAD_BYTES)
        state["settings"]["download_concurrency"] = min(max(state["settings"]["download_concurrency"], 1), 4)
        state["settings"]["retry_attempts"] = min(max(state["settings"]["retry_attempts"], 1), 10)
        state["settings"]["compression_level"] = min(max(state["settings"]["compression_level"], 0), 9)
        state["settings"]["ready_ttl_seconds"] = min(max(state["settings"]["ready_ttl_seconds"], 300), 86400)
        state["settings"]["max_files"] = min(max(state["settings"]["max_files"], 1), 20_000)
    except (FileNotFoundError, OSError, json.JSONDecodeError, AttributeError):
        pass
    return state


STATE = load_state()


def setting(name: str):
    return STATE["settings"][name]


def save_state() -> None:
    temporary = STATE_PATH.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(STATE, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, STATE_PATH)
    try:
        os.chmod(STATE_PATH, 0o600)
    except OSError:
        pass


def history_summary() -> dict:
    uploads = [item for item in STATE["history"] if item.get("kind") == "upload" and item.get("bytes_per_second", 0) > 0]
    speeds = [float(item["bytes_per_second"]) for item in uploads]
    ratios = [float(item.get("compression_ratio", 1)) for item in uploads if item.get("compression_ratio", 0) > 0]
    median_speed = int(statistics.median(speeds)) if speeds else 1024 * 1024
    median_ratio = statistics.median(ratios) if ratios else 1.0
    total = sum(int(item.get("bytes", 0)) for item in uploads)
    return {"completed_uploads": len(uploads), "median_upload_bps": median_speed, "median_upload_speed_label": f"{human_bytes(median_speed)}/s", "median_compression_ratio": round(median_ratio, 3), "total_uploaded_bytes": total, "total_uploaded_label": human_bytes(total)}


async def record_history(kind: str, **values) -> None:
    STATE["history"].append({"kind": kind, "at": datetime.now(timezone.utc).isoformat(), **values})
    STATE["history"] = STATE["history"][-100:]
    await asyncio.to_thread(save_state)


def validate_zip(path: Path) -> None:
    if not zipfile.is_zipfile(path):
        raise UserError("The uploaded file is not a valid ZIP archive.")
    with zipfile.ZipFile(path) as archive:
        bad = archive.testzip()
        if bad:
            raise UserError(f"The ZIP archive is corrupt near {bad!r}.")


def issue_session() -> str:
    expires = str(int(time.time()) + 86400)
    signature = hmac.new(SESSION_SECRET.encode(), expires.encode(), hashlib.sha256).hexdigest()
    return f"{expires}.{signature}"


def valid_session(token: str | None) -> bool:
    if not token or "." not in token:
        return False
    expires, signature = token.split(".", 1)
    if not expires.isdigit() or int(expires) < time.time():
        return False
    expected = hmac.new(SESSION_SECRET.encode(), expires.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, expected)


def csrf_for(token: str) -> str:
    return hmac.new(SESSION_SECRET.encode(), f"csrf:{token}".encode(), hashlib.sha256).hexdigest()


def check_manifest(data: object, channel_id: int | None = None) -> dict:
    if not isinstance(data, dict) or data.get("schema") not in SUPPORTED_MANIFEST_SCHEMAS:
        raise UserError("Unsupported or invalid Discord Cloud manifest.")
    required_keys = {"archive_id", "original_name", "size", "sha256", "channel_id", "chunks"}
    if not required_keys.issubset(data):
        raise UserError("The archive manifest is incomplete.")
    if channel_id is not None and int(data["channel_id"]) != channel_id:
        raise UserError("The archive manifest belongs to another channel.")
    if not isinstance(data["chunks"], list) or not data["chunks"]:
        raise UserError("The archive manifest has no chunks.")
    if len(data["chunks"]) > math.ceil(HARD_MAX_UPLOAD_BYTES / 1_000_000) + 1:
        raise UserError("The archive manifest is unreasonably large.")
    for index, chunk in enumerate(data["chunks"], 1):
        if not isinstance(chunk, dict) or not {"part", "message_id", "filename", "size", "sha256"}.issubset(chunk):
            raise UserError("The archive manifest contains an invalid chunk.")
        if int(chunk["part"]) != index or not 0 < int(chunk["size"]) <= HARD_MAX_CHUNK_BYTES:
            raise UserError("The archive manifest has invalid chunk ordering or size.")
    return data


async def manifest_from_message(message: discord.Message, channel_id: int) -> dict:
    if not client.user or message.author.id != client.user.id:
        raise UserError("The selected manifest was not created by this bot.")
    attachment = next((a for a in message.attachments if a.filename.endswith(MANIFEST_SUFFIX)), None)
    if not attachment or attachment.size > 4_000_000:
        raise UserError("The manifest attachment is missing or too large.")
    try:
        data = json.loads((await attachment.read()).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UserError("The archive manifest is unreadable.") from exc
    return check_manifest(data, channel_id)


async def list_archives(channel: discord.TextChannel) -> list[dict]:
    try:
        messages = await discord_retry("reading pinned manifests", channel.pins)
    except discord.Forbidden as exc:
        raise UserError("The bot needs Read Message History permission in that channel.") from exc
    found: list[dict] = []
    for message in messages:
        if not any(a.filename.endswith(MANIFEST_SUFFIX) for a in message.attachments):
            continue
        try:
            data = await manifest_from_message(message, channel.id)
        except UserError:
            continue
        found.append({
            "manifest_message_id": str(message.id),
            "channel_id": str(channel.id),
            "channel_name": channel.name,
            "category_id": str(channel.category_id or ""),
            "category_name": channel.category.name if channel.category else "Uncategorized",
            "discord_url": f"https://discord.com/channels/{GUILD_ID}/{channel.id}/{message.id}",
            "name": data["original_name"],
            "size": int(data["size"]),
            "size_label": human_bytes(int(data["size"])),
            "parts": len(data["chunks"]),
            "created_at": data.get("created_at"),
            "sha256": data["sha256"],
            "note": str(data.get("note", ""))[:500],
            "tags": [str(tag)[:32] for tag in data.get("tags", [])[:6]],
            "schema": data.get("schema", 1),
            "file_count": int(data.get("file_count", 1)),
            "source_bytes": int(data.get("source_bytes", data["size"])),
            "compression_ratio": float(data.get("compression_ratio", 1)),
            "inventory": data.get("inventory", [])[:500],
            "inventory_truncated": bool(data.get("inventory_truncated", False)),
            "type_summary": data.get("type_summary", []),
            "suspicious_count": int(data.get("suspicious_count", 0)),
            "content_id": str(data.get("content_id", "")),
        })
    found.sort(key=lambda item: item.get("created_at") or "", reverse=True)
    return found


async def list_library(refresh: bool = False) -> list[dict]:
    now = time.time()
    if not refresh and library_cache["expires"] > now:
        return library_cache["data"]
    found: list[dict] = []
    server = guild()
    for category in server.categories:
        for channel in category.text_channels:
            if not (channel.topic or "").startswith("Discord Cloud archive:"):
                continue
            try:
                found.extend(await list_archives(channel))
            except (UserError, discord.HTTPException):
                log.warning("Could not index cloud channel %s", channel.id, exc_info=True)
    found.sort(key=lambda item: item.get("created_at") or "", reverse=True)
    library_cache.update(expires=now + int(setting("library_cache_seconds")), data=found)
    return found


async def run_upload(job: Job, source: Path, category_id: int, requested_channel: str, note: str, tags: list[str], package: dict) -> None:
    channel: discord.TextChannel | None = None
    try:
        async with upload_lock:
            chunk_bytes = int(setting("chunk_bytes"))
            server = guild()
            discord_limit = max(1_000_000, int(server.filesize_limit) - 256 * 1024)
            if chunk_bytes > discord_limit:
                raise UserError(f"Chunk size is above this server's safe Discord limit of {human_bytes(discord_limit)}. Change it in Settings.")
            job.set("working", "Checking generated ZIP integrity", 2)
            await asyncio.to_thread(validate_zip, source)
            category = server.get_channel(category_id)
            if not isinstance(category, discord.CategoryChannel):
                raise UserError("The selected category no longer exists.")
            job.set("working", "Creating Discord channel", 5)
            channel = await server.create_text_channel(
                safe_channel_name(requested_channel),
                category=category,
                topic=f"Discord Cloud archive: {job.name}",
                reason="Discord Cloud upload",
            )
            job.channel_id = channel.id
            size = source.stat().st_size
            job.packaged_bytes = size
            job.bytes_total = size
            total_parts = math.ceil(size / chunk_bytes)
            archive_hash = hashlib.sha256()
            chunks: list[dict] = []
            sent_bytes = 0
            transfer_started = time.time()
            with source.open("rb") as handle:
                for part in range(1, total_parts + 1):
                    if job.cancel_requested:
                        raise UserError("Transfer canceled")
                    data = await asyncio.to_thread(handle.read, chunk_bytes)
                    if not data:
                        raise UserError("The generated ZIP changed while it was being uploaded.")
                    archive_hash.update(data)
                    part_hash = hashlib.sha256(data).hexdigest()
                    part_name = part_filename(job.name, part, total_parts)
                    job.set("working", f"Uploading part {part} of {total_parts}", 5 + int(88 * part / total_parts))

                    async def send_part():
                        return await channel.send(
                            content=f"Archive part `{part}/{total_parts}` - {human_bytes(len(data))}",
                            file=discord.File(io.BytesIO(data), filename=part_name),
                            silent=True,
                        )

                    message = await discord_retry(f"part {part}", send_part, job)
                    if not message.attachments:
                        raise UserError(f"Discord accepted part {part} without an attachment.")
                    attachment = message.attachments[0]
                    chunks.append({"part": part, "message_id": str(message.id), "attachment_id": str(attachment.id), "filename": attachment.filename, "display_name": part_name, "size": len(data), "sha256": part_hash})
                    sent_bytes += len(data)
                    job.transferred(sent_bytes, size)

            digest = archive_hash.hexdigest()
            job.sha256 = digest
            manifest = {
                "schema": MANIFEST_SCHEMA,
                "archive_id": digest[:20],
                "content_id": package["content_id"],
                "original_name": job.name,
                "size": size,
                "source_bytes": package["source_bytes"],
                "file_count": package["file_count"],
                "compression_ratio": package["compression_ratio"],
                "sha256": digest,
                "chunk_bytes": chunk_bytes,
                "channel_id": str(channel.id),
                "guild_id": str(server.id),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "note": note,
                "tags": tags,
                "inventory": package["inventory"][:500],
                "inventory_truncated": package["file_count"] > 500,
                "type_summary": package["type_summary"],
                "suspicious_count": package["suspicious_count"],
                "chunks": chunks,
            }
            payload = json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if len(payload) > min(2_000_000, discord_limit):
                manifest["inventory"] = manifest["inventory"][:100]
                manifest["inventory_truncated"] = True
                payload = json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            manifest_name = f"{job.name}{MANIFEST_SUFFIX}"

            async def send_manifest():
                return await channel.send(
                    content=f"Archive complete: `{job.name}` - {package['file_count']} files - {human_bytes(size)} - {total_parts} parts\nSHA-256 `{digest}`",
                    file=discord.File(io.BytesIO(payload), filename=manifest_name),
                    silent=True,
                )

            message = await discord_retry("archive manifest", send_manifest, job)
            try:
                await discord_retry("pinning archive manifest", lambda: message.pin(reason="Discord Cloud manifest"), job)
            except discord.Forbidden as exc:
                raise UserError("The bot needs Manage Messages permission so it can pin the archive manifest.") from exc
            job.manifest_message_id = message.id
            job.set("complete", f"Stored {package['file_count']} files in #{channel.name}", 100)
            library_cache["expires"] = 0
            seconds = max(time.time() - transfer_started, 0.001)
            try:
                await record_history("upload", name=job.name, bytes=size, source_bytes=package["source_bytes"], file_count=package["file_count"], parts=total_parts, chunk_bytes=chunk_bytes, seconds=round(seconds, 3), bytes_per_second=int(size / seconds), compression_ratio=package["compression_ratio"])
            except OSError:
                log.exception("Could not persist transfer history")
            log.info("Uploaded %s files as %s parts to #%s", package["file_count"], total_parts, channel.name)
    except Exception as exc:
        log.exception("Upload job %s failed", job.id)
        if channel:
            try:
                await channel.delete(reason="Rolling back incomplete Discord Cloud upload")
            except discord.HTTPException:
                log.exception("Could not delete partial channel %s", channel.id)
        if job.cancel_requested:
            job.error = None
            job.set("canceled", "Transfer canceled and partial channel removed", job.progress)
        else:
            job.error = str(exc) if isinstance(exc, UserError) else "Discord upload failed. Check the service log."
            job.set("error", job.error, job.progress)
    finally:
        source.unlink(missing_ok=True)


def append_bytes(path: Path, data: bytes) -> None:
    with path.open("ab") as handle:
        handle.write(data)


async def locate_chunk(channel: discord.TextChannel, index: int, chunk: dict, job: Job):
    try:
        message = await discord_retry(
            f"finding part {index}",
            lambda: channel.fetch_message(int(chunk["message_id"])),
            job,
        )
    except discord.NotFound as exc:
        raise UserError(f"Part {index} message is missing from Discord.") from exc
    if not client.user or message.author.id != client.user.id:
        raise UserError(f"Part {index} was not created by this bot.")
    attachment = choose_attachment(message.attachments, chunk)
    if not attachment:
        names = ", ".join(item.filename for item in message.attachments) or "no attachments"
        raise UserError(f"Part {index} attachment could not be matched. Discord returned: {names}")
    if attachment.size != int(chunk["size"]):
        raise UserError(f"Part {index} has the wrong size in Discord.")
    return attachment


async def fetch_chunk(channel: discord.TextChannel, index: int, chunk: dict, job: Job) -> tuple[int, bytes]:
    attachment = await locate_chunk(channel, index, chunk, job)
    data = await discord_retry(f"downloading part {index}", attachment.read, job)
    if hashlib.sha256(data).hexdigest() != chunk["sha256"]:
        raise UserError(f"Part {index} failed SHA-256 verification.")
    return index, data


async def run_download(job: Job, channel_id: int, manifest_message_id: int) -> None:
    partial = TEMP_DIR / f"download-{job.id}.part"
    ready = TEMP_DIR / f"download-{job.id}.zip"
    try:
        async with download_lock:
            channel = text_channel(channel_id)
            job.set("working", "Reading archive manifest", 2)
            message = await discord_retry("reading archive manifest", lambda: channel.fetch_message(manifest_message_id), job)
            manifest = await manifest_from_message(message, channel.id)
            job.name = safe_filename(str(manifest["original_name"]))
            job.sha256 = str(manifest["sha256"])
            job.note = str(manifest.get("note", ""))
            job.file_count = int(manifest.get("file_count", 1))
            job.source_bytes = int(manifest.get("source_bytes", manifest["size"]))
            job.compression_ratio = float(manifest.get("compression_ratio", 1))
            expected_size = int(manifest["size"])
            job.bytes_total = expected_size
            if expected_size > HARD_MAX_UPLOAD_BYTES:
                raise UserError("This archive exceeds the configured download limit.")
            if shutil.disk_usage(TEMP_DIR).free < expected_size + 64 * 1024**2:
                raise UserError("The Pi does not have enough temporary free space to reconstruct this archive.")
            archive_hash = hashlib.sha256()
            chunks = manifest["chunks"]
            written = 0
            for start in range(0, len(chunks), int(setting("download_concurrency"))):
                if job.cancel_requested:
                    raise UserError("Transfer canceled")
                batch = chunks[start:start + int(setting("download_concurrency"))]
                results = await asyncio.gather(*[
                    fetch_chunk(channel, start + offset + 1, chunk, job)
                    for offset, chunk in enumerate(batch)
                ])
                for index, data in sorted(results):
                    archive_hash.update(data)
                    await asyncio.to_thread(append_bytes, partial, data)
                    written += len(data)
                    job.transferred(written, expected_size)
                    job.set("working", f"Recovered part {index} of {len(chunks)}", 3 + int(90 * index / len(chunks)))
            if partial.stat().st_size != expected_size or archive_hash.hexdigest() != manifest["sha256"]:
                raise UserError("The reconstructed archive failed SHA-256 verification.")
            await asyncio.to_thread(validate_zip, partial)
            partial.replace(ready)
            job.path = ready
            job.set("ready", "Verified and ready to download", 100)
            try:
                elapsed = max(time.time() - job.started, 0.001)
                await record_history("recovery", name=job.name, bytes=expected_size, file_count=job.file_count, seconds=round(elapsed, 3), bytes_per_second=int(expected_size / elapsed))
            except OSError:
                log.exception("Could not persist recovery history")
            log.info("Reconstructed %s from #%s", job.name, channel.name)
    except Exception as exc:
        log.exception("Download job %s failed", job.id)
        partial.unlink(missing_ok=True)
        ready.unlink(missing_ok=True)
        if job.cancel_requested:
            job.error = None
            job.set("canceled", "Recovery canceled and temporary data removed", job.progress)
        else:
            job.error = str(exc) if isinstance(exc, UserError) else "Archive recovery failed. Check the service log."
            job.set("error", job.error, job.progress)


async def run_verify(job: Job, channel_id: int, manifest_message_id: int) -> None:
    try:
        channel = text_channel(channel_id)
        message = await discord_retry("reading archive manifest", lambda: channel.fetch_message(manifest_message_id), job)
        manifest = await manifest_from_message(message, channel.id)
        job.name = safe_filename(str(manifest["original_name"]))
        job.sha256 = str(manifest["sha256"])
        chunks = manifest["chunks"]
        job.bytes_total = int(manifest["size"])
        for start in range(0, len(chunks), int(setting("download_concurrency"))):
            if job.cancel_requested:
                raise UserError("Verification canceled")
            batch = chunks[start:start + int(setting("download_concurrency"))]
            await asyncio.gather(*[
                locate_chunk(channel, start + offset + 1, chunk, job)
                for offset, chunk in enumerate(batch)
            ])
            checked = min(start + len(batch), len(chunks))
            job.set("working", f"Found part {checked} of {len(chunks)}", int(100 * checked / len(chunks)))
        job.set("complete", f"All {len(chunks)} parts are present and correctly sized", 100)
    except Exception as exc:
        log.exception("Verify job %s failed", job.id)
        if job.cancel_requested:
            job.set("canceled", "Verification canceled", job.progress)
        else:
            job.error = str(exc) if isinstance(exc, UserError) else "Archive verification failed."
            job.set("error", job.error, job.progress)


async def cleanup_loop() -> None:
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for job_id, job in list(jobs.items()):
            if job.path and now - job.updated > int(setting("ready_ttl_seconds")):
                job.path.unlink(missing_ok=True)
                job.path = None
                if job.status == "ready":
                    job.set("expired", "Download expired; prepare it again", 100)
            if now - job.updated > max(int(setting("ready_ttl_seconds")) * 2, 3600) and job.status not in {"queued", "working"}:
                jobs.pop(job_id, None)


@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path in {"/login", "/healthz"} or request.path.startswith("/assets/"):
        return await handler(request)
    token = request.cookies.get(SESSION_COOKIE)
    if not valid_session(token):
        if request.path.startswith("/api/"):
            raise web.HTTPUnauthorized(text="Authentication required")
        raise web.HTTPFound("/login")
    request["session"] = token
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        if not hmac.compare_digest(request.headers.get("X-CSRF-Token", ""), csrf_for(token)):
            raise web.HTTPForbidden(text="Invalid request token")
    return await handler(request)


async def login(request: web.Request) -> web.Response:
    error = ""
    if request.method == "POST":
        data = await request.post()
        if hmac.compare_digest(str(data.get("password", "")), PASSWORD):
            token = issue_session()
            response = web.HTTPFound("/")
            response.set_cookie(
                SESSION_COOKIE,
                token,
                max_age=86400,
                httponly=True,
                secure=COOKIE_SECURE,
                samesite="Strict",
            )
            raise response
        await asyncio.sleep(0.6)
        error = "That password is not correct."
    return web.Response(text=LOGIN_HTML.replace("__ERROR__", error), content_type="text/html")


async def logout(_: web.Request) -> web.Response:
    response = web.HTTPFound("/login")
    response.del_cookie(SESSION_COOKIE)
    raise response


async def index(request: web.Request) -> web.Response:
    token = request["session"]
    html = INDEX_HTML.replace("__CSRF__", json.dumps(csrf_for(token)))
    return web.Response(text=html, content_type="text/html")


async def api_tree(_: web.Request) -> web.Response:
    server = guild()
    categories = []
    for category in sorted(server.categories, key=lambda item: item.position):
        channels = [
            {"id": str(channel.id), "name": channel.name, "cloud": (channel.topic or "").startswith("Discord Cloud archive:")}
            for channel in sorted(category.text_channels, key=lambda item: item.position)
        ]
        categories.append({"id": str(category.id), "name": category.name, "channels": channels})
    disk = shutil.disk_usage(TEMP_DIR)
    active = sum(job.status in {"queued", "working", "ready"} for job in jobs.values())
    current = STATE["settings"]
    discord_limit = max(1_000_000, int(server.filesize_limit) - 256 * 1024)
    return web.json_response({
        "guild": server.name,
        "connected": client.is_ready(),
        "categories": categories,
        "history": history_summary(),
        "system": {
            "version": VERSION,
            "uptime_seconds": int(time.time() - STARTED_AT),
            "chunk_bytes": int(current["chunk_bytes"]),
            "chunk_label": human_bytes(int(current["chunk_bytes"])),
            "discord_chunk_limit": discord_limit,
            "discord_chunk_limit_label": human_bytes(discord_limit),
            "download_concurrency": int(current["download_concurrency"]),
            "retry_attempts": int(current["retry_attempts"]),
            "compression_level": int(current["compression_level"]),
            "max_files": int(current["max_files"]),
            "max_upload_bytes": int(current["max_upload_bytes"]),
            "max_upload_label": human_bytes(int(current["max_upload_bytes"])),
            "ready_ttl_seconds": int(current["ready_ttl_seconds"]),
            "disk_free": disk.free,
            "disk_free_label": human_bytes(disk.free),
            "disk_total": disk.total,
            "active_jobs": active,
            "rate_limits": "Discord-aware pacing plus automatic retry",
            "safe_packaging": "Raw bytes are never opened, extracted, imported, or executed",
        },
    })


async def api_archives(request: web.Request) -> web.Response:
    channel_id = int(request.query.get("channel_id", "0"))
    category_id = int(request.query.get("category_id", "0"))
    channel = text_channel(channel_id)
    if channel.category_id != category_id:
        raise web.HTTPBadRequest(text="Channel/category mismatch")
    return web.json_response({"archives": await list_archives(channel)})


async def api_library(request: web.Request) -> web.Response:
    archives = await list_library(request.query.get("refresh") == "1")
    return web.json_response({
        "archives": archives,
        "summary": {
            "archives": len(archives),
            "bytes": sum(item["size"] for item in archives),
            "bytes_label": human_bytes(sum(item["size"] for item in archives)),
            "parts": sum(item["parts"] for item in archives),
            "channels": len({item["channel_id"] for item in archives}),
            "files": sum(item.get("file_count", 1) for item in archives),
            "source_bytes": sum(item.get("source_bytes", item["size"]) for item in archives),
            "source_bytes_label": human_bytes(sum(item.get("source_bytes", item["size"]) for item in archives)),
            "flagged_files": sum(item.get("suspicious_count", 0) for item in archives),
        },
    })


async def api_upload(request: web.Request) -> web.Response:
    reader = await request.multipart()
    source = TEMP_DIR / f"upload-{uuid.uuid4().hex}.zip"
    category_id = 0
    channel_name = ""
    package_name = ""
    note = ""
    tags_text = ""
    pending_path = ""
    first_path = ""
    inventory: list[dict] = []
    used_paths: set[str] = {"_discord-cloud/package-manifest.json"}
    source_bytes = 0
    free_before = shutil.disk_usage(TEMP_DIR).free
    if request.content_length and request.content_length > max(free_before - 128 * 1024**2, 0):
        raise UserError("The Pi does not have enough temporary free space for this selection.")
    max_upload = int(setting("max_upload_bytes"))
    max_files = int(setting("max_files"))
    compression_level = int(setting("compression_level"))
    zip_options = {"compression": zipfile.ZIP_STORED} if compression_level == 0 else {"compression": zipfile.ZIP_DEFLATED, "compresslevel": compression_level}
    try:
        with zipfile.ZipFile(source, "w", allowZip64=True, **zip_options) as package_zip:
            async for field in reader:
                if field.name == "category_id":
                    category_id = int((await field.text()) or 0)
                elif field.name == "channel_name":
                    channel_name = (await field.text()).strip()
                elif field.name == "package_name":
                    package_name = (await field.text()).strip()
                elif field.name == "note":
                    note = (await field.text()).strip()[:500]
                elif field.name == "tags":
                    tags_text = (await field.text()).strip()
                elif field.name == "path":
                    pending_path = (await field.text()).strip()
                elif field.name in {"file", "files"}:
                    if len(inventory) >= max_files:
                        raise web.HTTPBadRequest(text=f"Too many files; the current limit is {max_files}")
                    fallback = f"file-{len(inventory) + 1}"
                    archive_path = unique_archive_path(safe_archive_path(pending_path or field.filename or fallback, fallback), used_paths)
                    pending_path = ""
                    first_path = first_path or archive_path
                    file_hash = hashlib.sha256()
                    file_size = 0
                    with package_zip.open(archive_path, "w", force_zip64=True) as entry:
                        while chunk := await field.read_chunk(1024 * 1024):
                            source_bytes += len(chunk)
                            file_size += len(chunk)
                            if source_bytes > max_upload:
                                raise web.HTTPRequestEntityTooLarge(max_size=max_upload, actual_size=source_bytes)
                            if source_bytes % (64 * 1024**2) < len(chunk) and shutil.disk_usage(TEMP_DIR).free < 128 * 1024**2:
                                raise UserError("The Pi ran low on temporary disk space while packaging.")
                            file_hash.update(chunk)
                            await asyncio.to_thread(entry.write, chunk)
                    extension = Path(archive_path).suffix.casefold()
                    inventory.append({
                        "path": archive_path,
                        "size": file_size,
                        "sha256": file_hash.hexdigest(),
                        "type": mimetypes.guess_type(archive_path)[0] or "application/octet-stream",
                        "flagged": extension in SUSPICIOUS_EXTENSIONS,
                    })
            if not inventory:
                raise web.HTTPBadRequest(text="Choose at least one file or folder")
            content_hash = hashlib.sha256()
            for item in sorted(inventory, key=lambda value: value["path"].casefold()):
                content_hash.update(item["path"].encode("utf-8"))
                content_hash.update(str(item["size"]).encode())
                content_hash.update(item["sha256"].encode())
            content_id = content_hash.hexdigest()
            package_receipt = {
                "format": "Discord Cloud portable package",
                "version": VERSION,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "content_id": content_id,
                "source_bytes": source_bytes,
                "file_count": len(inventory),
                "safe_handling": "Files were copied as raw bytes and were not opened, extracted, imported, or executed.",
                "files": inventory,
            }
            await asyncio.to_thread(package_zip.writestr, "_discord-cloud/package-manifest.json", json.dumps(package_receipt, ensure_ascii=False, separators=(",", ":")))

        if not category_id or not channel_name:
            raise web.HTTPBadRequest(text="Category and channel name are required")
        server = guild()
        if not isinstance(server.get_channel(category_id), discord.CategoryChannel):
            raise web.HTTPBadRequest(text="Invalid category")
        if not package_name:
            base = first_path.split("/", 1)[0] if len(inventory) > 1 else Path(first_path).stem
            package_name = f"{base or 'cloud-package'}.zip"
        package_name = safe_filename(package_name)
        if not package_name.lower().endswith(".zip"):
            package_name += ".zip"
        tags = []
        for value in tags_text.split(","):
            value = re.sub(r"[^A-Za-z0-9 _.-]+", "", value.strip())[:32]
            if value and value.casefold() not in {tag.casefold() for tag in tags}:
                tags.append(value)
            if len(tags) == 6:
                break
        extension_counts: dict[str, int] = {}
        for item in inventory:
            extension = Path(item["path"]).suffix.casefold() or "no extension"
            extension_counts[extension] = extension_counts.get(extension, 0) + 1
        package_size = source.stat().st_size
        package = {
            "content_id": content_id,
            "source_bytes": source_bytes,
            "file_count": len(inventory),
            "compression_ratio": round(package_size / max(source_bytes, 1), 4),
            "inventory": inventory,
            "type_summary": [{"type": key, "count": value} for key, value in sorted(extension_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:20]],
            "suspicious_count": sum(bool(item["flagged"]) for item in inventory),
        }
        job = Job("upload", package_name, note=note, file_count=len(inventory), source_bytes=source_bytes, packaged_bytes=package_size, compression_ratio=package["compression_ratio"])
        jobs[job.id] = job
        start_task(run_upload(job, source, category_id, channel_name, note, tags, package))
        return web.json_response({"job": job.public(), "package": {"name": package_name, "source_bytes": source_bytes, "package_bytes": package_size, "file_count": len(inventory), "parts": math.ceil(package_size / int(setting("chunk_bytes"))), "compression_ratio": package["compression_ratio"], "suspicious_count": package["suspicious_count"]}}, status=202)
    except Exception:
        source.unlink(missing_ok=True)
        raise


def public_settings() -> dict:
    server = guild()
    values = dict(STATE["settings"])
    discord_limit = max(1_000_000, int(server.filesize_limit) - 256 * 1024)
    return {
        **values,
        "chunk_mb": round(int(values["chunk_bytes"]) / 1024**2, 2),
        "max_upload_gb": round(int(values["max_upload_bytes"]) / 1024**3, 2),
        "ready_ttl_minutes": round(int(values["ready_ttl_seconds"]) / 60),
        "discord_chunk_limit": discord_limit,
        "discord_chunk_limit_mb": round(discord_limit / 1024**2, 2),
        "hard_max_upload_gb": round(HARD_MAX_UPLOAD_BYTES / 1024**3),
        "history": history_summary(),
    }


async def api_settings(request: web.Request) -> web.Response:
    if request.method == "GET":
        return web.json_response({"settings": public_settings()})
    data = await request.json()
    server_limit = max(1_000_000, int(guild().filesize_limit) - 256 * 1024)
    chunk_bytes = int(float(data.get("chunk_mb", 0)) * 1024**2)
    max_upload_bytes = int(float(data.get("max_upload_gb", 0)) * 1024**3)
    download_concurrency = int(data.get("download_concurrency", 0))
    retry_attempts = int(data.get("retry_attempts", 0))
    compression_level = int(data.get("compression_level", -1))
    ready_ttl_seconds = int(float(data.get("ready_ttl_minutes", 0)) * 60)
    max_files = int(data.get("max_files", 0))
    if not 1_000_000 <= chunk_bytes <= min(server_limit, HARD_MAX_CHUNK_BYTES):
        raise UserError(f"Chunk size must be between 1 MB and this server's safe limit of {human_bytes(server_limit)}.")
    if not 1024**2 <= max_upload_bytes <= HARD_MAX_UPLOAD_BYTES:
        raise UserError(f"Maximum upload must be between 1 MB and {human_bytes(HARD_MAX_UPLOAD_BYTES)}.")
    if not 1 <= download_concurrency <= 4 or not 1 <= retry_attempts <= 10:
        raise UserError("Recovery concurrency must be 1-4 and retries must be 1-10.")
    if not 0 <= compression_level <= 9:
        raise UserError("Compression level must be between 0 and 9.")
    if not 300 <= ready_ttl_seconds <= 86400:
        raise UserError("Download expiry must be between 5 minutes and 24 hours.")
    if not 1 <= max_files <= 20_000:
        raise UserError("File count limit must be between 1 and 20,000.")
    STATE["settings"].update(chunk_bytes=chunk_bytes, max_upload_bytes=max_upload_bytes, download_concurrency=download_concurrency, retry_attempts=retry_attempts, compression_level=compression_level, ready_ttl_seconds=ready_ttl_seconds, max_files=max_files)
    await asyncio.to_thread(save_state)
    return web.json_response({"settings": public_settings()})


async def api_history(_: web.Request) -> web.Response:
    return web.json_response({"history": list(reversed(STATE["history"][-50:])), "summary": history_summary()})


async def api_clear_history(request: web.Request) -> web.Response:
    data = await request.json()
    if not hmac.compare_digest(str(data.get("password", "")), PASSWORD):
        await asyncio.sleep(0.6)
        raise web.HTTPUnauthorized(text="Incorrect dashboard password")
    STATE["history"] = []
    await asyncio.to_thread(save_state)
    return web.json_response({"ok": True})


async def api_export(_: web.Request) -> web.Response:
    payload = {"product": "Discord Cloud", "version": VERSION, "exported_at": datetime.now(timezone.utc).isoformat(), "settings": STATE["settings"], "history_summary": history_summary()}
    return web.Response(text=json.dumps(payload, indent=2), content_type="application/json", headers={"Content-Disposition": "attachment; filename=discord-cloud-settings.json", "Cache-Control": "no-store"})


async def api_prepare_download(request: web.Request) -> web.Response:
    data = await request.json()
    channel_id = int(data.get("channel_id", 0))
    manifest_message_id = int(data.get("manifest_message_id", 0))
    if not channel_id or not manifest_message_id:
        raise web.HTTPBadRequest(text="Channel and archive are required")
    text_channel(channel_id)
    job = Job("download", "archive.zip", channel_id=channel_id, manifest_message_id=manifest_message_id)
    jobs[job.id] = job
    start_task(run_download(job, channel_id, manifest_message_id))
    return web.json_response({"job": job.public()}, status=202)


async def api_verify(request: web.Request) -> web.Response:
    data = await request.json()
    channel_id = int(data.get("channel_id", 0))
    manifest_message_id = int(data.get("manifest_message_id", 0))
    text_channel(channel_id)
    job = Job("verify", "Checking archive", channel_id=channel_id, manifest_message_id=manifest_message_id)
    jobs[job.id] = job
    start_task(run_verify(job, channel_id, manifest_message_id))
    return web.json_response({"job": job.public()}, status=202)


async def api_job(request: web.Request) -> web.Response:
    job = jobs.get(request.match_info["job_id"])
    if not job:
        raise web.HTTPNotFound(text="Job not found")
    return web.json_response({"job": job.public()})


async def api_jobs(_: web.Request) -> web.Response:
    recent = sorted(jobs.values(), key=lambda item: item.updated, reverse=True)[:30]
    return web.json_response({"jobs": [job.public() for job in recent]})


async def api_cancel(request: web.Request) -> web.Response:
    job = jobs.get(request.match_info["job_id"])
    if not job:
        raise web.HTTPNotFound(text="Job not found")
    if job.status not in {"queued", "working"}:
        raise web.HTTPConflict(text="This job can no longer be canceled")
    job.cancel_requested = True
    job.set("working", "Cancel requested; finishing the current Discord request", job.progress)
    return web.json_response({"job": job.public()})


async def api_rename_channel(request: web.Request) -> web.Response:
    data = await request.json()
    channel = text_channel(int(data.get("channel_id", 0)))
    name = safe_channel_name(str(data.get("name", "")))
    await discord_retry("renaming archive channel", lambda: channel.edit(name=name, reason="Discord Cloud dashboard rename"))
    library_cache["expires"] = 0
    return web.json_response({"ok": True, "name": name})


async def api_delete_channel(request: web.Request) -> web.Response:
    data = await request.json()
    if not hmac.compare_digest(str(data.get("password", "")), PASSWORD):
        await asyncio.sleep(0.6)
        raise web.HTTPUnauthorized(text="Incorrect dashboard password")
    channel = text_channel(int(data.get("channel_id", 0)))
    if any(job.channel_id == channel.id and job.status in {"queued", "working", "ready"} for job in jobs.values()):
        raise web.HTTPConflict(text="An active transfer is using this channel")
    await channel.delete(reason="Archive deleted from Discord Cloud dashboard")
    library_cache["expires"] = 0
    return web.json_response({"ok": True})


async def send_download_notice(job: Job) -> None:
    try:
        channel = text_channel(int(job.channel_id or 0))
        when = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        link = f"https://discord.com/channels/{GUILD_ID}/{channel.id}/{job.manifest_message_id}"
        await discord_retry(
            "download audit message",
            lambda: channel.send(
                f"Downloaded from Discord Cloud: `{job.name}` at {when}\nVerified SHA-256 `{job.sha256}` - [manifest]({link})",
                silent=True,
            ),
        )
    except Exception:
        log.exception("Could not send download audit message for job %s", job.id)


async def api_download(request: web.Request) -> web.StreamResponse:
    job = jobs.get(request.match_info["job_id"])
    if not job or job.kind != "download":
        raise web.HTTPNotFound(text="Download not found")
    if job.status != "ready" or not job.path or not job.path.exists():
        raise web.HTTPConflict(text="Download is not ready")
    if job.claimed:
        raise web.HTTPConflict(text="Download already started; prepare it again if needed")
    job.claimed = True
    path = job.path
    ascii_name = re.sub(r"[^A-Za-z0-9._-]", "_", job.name) or "archive.zip"
    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "application/zip",
            "Content-Length": str(path.stat().st_size),
            "Content-Disposition": f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(job.name)}",
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )
    try:
        await response.prepare(request)
        with path.open("rb") as handle:
            while chunk := await asyncio.to_thread(handle.read, 1024 * 1024):
                await response.write(chunk)
        await response.write_eof()
        job.set("downloaded", "Sent to your browser, logged in Discord, and removed from the Pi", 100)
        start_task(send_download_notice(job))
        return response
    finally:
        path.unlink(missing_ok=True)
        job.path = None


async def graceful_shutdown() -> None:
    await asyncio.sleep(1)
    log.warning("Shutdown requested from the authenticated dashboard")
    await client.close()


async def api_shutdown(request: web.Request) -> web.Response:
    data = await request.json()
    if not hmac.compare_digest(str(data.get("password", "")), PASSWORD):
        await asyncio.sleep(0.6)
        raise web.HTTPUnauthorized(text="Incorrect dashboard password")
    start_task(graceful_shutdown())
    return web.json_response({"ok": True, "message": "Discord Cloud is shutting down"}, status=202)


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True, "discord": client.is_ready(), "version": VERSION, "uptime_seconds": int(time.time() - STARTED_AT)})


@web.middleware
async def friendly_errors(request: web.Request, handler):
    try:
        return await handler(request)
    except UserError as exc:
        return web.json_response({"error": str(exc)}, status=400)
    except (ValueError, json.JSONDecodeError):
        return web.json_response({"error": "Invalid request"}, status=400)


def build_app() -> web.Application:
    app = web.Application(middlewares=[auth_middleware, friendly_errors], client_max_size=HARD_MAX_UPLOAD_BYTES + 16 * 1024**2)
    app.router.add_route("GET", "/login", login)
    app.router.add_route("POST", "/login", login)
    app.router.add_get("/logout", logout)
    app.router.add_get("/", index)
    app.router.add_get("/api/tree", api_tree)
    app.router.add_get("/api/archives", api_archives)
    app.router.add_get("/api/library", api_library)
    app.router.add_route("GET", "/api/settings", api_settings)
    app.router.add_route("POST", "/api/settings", api_settings)
    app.router.add_get("/api/history", api_history)
    app.router.add_post("/api/history/clear", api_clear_history)
    app.router.add_get("/api/export", api_export)
    app.router.add_post("/api/upload", api_upload)
    app.router.add_post("/api/prepare-download", api_prepare_download)
    app.router.add_post("/api/verify", api_verify)
    app.router.add_get("/api/jobs", api_jobs)
    app.router.add_get("/api/jobs/{job_id}", api_job)
    app.router.add_post("/api/jobs/{job_id}/cancel", api_cancel)
    app.router.add_get("/api/jobs/{job_id}/download", api_download)
    app.router.add_post("/api/channels/rename", api_rename_channel)
    app.router.add_post("/api/channels/delete", api_delete_channel)
    app.router.add_post("/api/shutdown", api_shutdown)
    app.router.add_get("/healthz", health)
    app.router.add_static("/assets/", ROOT / "assets", show_index=False)
    return app


@client.event
async def on_ready() -> None:
    log.info("Discord ready as %s in guild %s", client.user, GUILD_ID)


def self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "demo.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("hello.txt", "Discord Cloud")
        validate_zip(path)
        assert safe_filename("../../demo.zip") == "demo.zip"
        assert safe_channel_name(" My Archive! ") == "my-archive"
        assert safe_archive_path("folder\\demo.exe", "fallback") == "folder/demo.exe"
        assert safe_archive_path("CON.txt", "fallback") == "_CON.txt"
        used = set()
        assert unique_archive_path("same.txt", used) == "same.txt"
        assert unique_archive_path("same.txt", used) == "same (2).txt"
        try:
            safe_archive_path("../escape.txt", "fallback")
            raise AssertionError("parent path was accepted")
        except UserError:
            pass
        assert part_filename("demo.zip", 1, 3) == "demo.zip-part1-of3"
        assert human_bytes(1024) == "1.0 KB"
        fake = type("Attachment", (), {"id": 7, "filename": "discord-renamed-part"})()
        assert choose_attachment([fake], {"attachment_id": "7", "filename": "old-name"}) is fake
        assert choose_attachment([fake], {"filename": "old-v1-name"}) is fake
        sample = {
            "schema": 1,
            "archive_id": "a",
            "original_name": "demo.zip",
            "size": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "channel_id": "123",
            "chunks": [{"part": 1, "message_id": "456", "filename": "demo.zip-part0001", "size": 10, "sha256": "x"}],
        }
        assert check_manifest(sample, 123)["original_name"] == "demo.zip"
    print("self-test passed")


async def main() -> None:
    global BOT_TOKEN, GUILD_ID, PASSWORD, SESSION_SECRET
    BOT_TOKEN = required("DISCORD_BOT_TOKEN")
    GUILD_ID = int(required("DISCORD_GUILD_ID"))
    PASSWORD = required("DISCORD_CLOUD_PASSWORD")
    SESSION_SECRET = required("DISCORD_CLOUD_SECRET")
    if len(PASSWORD) < 12:
        raise SystemExit("DISCORD_CLOUD_PASSWORD must be at least 12 characters.")
    if len(SESSION_SECRET) < 32:
        raise SystemExit("DISCORD_CLOUD_SECRET must be at least 32 characters.")
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    for stale in TEMP_DIR.glob("upload-*.part"):
        stale.unlink(missing_ok=True)
    for stale in TEMP_DIR.glob("download-*"):
        stale.unlink(missing_ok=True)

    runner = web.AppRunner(build_app())
    await runner.setup()
    await web.TCPSite(runner, HOST, PORT).start()
    start_task(cleanup_loop())
    log.info("Dashboard listening on http://%s:%s", HOST, PORT)
    try:
        await client.start(BOT_TOKEN)
    finally:
        for task in list(tasks):
            task.cancel()
        await runner.cleanup()
        if not client.is_closed():
            await client.close()


LOGIN_HTML = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sign in · Discord Cloud</title><style>
:root{color-scheme:dark;--ink:#eaf2ef;--muted:#8ea09a;--line:#25352f;--accent:#70f0b4;--bg:#09100e}
*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;font:15px/1.5 Inter,ui-sans-serif,system-ui,sans-serif;color:var(--ink);background:linear-gradient(#071216aa,#071216ee),url('/assets/cloud-background.webp') center/cover fixed}.login{width:min(420px,calc(100% - 32px));padding:36px;border:1px solid var(--line);background:#0e1714;box-shadow:0 24px 80px #0008}.mark{width:44px;height:44px;display:grid;place-items:center;border-radius:12px;color:#07110d;background:var(--accent);font-weight:900;font-size:20px}h1{margin:22px 0 6px;font-size:28px;letter-spacing:-.04em}.sub{margin:0 0 26px;color:var(--muted)}label{display:block;margin-bottom:8px;font-size:13px;color:#b7c6c1}input{width:100%;height:48px;padding:0 14px;color:var(--ink);background:#08100d;border:1px solid #31463e;border-radius:8px;font:inherit;outline:none}input:focus{border-color:var(--accent);box-shadow:0 0 0 3px #70f0b422}button{width:100%;height:48px;margin-top:14px;border:0;border-radius:8px;background:var(--accent);color:#06110d;font:700 15px inherit;cursor:pointer;transition:.18s transform,.18s filter}button:hover{filter:brightness(1.08);transform:translateY(-1px)}.error{min-height:24px;margin:12px 0 0;color:#ff9d98;font-size:13px}@media(prefers-reduced-motion:reduce){*{transition:none!important}}

</style></head><body><main class="login"><div class="mark">DC</div><h1>Discord Cloud</h1><p class="sub">Private access to your Discord archive.</p><form method="post"><label for="password">Dashboard password</label><input id="password" name="password" type="password" autocomplete="current-password" autofocus required><button>Unlock cloud</button><p class="error" role="alert">__ERROR__</p></form></main></body></html>'''


INDEX_HTML = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Discord Cloud</title><style>
:root{color-scheme:dark;--bg:#07100d;--surface:#0d1713;--surface2:#111f19;--ink:#eef7f3;--muted:#8ea59c;--line:#263d33;--accent:#72f2b8;--blue:#60c4ff;--danger:#ff7f78;--warn:#ffd166;--shadow:0 24px 80px #0007}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;min-height:100vh;background:var(--bg);color:var(--ink);font:14px/1.5 Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;background-image:linear-gradient(#13231c55 1px,transparent 1px),linear-gradient(90deg,#13231c55 1px,transparent 1px);background-size:40px 40px}button,input,select,textarea{font:inherit}button,a,input,select,textarea{outline:none}button:focus-visible,a:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible,.drop:focus-visible{box-shadow:0 0 0 3px #72f2b833;border-color:var(--accent)!important}.shell{max-width:1220px;margin:auto;padding:25px 24px 64px}.topbar{display:flex;justify-content:space-between;align-items:center;gap:20px;margin-bottom:36px}.brand{display:flex;align-items:center;gap:12px}.mark{width:40px;height:40px;display:grid;place-items:center;border-radius:11px;background:var(--accent);color:#06110d;font-weight:950}.brand strong,.brand small{display:block}.brand small{color:var(--muted)}.top-actions{display:flex;align-items:center;gap:10px}.status{display:flex;align-items:center;gap:7px;color:var(--muted);font-size:12px}.dot{width:8px;height:8px;border-radius:50%;background:var(--warn);box-shadow:0 0 0 4px #ffd16618}.dot.online{background:var(--accent);box-shadow:0 0 0 4px #72f2b818}.linkbtn{height:36px;padding:0 11px;border:1px solid transparent;border-radius:7px;background:transparent;color:var(--muted);text-decoration:none;cursor:pointer}.linkbtn:hover{background:#fff1;color:var(--ink)}.hero{display:grid;grid-template-columns:1.2fr .8fr;align-items:end;gap:36px;margin-bottom:28px}.eyebrow{margin:0 0 9px;color:var(--accent);font-size:11px;font-weight:850;letter-spacing:.14em;text-transform:uppercase}.hero h1{margin:0;font-size:clamp(38px,5vw,62px);line-height:.98;letter-spacing:-.055em}.hero p:last-child{margin:0;color:var(--muted);font-size:15px;max-width:430px}.stats{display:grid;grid-template-columns:repeat(4,1fr);border:1px solid var(--line);background:var(--surface);margin-bottom:25px;box-shadow:var(--shadow)}.stat{padding:16px 18px;border-right:1px solid var(--line)}.stat:last-child{border:0}.stat span,.stat strong{display:block}.stat span{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.1em}.stat strong{margin-top:3px;font-size:19px;letter-spacing:-.03em}.tabs{display:flex;overflow:auto;border-bottom:1px solid var(--line);margin-bottom:24px}.tab{position:relative;padding:12px 17px;border:0;background:none;color:var(--muted);white-space:nowrap;cursor:pointer}.tab.active{color:var(--ink)}.tab.active:after{content:"";position:absolute;left:14px;right:14px;bottom:-1px;height:2px;background:var(--accent)}.panel{display:none;animation:rise .25s ease}.panel.active{display:block}.workspace{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:20px}.tool{border:1px solid var(--line);background:var(--surface);box-shadow:var(--shadow)}.main{padding:25px}.side{padding:22px;background:var(--surface2)}.section-title{display:flex;justify-content:space-between;align-items:center;gap:14px;margin-bottom:19px}.section-title h2{margin:0;font-size:20px;letter-spacing:-.03em}.kicker{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.12em}.drop{min-height:175px;display:grid;place-items:center;text-align:center;padding:22px;border:1px dashed #3e6151;border-radius:11px;background:#08110d;cursor:pointer;transition:.18s}.drop:hover,.drop.drag{border-color:var(--accent);background:#0b1812;transform:translateY(-1px)}.drop svg{width:34px;color:var(--accent)}.drop strong,.drop span{display:block}.drop strong{margin-top:8px;font-size:16px}.drop span{margin-top:4px;color:var(--muted)}.file-pill{display:none;justify-content:space-between;align-items:center;gap:12px;margin-top:11px;padding:11px 13px;border:1px solid var(--line);border-radius:8px;background:#09120f}.file-pill.visible{display:flex}.file-pill strong,.file-pill small{display:block}.file-pill small{color:var(--muted)}.grid2,.grid3{display:grid;gap:13px;margin-top:16px}.grid2{grid-template-columns:1fr 1fr}.grid3{grid-template-columns:1fr 1fr 1fr}label{display:block;margin-bottom:6px;color:#bdcdc6;font-size:12px;font-weight:700}input,select,textarea{width:100%;border:1px solid #304a3f;border-radius:8px;background:#07100d;color:var(--ink)}input,select{height:43px;padding:0 12px}textarea{min-height:78px;padding:10px 12px;resize:vertical}select:disabled,input:disabled{opacity:.5}.primary,.secondary,.danger{height:43px;padding:0 15px;border-radius:8px;font-weight:800;cursor:pointer;transition:.18s}.primary{border:0;background:var(--accent);color:#05100c}.primary:hover:not(:disabled){filter:brightness(1.08);transform:translateY(-1px)}.primary:disabled{opacity:.42;cursor:not-allowed}.secondary{border:1px solid #3a5b4c;background:#10231b;color:var(--accent)}.secondary:hover{background:#173127}.danger{border:1px solid #6d3c39;background:#261514;color:#ffaaa5}.danger:hover{background:#351a18}.wide{width:100%;margin-top:16px}.ghost{border:0;background:none;color:var(--muted);cursor:pointer}.ghost:hover{color:var(--danger)}.fact{display:flex;gap:10px;margin:15px 0}.fact b{width:24px;height:24px;display:grid;place-items:center;flex:0 0 auto;border:1px solid #365546;border-radius:50%;color:var(--accent);font-size:11px}.fact small{display:block;color:var(--muted)}.notice{margin:18px 0 0;padding-top:16px;border-top:1px solid var(--line);color:var(--muted);font-size:12px}.job{display:none;margin-top:16px;padding:15px;border:1px solid var(--line);border-radius:9px;background:#08110e}.job.visible{display:block}.job.error{border-color:#703d39}.job-head,.job-foot{display:flex;justify-content:space-between;align-items:center;gap:12px}.job-head strong{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.job-head span,.job-foot{color:var(--muted);font-size:12px}.track{height:6px;margin:10px 0;overflow:hidden;border-radius:99px;background:#21352c}.bar{height:100%;width:0;background:var(--accent);transition:width .35s}.job.error .bar{background:var(--danger)}.filters{display:grid;grid-template-columns:1.2fr 1fr 1fr .8fr auto;gap:10px;align-items:end;margin-bottom:17px}.archive-list{display:grid;gap:9px}.archive{padding:16px;border:1px solid var(--line);border-radius:10px;background:#09120f;transition:.18s}.archive:hover{border-color:#426958;transform:translateY(-1px)}.archive-top{display:flex;justify-content:space-between;gap:20px}.archive h3{margin:0 0 3px;font-size:15px;overflow-wrap:anywhere}.meta,.note{color:var(--muted);font-size:12px}.note{margin:9px 0 0;color:#b7c8c1}.tags{display:flex;flex-wrap:wrap;gap:6px;margin-top:9px}.tag{padding:3px 7px;border:1px solid #315344;border-radius:99px;color:#a9e8ca;font-size:10px}.actions{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:7px}.actions button,.actions a{height:34px;padding:0 10px;display:inline-grid;place-items:center;border:1px solid #355647;border-radius:7px;background:#10221b;color:#bcefd7;text-decoration:none;font:700 11px inherit;cursor:pointer}.actions .download{background:var(--accent);color:#06110d;border-color:var(--accent)}.actions .delete{color:#ffaaa5;border-color:#653a37;background:#211413}.empty{padding:48px 18px;text-align:center;border:1px dashed var(--line);border-radius:10px;color:var(--muted)}.empty strong{display:block;color:#c7d5cf;margin-bottom:4px}.activity-list{display:grid;gap:8px}.activity{display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:12px;padding:13px;border-bottom:1px solid var(--line)}.activity:last-child{border:0}.badge{padding:4px 7px;border-radius:6px;background:#173126;color:var(--accent);font-size:10px;text-transform:uppercase}.activity small{display:block;color:var(--muted)}.system-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--line);border:1px solid var(--line)}.system-item{padding:17px;background:var(--surface)}.system-item small,.system-item strong{display:block}.system-item small{color:var(--muted)}.system-item strong{margin-top:4px}.danger-zone{display:flex;justify-content:space-between;align-items:center;gap:20px;margin-top:22px;padding:20px;border:1px solid #5d3431;background:#160e0d}.danger-zone h3,.danger-zone p{margin:0}.danger-zone p{color:#c89c99;font-size:12px}.toast{position:fixed;right:22px;bottom:22px;z-index:20;max-width:min(400px,calc(100% - 44px));padding:13px 16px;border:1px solid #3b5d4e;border-radius:8px;background:#10221a;box-shadow:var(--shadow);transform:translateY(18px);opacity:0;pointer-events:none;transition:.22s}.toast.show{opacity:1;transform:none}.toast.bad{border-color:#6f3c38;color:#ffc1bd}.modal-wrap{position:fixed;inset:0;z-index:30;display:none;place-items:center;padding:18px;background:#000b}.modal-wrap.open{display:grid}.modal{width:min(440px,100%);padding:24px;border:1px solid var(--line);background:var(--surface);box-shadow:var(--shadow);animation:rise .2s}.modal h3{margin:0 0 5px;font-size:21px}.modal p{margin:0 0 17px;color:var(--muted)}.modal-actions{display:flex;justify-content:flex-end;gap:9px;margin-top:15px}.offline{min-height:100vh;display:grid;place-items:center;text-align:center;padding:30px}.offline h1{font-size:38px;margin:0}.offline p{color:var(--muted)}@keyframes rise{from{opacity:0;transform:translateY(7px)}to{opacity:1;transform:none}}@media(max-width:900px){.hero{grid-template-columns:1fr}.workspace{grid-template-columns:1fr}.side{display:grid;grid-template-columns:1fr 1fr;gap:0 22px}.notice{grid-column:1/-1}.filters{grid-template-columns:1fr 1fr}.filters .search{grid-column:1/-1}.system-grid{grid-template-columns:1fr 1fr}}@media(max-width:600px){.shell{padding:18px 13px 42px}.topbar{align-items:flex-start}.status span,.version-label{display:none}.hero h1{font-size:39px}.stats{grid-template-columns:1fr 1fr}.stat:nth-child(2){border-right:0}.stat:nth-child(-n+2){border-bottom:1px solid var(--line)}.main,.side{padding:17px}.side,.grid2,.grid3,.filters,.system-grid{grid-template-columns:1fr}.archive-top{display:block}.actions{justify-content:flex-start;margin-top:13px}.activity{grid-template-columns:1fr}.danger-zone{display:block}.danger-zone button{width:100%;margin-top:14px}}@media(prefers-reduced-motion:reduce){*,*:before,*:after{animation:none!important;transition:none!important;scroll-behavior:auto!important}}

body{background:#071216;background-image:none!important}body:before{content:"";position:fixed;inset:0;z-index:-2;background:url('/assets/cloud-background.webp') center/cover no-repeat}body:after{content:"";position:fixed;inset:0;z-index:-1;background:linear-gradient(180deg,#071216aa,#071216f2 44%,#071216)}.tool{border-radius:14px;background:#0e1b1fdd;backdrop-filter:blur(14px)}.hero{min-height:230px;padding:30px;border:1px solid #d8f7ff35;border-radius:18px;background:linear-gradient(110deg,#07171be0,#0b263080 58%,#dff8ff16);box-shadow:var(--shadow)}.hero-copy{color:#d2e0e3!important}.trust{display:inline-flex;margin-top:14px;padding:6px 10px;border:1px solid #bdfbe64a;border-radius:99px;background:#07181699;color:#caffed;font-size:11px}.pick-actions{display:flex;gap:9px;margin-bottom:10px}.pick-actions>*{flex:1}.selection{display:none;margin-top:12px;padding:14px;border:1px solid var(--line);border-radius:10px;background:#081519}.selection.visible{display:block}.selection-head{display:flex;justify-content:space-between;gap:14px}.selection-head small{display:block;color:var(--muted)}.mini-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:11px}.mini{padding:9px;border:1px solid #294149;border-radius:7px}.mini small,.mini strong{display:block}.mini small{color:var(--muted);font-size:9px;text-transform:uppercase}.file-preview{max-height:150px;overflow:auto;margin-top:9px;border-top:1px solid var(--line)}.file-row{display:flex;justify-content:space-between;gap:12px;padding:6px 1px;border-bottom:1px solid #20343a;font-size:11px}.file-row span:first-child{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.flag{color:#ffd09c}.safe-note{padding:12px;border-left:3px solid var(--accent);background:#0a1d1b;color:#c6d8d7;font-size:11px}.safe-note strong{color:#dffff5}.activity-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.chart{height:180px;display:flex;align-items:end;gap:7px;padding:18px 12px 22px;border:1px solid var(--line);background:#071418}.chart-col{flex:1;min-width:7px;border-radius:5px 5px 0 0;background:linear-gradient(var(--accent2),#31566b);position:relative}.chart-col:hover:after{content:attr(data-label);position:absolute;left:50%;bottom:calc(100% + 5px);transform:translateX(-50%);padding:4px 6px;border-radius:5px;background:#061014;font-size:9px;white-space:nowrap}.settings-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:13px}.preset-row{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px}.contents{display:grid;gap:1px;background:var(--line);border:1px solid var(--line)}.content-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;padding:8px;background:#0a181c;font-size:11px}.modal{max-height:85vh;overflow:auto}.archive .warn{border-color:#7b5a36;color:#ffd09c}.stats{grid-template-columns:repeat(5,1fr)}@media(max-width:900px){.activity-grid{grid-template-columns:1fr}.settings-grid{grid-template-columns:1fr 1fr}.stats{grid-template-columns:repeat(3,1fr)}}@media(max-width:560px){.pick-actions,.mini-stats,.settings-grid{display:grid;grid-template-columns:1fr}.stats{grid-template-columns:1fr 1fr}}</style></head><body><div class="shell"><header class="topbar"><div class="brand"><div class="mark">DC</div><div><strong>Discord Cloud</strong><small id="guildName">Connecting...</small></div></div><div class="top-actions"><div class="status"><i id="dot" class="dot"></i><span id="connectionText">Discord offline</span></div><span class="linkbtn version-label" id="version">v3</span><a class="linkbtn" href="/logout">Lock</a></div></header><section class="hero"><div><p class="eyebrow">Your Discord storage appliance</p><h1>Everything goes in. Nothing runs.</h1><span class="trust">Raw bytes only ? Portable ZIP ? SHA-256 verified</span></div><p>Drop any files or whole folders. Discord Cloud wraps them into one portable ZIP, stores it in Discord, and restores it without local retention.</p></section><section class="stats"><div class="stat"><span>Archives</span><strong id="statArchives">-</strong></div><div class="stat"><span>Files protected</span><strong id="statFiles">-</strong></div><div class="stat"><span>Stored</span><strong id="statStored">-</strong></div><div class="stat"><span>Parts</span><strong id="statParts">-</strong></div><div class="stat"><span>Pi free space</span><strong id="statDisk">-</strong></div></section><nav class="tabs" aria-label="Cloud sections"><button class="tab active" data-tab="store">Store</button><button class="tab" data-tab="library">Library</button><button class="tab" data-tab="activity">Activity</button><button class="tab" data-tab="settings">Settings</button></nav>
<section id="store" class="panel active"><div class="workspace"><div class="tool main"><div class="section-title"><h2>Build a cloud package</h2><span class="kicker">Any files ? Any folders</span></div><input id="filePicker" type="file" multiple hidden><input id="folderPicker" type="file" webkitdirectory multiple hidden><div class="pick-actions"><button id="chooseFiles" class="secondary" type="button">Choose files</button><button id="chooseFolder" class="secondary" type="button">Choose folder</button></div><div id="drop" class="drop" tabindex="0" role="button"><div><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7"><path d="M4 7h6l2 2h8v9a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V7Z"/><path d="M12 16v-5m0 0-2 2m2-2 2 2"/></svg><strong>Drop files here</strong><span>Use Choose folder to preserve every relative path</span></div></div><div id="selection" class="selection"><div class="selection-head"><div><strong id="selectionTitle"></strong><small id="selectionWarning"></small></div><button id="clearSelection" class="ghost">Clear</button></div><div class="mini-stats"><div class="mini"><small>Raw size</small><strong id="selRaw">-</strong></div><div class="mini"><small>Est. ZIP</small><strong id="selZip">-</strong></div><div class="mini"><small>Est. parts</small><strong id="selParts">-</strong></div><div class="mini"><small>Est. time</small><strong id="selEta">-</strong></div></div><div id="filePreview" class="file-preview"></div></div><form id="uploadForm"><div class="grid2"><div><label for="packageName">Final ZIP name</label><input id="packageName" maxlength="180" placeholder="my-cloud-package.zip" required></div><div><label for="uploadCategory">Discord category</label><select id="uploadCategory" required><option value="">Choose category</option></select></div></div><div class="grid2"><div><label for="channelName">New Discord channel</label><input id="channelName" maxlength="90" required></div><div><label for="tags">Tags</label><input id="tags" maxlength="200" placeholder="photos, project, 2026"></div></div><div class="grid2"><div><label for="note">Archive note</label><textarea id="note" maxlength="500"></textarea></div><div class="safe-note"><strong>No-execution guarantee</strong><br>Discord Cloud only copies bytes into a ZIP. A harmful file cannot run here, but may still be harmful if opened after extraction.</div></div><button id="uploadButton" class="primary wide" disabled>Package and store in Discord</button></form><div id="localJob" class="job"><div class="job-head"><strong>Sending to Pi</strong><span>0%</span></div><div class="track"><div class="bar"></div></div><div class="job-foot"><span class="message">Waiting</span></div></div><div id="uploadJob" class="job"><div class="job-head"><strong></strong><span>0%</span></div><div class="track"><div class="bar"></div></div><div class="job-foot"><span class="message"></span><button class="ghost cancel">Cancel</button></div></div></div><aside class="tool side"><p class="kicker">Safe packaging</p><div class="fact"><b>1</b><span>Never executed<small>No opening, importing, extracting, or previewing.</small></span></div><div class="fact"><b>2</b><span>Streamed ZIP64<small>Large packages stay memory-safe on Pi 4.</small></span></div><div class="fact"><b>3</b><span>Portable receipt<small>Names, sizes, and hashes travel inside the ZIP.</small></span></div><div class="fact"><b>4</b><span>Learned estimates<small>Predictions improve after every upload.</small></span></div><div class="fact"><b>5</b><span>Discord-aware limits<small>Chunk settings cannot exceed server capacity.</small></span></div><p class="notice">Browser folder selection preserves all files and relative paths. Empty folders contain no browser-uploadable item and are omitted.</p></aside></div></section><section id="library" class="panel"><div class="tool main"><div class="section-title"><h2>Cloud library</h2><button id="refreshLibrary" class="secondary">Refresh Discord</button></div><div class="filters"><div class="search"><label for="search">Search name, note, or tag</label><input id="search" type="search" placeholder="Search archives"></div><div><label for="filterCategory">Category</label><select id="filterCategory"><option value="">All categories</option></select></div><div><label for="filterChannel">Channel</label><select id="filterChannel"><option value="">All channels</option></select></div><div><label for="sort">Sort</label><select id="sort"><option value="new">Newest</option><option value="old">Oldest</option><option value="name">Name</option><option value="size">Largest</option><option value="files">Most files</option></select></div><div><label>&nbsp;</label><button id="clearFilters" class="secondary">Clear</button></div></div><div id="downloadJob" class="job"><div class="job-head"><strong></strong><span>0%</span></div><div class="track"><div class="bar"></div></div><div class="job-foot"><span class="message"></span><button class="ghost cancel" type="button">Cancel</button></div></div><div id="archiveList" class="archive-list"><div class="empty"><strong>Loading cloud library...</strong>Reading pinned Discord manifests.</div></div></div></section>
<section id="activity" class="panel"><div class="activity-grid"><div class="tool main"><div class="section-title"><h2>Current activity</h2><button id="refreshActivity" class="secondary">Refresh</button></div><div id="activityList" class="activity-list"><div class="empty">No active transfers.</div></div></div><div class="tool main"><div class="section-title"><h2>Upload throughput</h2><span id="historySpeed" class="kicker">Learning...</span></div><div id="speedChart" class="chart"></div></div></div><div class="tool main" style="margin-top:18px"><div class="section-title"><h2>Persistent history</h2><span class="kicker">Last 50 operations</span></div><div id="historyList" class="activity-list"></div></div></section><section id="settings" class="panel"><div class="tool main"><div class="section-title"><h2>Transfer settings</h2><span id="discordLimit" class="kicker"></span></div><div class="preset-row"><button class="secondary preset" data-preset="fast">Fast Pi</button><button class="secondary preset" data-preset="balanced">Balanced</button><button class="secondary preset" data-preset="compact">Compact ZIP</button><a class="secondary" href="/api/export" style="display:inline-grid;place-items:center;text-decoration:none">Export settings</a></div><form id="settingsForm"><div class="settings-grid"><div><label>Chunk size (MiB)</label><input id="setChunk" type="number" min="1" step="0.25"></div><div><label>Maximum selection (GiB)</label><input id="setMaxUpload" type="number" min="0.001" step="0.25"></div><div><label>ZIP compression</label><select id="setCompression"><option value="0">0 ? Fastest</option><option value="1">1 ? Fast</option><option value="3">3 ? Light</option><option value="6">6 ? Balanced</option><option value="9">9 ? Maximum</option></select></div><div><label>Parallel recovery</label><input id="setConcurrency" type="number" min="1" max="4"></div><div><label>Retry attempts</label><input id="setRetries" type="number" min="1" max="10"></div><div><label>Download expiry (minutes)</label><input id="setTtl" type="number" min="5" max="1440"></div><div><label>Maximum files</label><input id="setMaxFiles" type="number" min="1" max="20000"></div></div><button class="primary wide">Save settings</button></form><div id="systemGrid" class="system-grid"></div><div class="danger-zone"><div><h3>Administrative controls</h3><p>History clearing and shutdown require your password.</p></div><div class="pick-actions"><button id="clearHistory" class="secondary">Clear history</button><button id="shutdown" class="danger">Turn off app</button></div></div></div></section></div><div id="toast" class="toast" role="status"></div><div id="passwordModal" class="modal-wrap" role="dialog" aria-modal="true"><form id="passwordForm" class="modal"><h3 id="modalTitle">Confirm action</h3><p id="modalText"></p><label for="confirmPassword">Dashboard password</label><input id="confirmPassword" type="password" autocomplete="current-password" required><div class="modal-actions"><button id="modalCancel" type="button" class="secondary">Cancel</button><button class="danger">Confirm</button></div></form></div>
<div id="contentsModal" class="modal-wrap" role="dialog" aria-modal="true"><div class="modal"><h3 id="contentsTitle">Package contents</h3><p id="contentsMeta"></p><div id="contentsList" class="contents"></div><div class="modal-actions"><button id="contentsClose" class="secondary">Close</button></div></div></div><script>
const CSRF=__CSRF__,dangerExt=new Set(['apk','app','bat','cmd','com','dll','dmg','exe','jar','js','msi','ps1','scr','sh','vbs']);let tree=null,settings=null,library=[],selectedFiles=[],pendingPasswordAction=null;const $=s=>document.querySelector(s),$$=s=>document.querySelectorAll(s),sleep=ms=>new Promise(r=>setTimeout(r,ms));const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function toast(m,bad=false){const e=$('#toast');e.textContent=m;e.className='toast show'+(bad?' bad':'');clearTimeout(e.t);e.t=setTimeout(()=>e.className='toast',4300)}function bytes(n){const u=['B','KB','MB','GB','TB'];let i=0;while(n>=1024&&i<u.length-1){n/=1024;i++}return `${n.toFixed(i?1:0)} ${u[i]}`}function duration(s){if(!Number.isFinite(s)||s<=0)return 'n/a';if(s<60)return `${Math.ceil(s)}s`;if(s<3600)return `${Math.ceil(s/60)}m`;return `${(s/3600).toFixed(1)}h`}function filePath(f){return f.webkitRelativePath||f.name}function flagged(p){return dangerExt.has(p.split('.').pop().toLowerCase())}
async function api(url,opt={}){opt.headers={...(opt.headers||{}),'X-CSRF-Token':CSRF};const r=await fetch(url,opt);if(r.status===401&&!/delete|shutdown|clear/.test(url)){location='/login';return}if(!r.ok){let m;try{m=(await r.json()).error}catch{m=await r.text()}throw new Error(m||`Request failed (${r.status})`)}return r.json()}
function uploadRequest(data,progress){return new Promise((ok,bad)=>{const x=new XMLHttpRequest();x.open('POST','/api/upload');x.setRequestHeader('X-CSRF-Token',CSRF);x.upload.onprogress=e=>{if(e.lengthComputable)progress(Math.round(e.loaded/e.total*100),`Sending ${bytes(e.loaded)} of ${bytes(e.total)} to the Pi`)};x.onload=()=>{let b={};try{b=JSON.parse(x.responseText)}catch{}x.status>=200&&x.status<300?ok(b):bad(new Error(b.error||x.responseText||`Upload failed (${x.status})`))};x.onerror=()=>bad(new Error('Connection to the Pi was interrupted'));x.send(data)})}
function fillCategories(){const o=tree.categories.map(c=>`<option value="${c.id}">${esc(c.name)}</option>`).join('');$('#uploadCategory').innerHTML='<option value="">Choose category</option>'+o;$('#filterCategory').innerHTML='<option value="">All categories</option>'+o;updateChannels()}function updateChannels(){const cat=$('#filterCategory').value,chs=tree.categories.flatMap(c=>c.channels.map(x=>({...x,cat:c.id}))).filter(c=>c.cloud&&(!cat||c.cat===cat));$('#filterChannel').innerHTML='<option value="">All channels</option>'+chs.map(c=>`<option value="${c.id}"># ${esc(c.name)}</option>`).join('')}
async function loadTree(){tree=await api('/api/tree');$('#guildName').textContent=tree.guild;$('#dot').classList.toggle('online',tree.connected);$('#connectionText').textContent=tree.connected?'Discord connected':'Discord offline';$('#version').textContent='v'+tree.system.version;$('#statDisk').textContent=tree.system.disk_free_label;fillCategories();renderSystem();updateSelection()}
async function loadSettings(){settings=(await api('/api/settings')).settings;$('#setChunk').value=settings.chunk_mb;$('#setChunk').max=settings.discord_chunk_limit_mb;$('#setMaxUpload').value=settings.max_upload_gb;$('#setCompression').value=settings.compression_level;$('#setConcurrency').value=settings.download_concurrency;$('#setRetries').value=settings.retry_attempts;$('#setTtl').value=settings.ready_ttl_minutes;$('#setMaxFiles').value=settings.max_files;$('#discordLimit').textContent=`Discord safe maximum: ${settings.discord_chunk_limit_mb} MiB`;updateSelection()}
async function loadLibrary(refresh=false){try{const d=await api('/api/library'+(refresh?'?refresh=1':''));library=d.archives;$('#statArchives').textContent=d.summary.archives;$('#statFiles').textContent=d.summary.files;$('#statStored').textContent=d.summary.bytes_label;$('#statParts').textContent=d.summary.parts;renderLibrary()}catch(e){$('#archiveList').innerHTML=`<div class="empty"><strong>Library unavailable</strong>${esc(e.message)}</div>`;toast(e.message,true)}}
function setFiles(files){selectedFiles=[...files];if(!tree)return;const total=selectedFiles.reduce((n,f)=>n+f.size,0);if(selectedFiles.length>tree.system.max_files){selectedFiles=[];return toast(`Limit: ${tree.system.max_files} files. Change it in Settings.`,true)}if(total>tree.system.max_upload_bytes){selectedFiles=[];return toast(`Selection exceeds ${tree.system.max_upload_label}.`,true)}if(selectedFiles.length&&!$('#packageName').value){const p=filePath(selectedFiles[0]),base=p.includes('/')?p.split('/')[0]:selectedFiles.length===1?selectedFiles[0].name.replace(/\.[^.]+$/,''):'cloud-package';$('#packageName').value=(base||'cloud-package')+'.zip';$('#channelName').value=(base||'cloud-package').replace(/[^a-z0-9]+/gi,'-').replace(/^-|-$/g,'').toLowerCase().slice(0,90)}updateSelection()}
function updateSelection(){const box=$('#selection');if(!selectedFiles.length){box.classList.remove('visible');$('#uploadButton').disabled=true;return}box.classList.add('visible');const raw=selectedFiles.reduce((n,f)=>n+f.size,0),ratio=Math.min(1.08,Math.max(.05,tree?.history?.median_compression_ratio||1)),pack=Math.max(128,Math.round(raw*ratio+selectedFiles.length*110)),parts=Math.ceil(pack/(tree?.system?.chunk_bytes||9437184)),eta=pack/(tree?.history?.median_upload_bps||1048576)+raw/(30*1024*1024),bad=selectedFiles.filter(f=>flagged(filePath(f))).length;$('#selectionTitle').textContent=`${selectedFiles.length} file${selectedFiles.length===1?'':'s'} selected`;$('#selectionWarning').textContent=bad?`${bad} executable/script file${bad===1?'':'s'} will stay quarantined inside the ZIP`:'Every item will be copied as raw bytes';$('#selRaw').textContent=bytes(raw);$('#selZip').textContent='~'+bytes(pack);$('#selParts').textContent=parts;$('#selEta').textContent='~'+duration(eta);$('#filePreview').innerHTML=selectedFiles.slice(0,10).map(f=>`<div class="file-row"><span class="${flagged(filePath(f))?'flag':''}">${esc(filePath(f))}</span><span>${bytes(f.size)}</span></div>`).join('')+(selectedFiles.length>10?`<div class="file-row"><span>+ ${selectedFiles.length-10} more</span><span></span></div>`:'');$('#uploadButton').disabled=!(selectedFiles.length&&$('#packageName').value.trim()&&$('#channelName').value.trim()&&$('#uploadCategory').value)}
function showLocal(p,m){const b=$('#localJob');b.classList.add('visible');b.querySelector('.job-head span').textContent=p+'%';b.querySelector('.bar').style.width=p+'%';b.querySelector('.message').textContent=m}function showJob(id,j){const b=$('#'+id);b.className='job visible'+(j.status==='error'?' error':'');b.dataset.job=j.id;b.querySelector('strong').textContent=j.name;b.querySelector('.job-head span').textContent=j.progress+'%';b.querySelector('.bar').style.width=j.progress+'%';b.querySelector('.message').textContent=[j.message,j.speed_label,j.eta_seconds?`ETA ${duration(j.eta_seconds)}`:''].filter(Boolean).join(' ? ');const c=b.querySelector('.cancel');if(c)c.style.display=j.cancelable?'inline':'none'}async function poll(j,id,done){showJob(id,j);while(['queued','working'].includes(j.status)){await sleep(1100);j=(await api('/api/jobs/'+j.id)).job;showJob(id,j)}loadActivity();if(j.status==='error')throw new Error(j.error);if(j.status==='canceled')throw new Error('Transfer canceled');done?.(j)}async function cancelBox(b){if(!b.dataset.job)return;try{await api(`/api/jobs/${b.dataset.job}/cancel`,{method:'POST'});toast('Cancellation requested')}catch(e){toast(e.message,true)}}
function renderLibrary(){const q=$('#search').value.trim().toLowerCase(),cat=$('#filterCategory').value,ch=$('#filterChannel').value,sort=$('#sort').value;let a=library.filter(x=>(!cat||x.category_id===cat)&&(!ch||x.channel_id===ch)&&(!q||[x.name,x.note,...x.tags].join(' ').toLowerCase().includes(q)));a.sort((x,y)=>sort==='old'?String(x.created_at).localeCompare(String(y.created_at)):sort==='name'?x.name.localeCompare(y.name):sort==='size'?y.size-x.size:sort==='files'?y.file_count-x.file_count:String(y.created_at).localeCompare(String(x.created_at)));if(!a.length){$('#archiveList').innerHTML='<div class="empty"><strong>No packages match</strong>Clear filters or upload something.</div>';return}$('#archiveList').innerHTML=a.map(x=>`<article class="archive" data-channel="${x.channel_id}" data-manifest="${x.manifest_message_id}"><div class="archive-top"><div><h3>${esc(x.name)}</h3><div class="meta">${esc(x.category_name)} / #${esc(x.channel_name)} ? ${x.file_count} files ? ${esc(x.size_label)} ? ${x.parts} parts</div>${x.note?`<p class="note">${esc(x.note)}</p>`:''}<div class="tags"><span class="tag">schema v${x.schema}</span>${x.suspicious_count?`<span class="tag warn">${x.suspicious_count} quarantined executable/script</span>`:''}${x.tags.map(t=>`<span class="tag">${esc(t)}</span>`).join('')}</div></div><div class="actions"><button class="download">Download</button>${x.inventory?.length?'<button class="contents">Contents</button>':''}<button class="verify">Verify</button><a href="${x.discord_url}" target="_blank">Discord</a><button class="copy">Copy</button><button class="rename">Rename</button><button class="delete">Delete</button></div></div></article>`).join('')}
async function loadActivity(){try{const [j,h]=await Promise.all([api('/api/jobs'),api('/api/history')]);$('#activityList').innerHTML=j.jobs.length?j.jobs.map(x=>`<div class="activity"><span class="badge">${esc(x.kind)}</span><div><strong>${esc(x.name)}</strong><small>${esc(x.message)}${x.speed_label?' ? '+esc(x.speed_label):''}</small></div><span>${x.progress}% ? ${esc(x.status)}</span></div>`).join(''):'<div class="empty">No current activity.</div>';const up=h.history.filter(x=>x.kind==='upload').slice(0,14).reverse(),max=Math.max(...up.map(x=>x.bytes_per_second||0),1);$('#speedChart').innerHTML=up.length?up.map(x=>`<div class="chart-col" style="height:${Math.max(7,(x.bytes_per_second||0)/max*100)}%" data-label="${bytes(x.bytes_per_second||0)}/s"></div>`).join(''):'<div class="empty" style="width:100%">Complete an upload to train estimates.</div>';$('#historySpeed').textContent=h.summary.median_upload_speed_label;$('#historyList').innerHTML=h.history.length?h.history.slice(0,20).map(x=>`<div class="activity"><span class="badge">${esc(x.kind)}</span><div><strong>${esc(x.name||'Transfer')}</strong><small>${new Date(x.at).toLocaleString()} ? ${bytes(x.bytes||0)}</small></div><span>${x.bytes_per_second?bytes(x.bytes_per_second)+'/s':''}</span></div>`).join(''):'<div class="empty">History is empty.</div>'}catch(e){toast(e.message,true)}}
function renderSystem(){if(!tree)return;const s=tree.system,h=tree.history,rows=[['Version','v'+s.version],['Uptime',duration(s.uptime_seconds)],['Chunk',s.chunk_label],['Discord limit',s.discord_chunk_limit_label],['Maximum selection',s.max_upload_label],['Compression','Level '+s.compression_level],['Parallel recovery',s.download_concurrency+' parts'],['Maximum files',String(s.max_files)],['Learned speed',h.median_upload_speed_label],['Completed uploads',String(h.completed_uploads)],['Historical transfer',h.total_uploaded_label],['Ingestion','Raw bytes only']];$('#systemGrid').innerHTML=rows.map(r=>`<div class="system-item"><small>${esc(r[0])}</small><strong>${esc(r[1])}</strong></div>`).join('')}
function openPassword(t,m,a){pendingPasswordAction=a;$('#modalTitle').textContent=t;$('#modalText').textContent=m;$('#confirmPassword').value='';$('#passwordModal').classList.add('open');setTimeout(()=>$('#confirmPassword').focus(),30)}function closePassword(){$('#passwordModal').classList.remove('open');pendingPasswordAction=null}function showContents(a){$('#contentsTitle').textContent=a.name;$('#contentsMeta').textContent=`${a.file_count} files ? ${a.source_bytes?bytes(a.source_bytes):a.size_label}${a.inventory_truncated?' ? preview truncated':''}`;$('#contentsList').innerHTML=a.inventory.map(i=>`<div class="content-row"><span class="${i.flagged?'flag':''}">${esc(i.path)}${i.flagged?' ? quarantined':''}</span><span>${bytes(i.size)}</span></div>`).join('');$('#contentsModal').classList.add('open')}
$$('.tab').forEach(t=>t.onclick=()=>{$$('.tab').forEach(x=>x.classList.toggle('active',x===t));$$('.panel').forEach(x=>x.classList.toggle('active',x.id===t.dataset.tab));if(t.dataset.tab==='library')loadLibrary();if(t.dataset.tab==='activity')loadActivity();if(t.dataset.tab==='settings'){loadSettings();loadTree()}});$('#chooseFiles').onclick=()=>$('#filePicker').click();$('#chooseFolder').onclick=()=>$('#folderPicker').click();$('#filePicker').onchange=e=>setFiles(e.target.files);$('#folderPicker').onchange=e=>setFiles(e.target.files);const drop=$('#drop');drop.onclick=()=>$('#filePicker').click();drop.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();$('#filePicker').click()}};drop.ondragover=e=>{e.preventDefault();drop.classList.add('drag')};drop.ondragleave=()=>drop.classList.remove('drag');drop.ondrop=e=>{e.preventDefault();drop.classList.remove('drag');setFiles(e.dataTransfer.files)};$('#clearSelection').onclick=()=>{selectedFiles=[];$('#filePicker').value='';$('#folderPicker').value='';updateSelection()};['packageName','channelName'].forEach(id=>$('#'+id).oninput=updateSelection);$('#uploadCategory').onchange=updateSelection;$('#uploadJob .cancel').onclick=()=>cancelBox($('#uploadJob'));$('#downloadJob .cancel').onclick=()=>cancelBox($('#downloadJob'));
$('#uploadForm').onsubmit=async e=>{e.preventDefault();const d=new FormData();d.append('category_id',$('#uploadCategory').value);d.append('channel_name',$('#channelName').value.trim());d.append('package_name',$('#packageName').value.trim());d.append('tags',$('#tags').value);d.append('note',$('#note').value);for(const f of selectedFiles){d.append('path',filePath(f));d.append('files',f,f.name)}try{$('#uploadButton').disabled=true;const r=await uploadRequest(d,showLocal);showLocal(100,`Safely packaged ${r.package.file_count} files into ${r.package.parts} parts`);await poll(r.job,'uploadJob',()=>{toast('Package stored and verified.');loadTree();loadLibrary(true)});selectedFiles=[];$('#filePicker').value='';$('#folderPicker').value='';$('#packageName').value='';$('#channelName').value='';$('#tags').value='';$('#note').value=''}catch(e){toast(e.message,true)}finally{updateSelection()}};
$('#filterCategory').onchange=()=>{updateChannels();renderLibrary()};$('#filterChannel').onchange=renderLibrary;$('#search').oninput=renderLibrary;$('#sort').onchange=renderLibrary;$('#clearFilters').onclick=()=>{$('#search').value='';$('#filterCategory').value='';updateChannels();$('#sort').value='new';renderLibrary()};$('#refreshLibrary').onclick=()=>loadLibrary(true);$('#refreshActivity').onclick=loadActivity;
$('#archiveList').onclick=async e=>{const b=e.target.closest('button');if(!b)return;const card=b.closest('.archive'),channel_id=card.dataset.channel,manifest_message_id=card.dataset.manifest,a=library.find(x=>x.channel_id===channel_id&&x.manifest_message_id===manifest_message_id);try{if(b.classList.contains('download')){const {job}=await api('/api/prepare-download',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channel_id,manifest_message_id})});await poll(job,'downloadJob',r=>{toast('Verified. Download starting...');location=`/api/jobs/${r.id}/download`})}else if(b.classList.contains('contents'))showContents(a);else if(b.classList.contains('verify')){const {job}=await api('/api/verify',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channel_id,manifest_message_id})});await poll(job,'downloadJob',x=>toast(x.message))}else if(b.classList.contains('copy')){await navigator.clipboard.writeText(a.discord_url);toast('Discord link copied')}else if(b.classList.contains('rename')){const name=prompt('New channel name',a.channel_name);if(name){await api('/api/channels/rename',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channel_id,name})});await loadTree();await loadLibrary(true)}}else if(b.classList.contains('delete'))openPassword('Delete cloud package',`Permanently delete #${a.channel_name}?`,async password=>{await api('/api/channels/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channel_id,password})});await loadTree();await loadLibrary(true)})}catch(err){toast(err.message,true)}};
$('#settingsForm').onsubmit=async e=>{e.preventDefault();try{const body={chunk_mb:$('#setChunk').value,max_upload_gb:$('#setMaxUpload').value,compression_level:$('#setCompression').value,download_concurrency:$('#setConcurrency').value,retry_attempts:$('#setRetries').value,ready_ttl_minutes:$('#setTtl').value,max_files:$('#setMaxFiles').value};settings=(await api('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).settings;await loadTree();toast('Settings saved and active')}catch(err){toast(err.message,true)}};$$('.preset').forEach(b=>b.onclick=()=>{const p=b.dataset.preset,max=Math.min(9,settings?.discord_chunk_limit_mb||9);$('#setChunk').value=p==='compact'?Math.min(5,max):max;$('#setCompression').value=p==='fast'?0:p==='balanced'?1:9;$('#setConcurrency').value=p==='compact'?2:3;toast(`${b.textContent} loaded ? press Save settings`)});$('#clearHistory').onclick=()=>openPassword('Clear history','Delete learned speeds and analytics?',async password=>{await api('/api/history/clear',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password})});await loadActivity();await loadTree()});$('#shutdown').onclick=()=>openPassword('Turn off Discord Cloud','Stop the website and bot?',async password=>{await api('/api/shutdown',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password})});document.body.innerHTML='<main class="offline"><h1>Discord Cloud is off.</h1><p>Restart with sudo systemctl start discord-cloud</p></main>'});$('#modalCancel').onclick=closePassword;$('#passwordModal').onclick=e=>{if(e.target===$('#passwordModal'))closePassword()};$('#passwordForm').onsubmit=async e=>{e.preventDefault();try{await pendingPasswordAction?.($('#confirmPassword').value);closePassword()}catch(err){toast(err.message,true);$('#confirmPassword').select()}};$('#contentsClose').onclick=()=>$('#contentsModal').classList.remove('open');$('#contentsModal').onclick=e=>{if(e.target===$('#contentsModal'))$('#contentsModal').classList.remove('open')};document.onkeydown=e=>{if(e.key==='Escape'){closePassword();$('#contentsModal').classList.remove('open')}};
(async()=>{try{await loadTree();await loadSettings();await loadLibrary();loadActivity();setInterval(()=>{loadTree();loadActivity()},30000)}catch(e){toast(e.message,true)}})();
</script></body></html>'''


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        self_test()
    else:
        asyncio.run(main())
