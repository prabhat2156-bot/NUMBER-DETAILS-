# bot.py — God Madara Hosting Bot (FINAL MERGED)
# Production-ready, async Python Telegram bot
# Python 3.11+ | python-telegram-bot v20+ | Includes full persistence layer
#
# This is ONE complete file — no external persistence.py needed.
# Deploy directly to Render or any Python host.

# ═══ IMPORTS ════════════════════════════════════════════════════════════════

import asyncio
import hashlib
import io
import logging
import mimetypes
import os
import platform
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import psutil
from bson import ObjectId
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# ═══ ENVIRONMENT & CONFIG ════════════════════════════════════════════════════

load_dotenv()

BOT_TOKEN    = os.environ["BOT_TOKEN"]
OWNER_ID     = int(os.environ["OWNER_ID"])
MONGODB_URI  = os.environ["MONGODB_URI"]
FM_PORT      = int(os.environ.get("FILE_MANAGER_PORT", 8080))
FM_BASE_URL  = os.environ.get("FILE_MANAGER_URL", f"http://localhost:{FM_PORT}")
PROJECTS_DIR = Path(os.environ.get("PROJECTS_DIR", "projects"))
LOGS_DIR     = Path(os.environ.get("LOGS_DIR", "logs"))

PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ═══ LOGGING ════════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ═══ MONGODB ════════════════════════════════════════════════════════════════

mongo_client = AsyncIOMotorClient(MONGODB_URI)
db           = mongo_client["madara_hosting"]
users_col    = db["users"]
projects_col = db["projects"]
backup_meta_col = db["backup_meta"]

# GridFS bucket for project file backups
fs_bucket: AsyncIOMotorGridFSBucket = AsyncIOMotorGridFSBucket(db, bucket_name="project_files")

# ═══ IN-MEMORY PROCESS TRACKER ══════════════════════════════════════════════
# { "user_id:project_name" → subprocess.Popen }
running_procs: dict[str, subprocess.Popen] = {}

# ═══ CONVERSATION STATES ════════════════════════════════════════════════════

(
    NP_NAME, NP_FILES, NP_CMD,
    EDIT_CMD,
    ADMIN_GIVE_PREM, ADMIN_REM_PREM,
    ADMIN_TEMP_PREM_ID, ADMIN_TEMP_PREM_DAYS,
    ADMIN_BAN, ADMIN_UNBAN,
    ADMIN_BROADCAST_MSG, ADMIN_SEND_USER_ID, ADMIN_SEND_USER_MSG,
) = range(13)

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: GENERAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def is_valid_project_name(name: str) -> bool:
    return bool(re.match(r'^[A-Za-z0-9_\-]{1,32}$', name))


async def get_or_create_user(user) -> dict:
    doc = await users_col.find_one({"user_id": user.id})
    if not doc:
        doc = {
            "user_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name or "",
            "plan": "free",
            "premium_expiry": None,
            "banned": False,
            "joined_at": datetime.utcnow(),
        }
        await users_col.insert_one(doc)
    else:
        await users_col.update_one(
            {"user_id": user.id},
            {"$set": {"username": user.username or "", "first_name": user.first_name or ""}}
        )
    return doc


async def is_user_banned(user_id: int) -> bool:
    doc = await users_col.find_one({"user_id": user_id})
    return bool(doc and doc.get("banned"))


async def check_premium_expiry(user_id: int):
    doc = await users_col.find_one({"user_id": user_id})
    if doc and doc.get("plan") == "premium" and doc.get("premium_expiry"):
        if datetime.utcnow() > doc["premium_expiry"]:
            await users_col.update_one(
                {"user_id": user_id},
                {"$set": {"plan": "free", "premium_expiry": None}}
            )


def get_plan_label(doc: dict) -> str:
    return "Premium ✨" if doc.get("plan") == "premium" else "Free"


def get_project_path(user_id: int, project_name: str) -> Path:
    return PROJECTS_DIR / str(user_id) / project_name


def get_log_path(project_id: str) -> Path:
    return LOGS_DIR / f"{project_id}.log"


def uptime_str(started_at: datetime) -> str:
    if not started_at:
        return "N/A"
    delta = datetime.utcnow() - started_at
    h, rem = divmod(int(delta.total_seconds()), 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m {s}s"


def bytes_to_gb(b: int) -> str:
    return f"{b / (1024**3):.1f} GB"


def bytes_human(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num_bytes < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def get_system_stats() -> dict:
    cpu  = psutil.cpu_percent(interval=0.3)
    ram  = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    return {
        "cpu": cpu,
        "ram_used": bytes_to_gb(ram.used),
        "ram_total": bytes_to_gb(ram.total),
        "ram_pct": ram.percent,
        "disk_used": bytes_to_gb(disk.used),
        "disk_total": bytes_to_gb(disk.total),
        "disk_pct": disk.percent,
    }


async def project_count(user_id: int) -> int:
    return await projects_col.count_documents({"user_id": user_id})


def generate_fm_token(project_id: str, user_id: int) -> str:
    token = uuid.uuid4().hex
    token_path = Path("fm_tokens") / f"{token}.txt"
    token_path.parent.mkdir(exist_ok=True)
    expiry = datetime.utcnow() + timedelta(hours=1)
    with open(token_path, "w") as f:
        f.write(f"{project_id}\n{user_id}\n{expiry.isoformat()}")
    return token


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: PROCESS MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def _build_run_command(run_command: str, project_path: Path) -> list[str]:
    """Convert a stored run_command string into a shell-safe list for Popen."""
    return shlex.split(run_command)


def start_project_process(project: dict) -> Optional[subprocess.Popen]:
    proj_path = Path(project["path"])
    cmd = project.get("run_command", "python3 main.py")
    proj_id_str = str(project["_id"])
    log_path = get_log_path(proj_id_str)
    registry_key = f"{project['user_id']}:{project['name']}"
    try:
        log_file = open(log_path, "a")
        proc = subprocess.Popen(
            cmd, shell=True, cwd=str(proj_path),
            stdout=log_file, stderr=log_file,
            preexec_fn=os.setsid if sys.platform != "win32" else None,
        )
        running_procs[proj_id_str] = proc
        running_procs[registry_key] = proc  # dual-key for watcher compatibility
        return proc
    except Exception as e:
        logger.error(f"Failed to start project {project['name']}: {e}")
        return None


async def stop_project_process(project_id: str, pid: Optional[int]) -> bool:
    proc = running_procs.get(project_id)
    if proc:
        try:
            if sys.platform != "win32":
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            else:
                proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        running_procs.pop(project_id, None)
        return True
    if pid and psutil.pid_exists(pid):
        try:
            p = psutil.Process(pid)
            p.terminate()
            p.wait(timeout=5)
        except Exception:
            pass
        return True
    return False


def read_logs(project_id: str, lines: int = 50) -> str:
    log_path = get_log_path(project_id)
    if not log_path.exists():
        return "No logs yet."
    try:
        with open(log_path, "r", errors="replace") as f:
            all_lines = f.readlines()
        return "".join(all_lines[-lines:]) or "No output yet."
    except Exception:
        return "Could not read logs."


def _is_process_alive(pid: Optional[int], process: Optional[subprocess.Popen]) -> bool:
    """Return True if the process is still running."""
    if process is not None:
        return process.poll() is None
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: GRIDFS PERSISTENCE — BACKUP FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_md5(file_path: Path) -> str:
    """Compute MD5 hex-digest of a file, reading in 8 MB chunks."""
    h = hashlib.md5()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _relative_path(file_path: Path, project_path: Path) -> str:
    """Return the POSIX relative path of file_path inside project_path."""
    return file_path.relative_to(project_path).as_posix()


async def backup_single_file(
    user_id: int,
    project_name: str,
    file_path: Path,
    project_path: Path,
) -> bool:
    """
    Backup a single file to GridFS.

    Steps:
      1. Compute MD5 of the local file.
      2. Check whether an identical GridFS document already exists. Skip if unchanged.
      3. Delete any previous GridFS document for this (user_id, project_name, relative_path).
      4. Upload the new version with rich metadata.

    Returns True on success, False on failure (never raises).
    """
    if not file_path.is_file():
        logger.warning("backup_single_file: %s is not a file — skipped.", file_path)
        return False

    try:
        relative = _relative_path(file_path, project_path)
        md5_hash = _compute_md5(file_path)
        file_size = file_path.stat().st_size

        # Check whether file is unchanged
        existing = await db["fs.files"].find_one({
            "metadata.user_id": user_id,
            "metadata.project_name": project_name,
            "metadata.relative_path": relative,
            "metadata.md5": md5_hash,
        })
        if existing:
            logger.debug("backup_single_file: %s/%s — unchanged, skipped.", project_name, relative)
            return True

        # Delete old version(s)
        async for old_doc in db["fs.files"].find({
            "metadata.user_id": user_id,
            "metadata.project_name": project_name,
            "metadata.relative_path": relative,
        }):
            await fs_bucket.delete(old_doc["_id"])
            logger.debug(
                "backup_single_file: deleted old GridFS doc %s for %s/%s",
                old_doc["_id"], project_name, relative,
            )

        # Upload new version
        metadata = {
            "user_id": user_id,
            "project_name": project_name,
            "relative_path": relative,
            "md5": md5_hash,
            "file_size": file_size,
            "uploaded_at": datetime.now(timezone.utc),
        }
        with open(file_path, "rb") as fh:
            await fs_bucket.upload_from_stream(
                filename=f"{user_id}/{project_name}/{relative}",
                source=fh,
                metadata=metadata,
                chunk_size_bytes=255 * 1024,
            )

        logger.info("backup_single_file: ✅ %s/%s (%s bytes)", project_name, relative, file_size)
        return True

    except Exception as exc:
        logger.error(
            "backup_single_file: ❌ Failed to backup %s/%s — %s",
            project_name, file_path, exc, exc_info=True,
        )
        return False


async def backup_project_files(
    user_id: int,
    project_name: str,
    project_path: Path,
) -> dict:
    """
    Backup ALL files from a project directory tree to GridFS.
    Skips __pycache__, .git, node_modules, venv, .env directories.
    Returns summary dict: {backed_up, skipped, failed, total}.
    """
    if not project_path.is_dir():
        logger.warning("backup_project_files: %s does not exist — nothing to backup.", project_path)
        return {"backed_up": 0, "skipped": 0, "failed": 0, "total": 0}

    summary = {"backed_up": 0, "skipped": 0, "failed": 0, "total": 0}
    SKIP_DIRS = {"__pycache__", ".git", "node_modules", ".venv", "venv", ".env"}

    for file_path in sorted(project_path.rglob("*")):
        if not file_path.is_file():
            continue
        if any(part in SKIP_DIRS for part in file_path.parts):
            continue

        summary["total"] += 1
        relative = _relative_path(file_path, project_path)

        try:
            md5_hash = _compute_md5(file_path)
        except Exception:
            summary["failed"] += 1
            continue

        existing = await db["fs.files"].find_one({
            "metadata.user_id": user_id,
            "metadata.project_name": project_name,
            "metadata.relative_path": relative,
            "metadata.md5": md5_hash,
        })
        if existing:
            summary["skipped"] += 1
            continue

        success = await backup_single_file(user_id, project_name, file_path, project_path)
        if success:
            summary["backed_up"] += 1
        else:
            summary["failed"] += 1

    logger.info(
        "backup_project_files: %s — backed_up=%d skipped=%d failed=%d total=%d",
        project_name, summary["backed_up"], summary["skipped"],
        summary["failed"], summary["total"],
    )
    return summary


async def auto_backup_loop(interval_minutes: int = 10) -> None:
    """
    Background asyncio task. Wakes up every interval_minutes and backs up
    ALL projects from the MongoDB projects collection.
    Runs for the lifetime of the bot. Never raises.
    """
    logger.info("auto_backup_loop: started — interval=%d min", interval_minutes)
    interval_seconds = interval_minutes * 60

    while True:
        try:
            await asyncio.sleep(interval_seconds)
            logger.info("auto_backup_loop: ⏰ running scheduled backup…")

            total_projects = 0
            total_files = 0

            async for project_doc in projects_col.find({}):
                try:
                    uid = int(project_doc.get("user_id", 0))
                    pname = project_doc.get("name", "")
                    if not pname:
                        continue
                    ppath = PROJECTS_DIR / str(uid) / pname
                    if not ppath.is_dir():
                        continue
                    summary = await backup_project_files(uid, pname, ppath)
                    total_projects += 1
                    total_files += summary["backed_up"]
                except Exception as proj_exc:
                    logger.error("auto_backup_loop: error backing up project — %s", proj_exc, exc_info=True)

            await backup_meta_col.update_one(
                {"_id": "global"},
                {"$set": {
                    "last_backup_time": datetime.now(timezone.utc),
                    "last_projects_count": total_projects,
                    "last_files_count": total_files,
                }},
                upsert=True,
            )
            logger.info(
                "auto_backup_loop: ✅ done — %d projects, %d new files uploaded",
                total_projects, total_files,
            )

        except asyncio.CancelledError:
            logger.info("auto_backup_loop: task cancelled — exiting.")
            return
        except Exception as exc:
            logger.error("auto_backup_loop: unexpected error — %s", exc, exc_info=True)
            await asyncio.sleep(60)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: GRIDFS PERSISTENCE — RESTORE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

async def restore_project(
    user_id: int,
    project_name: str,
    project_path: Path,
) -> int:
    """
    Restore a single project's files from GridFS to disk.
    Creates any missing parent directories automatically.
    Returns the number of files restored.
    """
    restored = 0
    query = {
        "metadata.user_id": user_id,
        "metadata.project_name": project_name,
    }

    async for file_doc in db["fs.files"].find(query):
        relative_path_str: str = file_doc["metadata"].get("relative_path", "")
        if not relative_path_str:
            continue

        dest = project_path / relative_path_str
        dest.parent.mkdir(parents=True, exist_ok=True)

        try:
            grid_out = await fs_bucket.open_download_stream(file_doc["_id"])
            with open(dest, "wb") as fh:
                while True:
                    chunk = await grid_out.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    fh.write(chunk)
            restored += 1
            logger.debug("restore_project: ✅ %s/%s → %s", project_name, relative_path_str, dest)
        except Exception as exc:
            logger.error(
                "restore_project: ❌ failed to restore %s/%s — %s",
                project_name, relative_path_str, exc, exc_info=True,
            )

    logger.info("restore_project: %s — restored %d files to %s", project_name, restored, project_path)
    return restored


async def restore_all_projects() -> list[str]:
    """
    Called on bot startup. Reads ALL projects from MongoDB, recreates directory
    structure on disk, and downloads every file from GridFS.
    Returns a list of project names that were successfully restored.
    """
    logger.info("restore_all_projects: 🔄 starting full restore from GridFS…")
    restored_names: list[str] = []

    async for project_doc in projects_col.find({}):
        try:
            uid = int(project_doc.get("user_id", 0))
            pname = project_doc.get("name", "")
            if not pname:
                continue

            ppath = PROJECTS_DIR / str(uid) / pname
            ppath.mkdir(parents=True, exist_ok=True)

            count = await restore_project(uid, pname, ppath)
            restored_names.append(pname)
            logger.info("restore_all_projects: ✅ %s — %d files restored", pname, count)

        except Exception as exc:
            logger.error("restore_all_projects: ❌ error restoring project — %s", exc, exc_info=True)

    logger.info("restore_all_projects: done — %d projects restored.", len(restored_names))
    return restored_names


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: GRIDFS PERSISTENCE — AUTO-RESTART RUNNING PROJECTS
# ═══════════════════════════════════════════════════════════════════════════════

async def auto_restart_running_projects() -> list[str]:
    """
    Called after restore_all_projects() on bot startup.
    Finds every project whose status == 'running' and re-launches it.
    Updates the PID in the database.
    Returns a list of project names that were successfully restarted.
    """
    logger.info("auto_restart_running_projects: 🚀 checking for projects to restart…")
    restarted: list[str] = []

    async for project_doc in projects_col.find({"status": "running"}):
        try:
            uid = int(project_doc.get("user_id", 0))
            pname = project_doc.get("name", "")
            run_command: str = project_doc.get("run_command", "")

            if not pname or not run_command:
                logger.warning("auto_restart_running_projects: %s has no run_command — skipped.", pname)
                continue

            ppath = PROJECTS_DIR / str(uid) / pname
            if not ppath.is_dir():
                logger.warning("auto_restart_running_projects: %s directory missing — skipped.", pname)
                await projects_col.update_one(
                    {"_id": project_doc["_id"]},
                    {"$set": {"status": "stopped", "pid": None}},
                )
                continue

            log_path = get_log_path(str(project_doc["_id"]))
            log_file = open(log_path, "a")
            process = subprocess.Popen(
                run_command,
                shell=True,
                cwd=str(ppath),
                stdout=log_file,
                stderr=log_file,
                preexec_fn=os.setsid if sys.platform != "win32" else None,
            )

            proj_id_str = str(project_doc["_id"])
            registry_key = f"{uid}:{pname}"
            running_procs[proj_id_str] = process
            running_procs[registry_key] = process

            await projects_col.update_one(
                {"_id": project_doc["_id"]},
                {"$set": {
                    "pid": process.pid,
                    "status": "running",
                    "last_run": datetime.utcnow(),
                    "started_at": datetime.utcnow(),
                }},
            )

            restarted.append(pname)
            logger.info("auto_restart_running_projects: ✅ %s restarted — PID %d", pname, process.pid)

        except Exception as exc:
            logger.error(
                "auto_restart_running_projects: ❌ failed to restart %s — %s",
                project_doc.get("name", "?"), exc, exc_info=True,
            )

    logger.info("auto_restart_running_projects: done — %d projects restarted.", len(restarted))
    return restarted


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: GRIDFS PERSISTENCE — CLEANUP & STATS
# ═══════════════════════════════════════════════════════════════════════════════

async def delete_project_backup(user_id: int, project_name: str) -> int:
    """
    Delete ALL GridFS files associated with a project.
    Called when a project is permanently deleted.
    Returns the number of GridFS documents removed.
    """
    deleted = 0
    query = {
        "metadata.user_id": user_id,
        "metadata.project_name": project_name,
    }
    async for file_doc in db["fs.files"].find(query):
        try:
            await fs_bucket.delete(file_doc["_id"])
            deleted += 1
        except Exception as exc:
            logger.error(
                "delete_project_backup: failed to delete GridFS doc %s — %s",
                file_doc["_id"], exc,
            )

    logger.info("delete_project_backup: 🗑️ %s — deleted %d GridFS files", project_name, deleted)
    return deleted


async def get_backup_stats() -> dict:
    """
    Returns a summary of the current GridFS backup state.
    Keys: total_files, total_size_bytes, total_size_human, last_backup_time, projects_count
    """
    pipeline = [
        {"$group": {"_id": None, "count": {"$sum": 1}, "size": {"$sum": "$length"}}}
    ]
    agg_result = await db["fs.files"].aggregate(pipeline).to_list(length=1)

    if agg_result:
        total_files: int = agg_result[0]["count"]
        total_size_bytes: int = agg_result[0]["size"]
    else:
        total_files = 0
        total_size_bytes = 0

    distinct_projects = await db["fs.files"].distinct("metadata.project_name")
    projects_count = len(distinct_projects)

    meta = await backup_meta_col.find_one({"_id": "global"})
    last_backup_time: Optional[datetime] = meta.get("last_backup_time") if meta else None

    return {
        "total_files": total_files,
        "total_size_bytes": total_size_bytes,
        "total_size_human": bytes_human(total_size_bytes),
        "last_backup_time": last_backup_time,
        "projects_count": projects_count,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: GRIDFS PERSISTENCE — PROCESS WATCHER
# ═══════════════════════════════════════════════════════════════════════════════

async def process_watcher(interval_seconds: int = 60) -> None:
    """
    Background asyncio task. Runs every interval_seconds.
    For every project with status='running':
      1. Checks whether the stored PID is still alive.
      2. If dead: updates status to 'stopped', records exit_code.
      3. If auto_restart is enabled: restarts the project.
    """
    logger.info("process_watcher: started — interval=%ds", interval_seconds)

    while True:
        try:
            await asyncio.sleep(interval_seconds)

            async for project_doc in projects_col.find({"status": "running"}):
                uid = int(project_doc.get("user_id", 0))
                pname = project_doc.get("name", "")
                pid: Optional[int] = project_doc.get("pid")
                auto_restart_flag: bool = project_doc.get("auto_restart", False)
                run_command: str = project_doc.get("run_command", "")

                if not pname:
                    continue

                proj_id_str = str(project_doc["_id"])
                registry_key = f"{uid}:{pname}"
                process: Optional[subprocess.Popen] = (
                    running_procs.get(proj_id_str) or running_procs.get(registry_key)
                )

                alive = _is_process_alive(pid, process)
                if alive:
                    continue

                # Process is dead
                exit_code: Optional[int] = None
                if process is not None:
                    exit_code = process.returncode
                    running_procs.pop(proj_id_str, None)
                    running_procs.pop(registry_key, None)

                logger.warning(
                    "process_watcher: 💀 %s (PID %s) is dead — exit_code=%s",
                    pname, pid, exit_code,
                )

                if auto_restart_flag and run_command:
                    ppath = PROJECTS_DIR / str(uid) / pname
                    if ppath.is_dir():
                        try:
                            log_path = get_log_path(proj_id_str)
                            log_file = open(log_path, "a")
                            new_proc = subprocess.Popen(
                                run_command,
                                shell=True,
                                cwd=str(ppath),
                                stdout=log_file,
                                stderr=log_file,
                                preexec_fn=os.setsid if sys.platform != "win32" else None,
                            )
                            running_procs[proj_id_str] = new_proc
                            running_procs[registry_key] = new_proc

                            await projects_col.update_one(
                                {"_id": project_doc["_id"]},
                                {"$set": {
                                    "pid": new_proc.pid,
                                    "status": "running",
                                    "exit_code": exit_code,
                                    "last_run": datetime.utcnow(),
                                    "started_at": datetime.utcnow(),
                                }},
                            )
                            logger.info(
                                "process_watcher: 🔄 %s auto-restarted — new PID %d",
                                pname, new_proc.pid,
                            )
                            continue
                        except Exception as restart_exc:
                            logger.error("process_watcher: failed to restart %s — %s", pname, restart_exc)

                # Mark as stopped
                await projects_col.update_one(
                    {"_id": project_doc["_id"]},
                    {"$set": {
                        "status": "stopped",
                        "pid": None,
                        "exit_code": exit_code,
                    }},
                )

        except asyncio.CancelledError:
            logger.info("process_watcher: task cancelled — exiting.")
            return
        except Exception as exc:
            logger.error("process_watcher: unexpected error — %s", exc, exc_info=True)
            await asyncio.sleep(30)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════

def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆕 New Project", callback_data="new_project"),
         InlineKeyboardButton("📁 My Projects", callback_data="my_projects")],
        [InlineKeyboardButton("💎 Premium", callback_data="premium"),
         InlineKeyboardButton("📊 Bot Status", callback_data="bot_status")],
    ])


def back_to_main():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="main_menu")]])


def project_dashboard_keyboard(project: dict) -> InlineKeyboardMarkup:
    pid = project.get("pid")
    is_running = bool(pid and psutil.pid_exists(pid))
    proj_id = str(project["_id"])
    run_btn = (
        InlineKeyboardButton("⏹️ Stop", callback_data=f"stop_proj:{proj_id}")
        if is_running else
        InlineKeyboardButton("▶️ Run", callback_data=f"run_proj:{proj_id}")
    )
    return InlineKeyboardMarkup([
        [run_btn,
         InlineKeyboardButton("🔄 Restart", callback_data=f"restart_proj:{proj_id}"),
         InlineKeyboardButton("📋 Logs", callback_data=f"logs_proj:{proj_id}")],
        [InlineKeyboardButton("🔃 Refresh", callback_data=f"refresh_proj:{proj_id}"),
         InlineKeyboardButton("✏️ Edit Run CMD", callback_data=f"editcmd_proj:{proj_id}"),
         InlineKeyboardButton("📂 File Manager", callback_data=f"fm_proj:{proj_id}")],
        [InlineKeyboardButton("🗑️ Delete Project", callback_data=f"delete_proj:{proj_id}"),
         InlineKeyboardButton("⬅️ Back", callback_data="my_projects")],
    ])


def admin_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 All Users", callback_data="admin_users:0"),
         InlineKeyboardButton("🟢 Running Scripts", callback_data="admin_running")],
        [InlineKeyboardButton("💎 Give Premium", callback_data="admin_give_prem"),
         InlineKeyboardButton("❌ Remove Premium", callback_data="admin_rem_prem")],
        [InlineKeyboardButton("⏰ Temp Premium", callback_data="admin_temp_prem"),
         InlineKeyboardButton("🚫 Ban User", callback_data="admin_ban")],
        [InlineKeyboardButton("✅ Unban User", callback_data="admin_unban"),
         InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast_menu")],
        [InlineKeyboardButton("💾 Force Backup", callback_data="admin_force_backup"),
         InlineKeyboardButton("📊 Backup Stats", callback_data="bot_status")],
        [InlineKeyboardButton("⬅️ Back", callback_data="main_menu")],
    ])


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: /start COMMAND
# ═══════════════════════════════════════════════════════════════════════════════

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if await is_user_banned(user.id):
        await update.message.reply_text("🚫 You are banned from using this bot. Contact the owner.")
        return
    await check_premium_expiry(user.id)
    doc = await get_or_create_user(user)
    plan_label = get_plan_label(doc)
    max_proj = 10 if doc["plan"] == "premium" else 1
    count = await project_count(user.id)
    display = user.first_name or user.username or str(user.id)
    text = (
        f"🌟 *Welcome to God Madara Hosting Bot!*\n\n"
        f"👋 Hello {display}!\n\n"
        f"🚀 *What I can do:*\n"
        f"• Host Python projects 24/7\n"
        f"• Web File Manager — Edit files in browser\n"
        f"• Auto-install requirements.txt\n"
        f"• Real-time logs & monitoring\n"
        f"• Auto-backup to MongoDB GridFS\n"
        f"• Auto-restore after restarts\n"
        f"• Free: 1 project | Premium: Unlimited (up to 10)\n\n"
        f"📊 *Your Status:*\n"
        f"👤 ID: `{user.id}`\n"
        f"💎 Plan: {plan_label}\n"
        f"📁 Projects: {count}/{max_proj}\n\n"
        f"Choose an option below:"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_keyboard())


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: MAIN MENU CALLBACK
# ═══════════════════════════════════════════════════════════════════════════════

async def main_menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    await check_premium_expiry(user.id)
    doc = await get_or_create_user(user)
    plan_label = get_plan_label(doc)
    max_proj = 10 if doc["plan"] == "premium" else 1
    count = await project_count(user.id)
    display = user.first_name or user.username or str(user.id)
    text = (
        f"🌟 *Welcome to God Madara Hosting Bot!*\n\n"
        f"👋 Hello {display}!\n\n"
        f"🚀 *What I can do:*\n"
        f"• Host Python projects 24/7\n"
        f"• Web File Manager — Edit files in browser\n"
        f"• Auto-install requirements.txt\n"
        f"• Real-time logs & monitoring\n"
        f"• Auto-backup to MongoDB GridFS\n"
        f"• Auto-restore after restarts\n"
        f"• Free: 1 project | Premium: Unlimited (up to 10)\n\n"
        f"📊 *Your Status:*\n"
        f"👤 ID: `{user.id}`\n"
        f"💎 Plan: {plan_label}\n"
        f"📁 Projects: {count}/{max_proj}\n\n"
        f"Choose an option below:"
    )
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_keyboard())


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: BOT STATUS CALLBACK (with backup stats)
# ═══════════════════════════════════════════════════════════════════════════════

async def bot_status_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    t0 = time.monotonic()
    total_users   = await users_col.count_documents({})
    premium_users = await users_col.count_documents({"plan": "premium"})
    total_proj    = await projects_col.count_documents({})

    # Count running projects
    running = 0
    async for p in projects_col.find({"pid": {"$ne": None}}):
        if p.get("pid") and psutil.pid_exists(p["pid"]):
            running += 1

    ping_ms = int((time.monotonic() - t0) * 1000)
    stats = get_system_stats()
    py_ver = platform.python_version()

    # Backup stats
    try:
        bstats = await get_backup_stats()
        last_ts = bstats["last_backup_time"]
        if last_ts:
            if hasattr(last_ts, "strftime"):
                last_str = last_ts.strftime("%d %b %H:%M UTC")
            else:
                last_str = str(last_ts)
        else:
            last_str = "Never"
        backup_section = (
            f"\n\n💾 *Backup (GridFS):*\n"
            f"├ Files: {bstats['total_files']}\n"
            f"├ Size: {bstats['total_size_human']}\n"
            f"├ Projects: {bstats['projects_count']}\n"
            f"└ Last Run: {last_str}"
        )
    except Exception as exc:
        logger.error("bot_status_cb: get_backup_stats failed — %s", exc)
        backup_section = "\n\n💾 *Backup:* Stats unavailable"

    text = (
        f"📊 *Bot Dashboard*\n\n"
        f"👥 Total Users: {total_users}\n"
        f"💎 Premium Users: {premium_users}\n"
        f"📁 Total Projects: {total_proj}\n"
        f"🟢 Running Projects: {running}\n"
        f"🗄️ Database: MongoDB ✅\n"
        f"🐍 Python: {py_ver}\n\n"
        f"💻 *System:*\n"
        f"├ CPU: {stats['cpu']}%\n"
        f"├ RAM: {stats['ram_used']}/{stats['ram_total']} ({stats['ram_pct']}%)\n"
        f"└ Disk: {stats['disk_used']}/{stats['disk_total']} ({stats['disk_pct']}%)\n\n"
        f"🏓 Response Ping: {ping_ms}ms"
        + backup_section
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔃 Refresh", callback_data="bot_status"),
         InlineKeyboardButton("💾 Force Backup", callback_data="admin_force_backup")],
        [InlineKeyboardButton("⬅️ Back", callback_data="main_menu")],
    ])
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: PREMIUM CALLBACK
# ═══════════════════════════════════════════════════════════════════════════════

async def premium_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    await check_premium_expiry(user.id)
    doc = await get_or_create_user(user)
    is_prem = doc.get("plan") == "premium"
    status_line = "✨ You are Premium! ✨" if is_prem else "🔒 You are on Free Plan"
    text = (
        f"💎 *Premium Membership*\n\n"
        f"{status_line}\n\n"
        f"*Free Plan:*\n"
        f"• 1 Project only\n"
        f"• File Manager (10 min session)\n\n"
        f"*Premium Plan:*\n"
        f"• ✅ Unlimited projects (up to 10)\n"
        f"• ✅ Priority support\n"
        f"• ✅ Extended file manager\n"
        f"• ✅ Advanced monitoring\n\n"
    )
    if is_prem:
        text += "Premium is active! Enjoy your benefits 🎉"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="main_menu")]])
    else:
        text += "Contact owner for Premium:"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Contact Owner", url=f"tg://user?id={OWNER_ID}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="main_menu")],
        ])
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: MY PROJECTS & PROJECT DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

async def my_projects_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    if await is_user_banned(user.id):
        await query.answer("You are banned.", show_alert=True)
        return
    projects = await projects_col.find({"user_id": user.id}).to_list(length=50)
    if not projects:
        await query.edit_message_text(
            "📁 *My Projects*\n\nYou have no projects yet. Create one with 🆕 New Project!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🆕 New Project", callback_data="new_project")],
                [InlineKeyboardButton("⬅️ Back", callback_data="main_menu")],
            ])
        )
        return
    buttons = []
    for p in projects:
        pid = p.get("pid")
        status_icon = "🟢" if (pid and psutil.pid_exists(pid)) else "🔴"
        buttons.append([InlineKeyboardButton(
            f"{status_icon} {p['name']}",
            callback_data=f"proj_dash:{str(p['_id'])}"
        )])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="main_menu")])
    await query.edit_message_text(
        "📁 *My Projects*\n\nSelect a project to manage:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def project_dashboard_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    proj_id = query.data.split(":", 1)[1]
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    pid = project.get("pid")
    is_running = bool(pid and psutil.pid_exists(pid))
    status_icon = "🟢 Running" if is_running else "🔴 Stopped"
    last_run = project.get("last_run")
    last_run_str = last_run.strftime("%Y-%m-%d %H:%M UTC") if last_run else "Never"
    uptime = uptime_str(project.get("started_at")) if is_running else "N/A"
    text = (
        f"📊 *Project: {project['name']}*\n\n"
        f"🔹 Status: {status_icon}\n"
        f"🔹 PID: {pid or 'N/A'}\n"
        f"🔹 Uptime: {uptime}\n"
        f"🔹 Last Run: {last_run_str}\n"
        f"🔹 Exit Code: {project.get('exit_code', 'None')}\n"
        f"🔹 Run Command: `{project.get('run_command', 'python3 main.py')}`\n"
        f"📅 Created: {project['created_at'].strftime('%Y-%m-%d %H:%M UTC')}"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=project_dashboard_keyboard(project)
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: RUN / STOP / RESTART / LOGS / REFRESH / FILE MANAGER / DELETE
# ═══════════════════════════════════════════════════════════════════════════════

async def run_project_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("▶️ Starting project…")
    proj_id = query.data.split(":", 1)[1]
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    if project.get("admin_stopped"):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📩 Contact Owner", url=f"tg://user?id={OWNER_ID}")]])
        await query.edit_message_text(
            "⚠️ Your project was stopped by admin. Contact owner.",
            reply_markup=kb
        )
        return
    proc = start_project_process(project)
    if not proc:
        await query.answer("❌ Failed to start project.", show_alert=True)
        return
    now = datetime.utcnow()
    await projects_col.update_one(
        {"_id": ObjectId(proj_id)},
        {"$set": {"status": "running", "pid": proc.pid, "last_run": now, "started_at": now, "exit_code": None}}
    )
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    pid = project.get("pid")
    is_running = bool(pid and psutil.pid_exists(pid))
    status_icon = "🟢 Running" if is_running else "🔴 Stopped"
    text = (
        f"📊 *Project: {project['name']}*\n\n"
        f"🔹 Status: {status_icon}\n"
        f"🔹 PID: {pid or 'N/A'}\n"
        f"🔹 Uptime: {uptime_str(project.get('started_at'))}\n"
        f"🔹 Last Run: {project.get('last_run').strftime('%Y-%m-%d %H:%M UTC') if project.get('last_run') else 'Never'}\n"
        f"🔹 Exit Code: {project.get('exit_code', 'None')}\n"
        f"🔹 Run Command: `{project.get('run_command', 'python3 main.py')}`\n"
        f"📅 Created: {project['created_at'].strftime('%Y-%m-%d %H:%M UTC')}"
    )
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=project_dashboard_keyboard(project))


async def stop_project_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⏹️ Stopping project…")
    proj_id = query.data.split(":", 1)[1]
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    pid = project.get("pid")
    await stop_project_process(proj_id, pid)
    await projects_col.update_one(
        {"_id": ObjectId(proj_id)},
        {"$set": {"status": "stopped", "pid": None}}
    )
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    await query.edit_message_text(
        f"📊 *Project: {project['name']}*\n\n"
        f"🔹 Status: 🔴 Stopped\n"
        f"🔹 PID: N/A\n"
        f"🔹 Uptime: N/A\n"
        f"🔹 Last Run: {project.get('last_run').strftime('%Y-%m-%d %H:%M UTC') if project.get('last_run') else 'Never'}\n"
        f"🔹 Exit Code: {project.get('exit_code', 'None')}\n"
        f"🔹 Run Command: `{project.get('run_command', 'python3 main.py')}`\n"
        f"📅 Created: {project['created_at'].strftime('%Y-%m-%d %H:%M UTC')}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=project_dashboard_keyboard(project)
    )


async def restart_project_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🔄 Restarting…")
    proj_id = query.data.split(":", 1)[1]
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    if project.get("admin_stopped"):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📩 Contact Owner", url=f"tg://user?id={OWNER_ID}")]])
        await query.edit_message_text("⚠️ Your project was stopped by admin. Contact owner.", reply_markup=kb)
        return
    pid = project.get("pid")
    await stop_project_process(proj_id, pid)
    proc = start_project_process(project)
    if not proc:
        await query.answer("❌ Failed to restart.", show_alert=True)
        return
    now = datetime.utcnow()
    await projects_col.update_one(
        {"_id": ObjectId(proj_id)},
        {"$set": {"status": "running", "pid": proc.pid, "last_run": now, "started_at": now, "exit_code": None}}
    )
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    pid = project.get("pid")
    text = (
        f"📊 *Project: {project['name']}*\n\n"
        f"🔹 Status: 🟢 Running\n"
        f"🔹 PID: {pid}\n"
        f"🔹 Uptime: {uptime_str(project.get('started_at'))}\n"
        f"🔹 Last Run: {project.get('last_run').strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"🔹 Exit Code: None\n"
        f"🔹 Run Command: `{project.get('run_command', 'python3 main.py')}`\n"
        f"📅 Created: {project['created_at'].strftime('%Y-%m-%d %H:%M UTC')}"
    )
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=project_dashboard_keyboard(project))


async def logs_project_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    proj_id = query.data.split(":", 1)[1]
    logs = read_logs(proj_id)
    if len(logs) > 3800:
        logs = "…(truncated)\n" + logs[-3800:]
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔃 Refresh Logs", callback_data=f"logs_proj:{proj_id}"),
        InlineKeyboardButton("⬅️ Back", callback_data=f"proj_dash:{proj_id}"),
    ]])
    await query.edit_message_text(
        f"📋 *Logs (last 50 lines):*\n\n```\n{logs}\n```",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb
    )


async def refresh_project_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🔃 Refreshed!")
    proj_id = query.data.split(":", 1)[1]
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    # Update exit code if process ended
    proc = running_procs.get(proj_id)
    if proc:
        ret = proc.poll()
        if ret is not None:
            await projects_col.update_one(
                {"_id": ObjectId(proj_id)},
                {"$set": {"status": "stopped", "pid": None, "exit_code": ret}}
            )
            project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    pid = project.get("pid")
    is_running = bool(pid and psutil.pid_exists(pid))
    status_icon = "🟢 Running" if is_running else "🔴 Stopped"
    last_run = project.get("last_run")
    last_run_str = last_run.strftime("%Y-%m-%d %H:%M UTC") if last_run else "Never"
    uptime = uptime_str(project.get("started_at")) if is_running else "N/A"
    text = (
        f"📊 *Project: {project['name']}*\n\n"
        f"🔹 Status: {status_icon}\n"
        f"🔹 PID: {pid or 'N/A'}\n"
        f"🔹 Uptime: {uptime}\n"
        f"🔹 Last Run: {last_run_str}\n"
        f"🔹 Exit Code: {project.get('exit_code', 'None')}\n"
        f"🔹 Run Command: `{project.get('run_command', 'python3 main.py')}`\n"
        f"📅 Created: {project['created_at'].strftime('%Y-%m-%d %H:%M UTC')}"
    )
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=project_dashboard_keyboard(project))


async def fm_project_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    proj_id = query.data.split(":", 1)[1]
    user = update.effective_user
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    token = generate_fm_token(proj_id, user.id)
    # Write a meta file so file_manager.py can resolve the project path
    meta_file = PROJECTS_DIR / f"meta_{proj_id}.txt"
    meta_file.write_text(project["path"])
    fm_url = f"{FM_BASE_URL}/fm/{token}"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 Open File Manager", url=fm_url)],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"proj_dash:{proj_id}")],
    ])
    await query.edit_message_text(
        f"📂 *File Manager*\n\n"
        f"Your temporary file manager link (valid 1 hour):\n`{fm_url}`\n\n"
        f"Click the button below to open it in your browser.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb
    )
    # Trigger background backup after FM session is opened
    ppath = Path(project["path"])
    asyncio.create_task(
        backup_project_files(user.id, project["name"], ppath)
    )


async def delete_confirm_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    proj_id = query.data.split(":", 1)[1]
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Yes, Delete", callback_data=f"delete_confirm_yes:{proj_id}"),
         InlineKeyboardButton("❌ Cancel", callback_data=f"proj_dash:{proj_id}")],
    ])
    await query.edit_message_text(
        "⚠️ *Are you sure?*\n\nThis will permanently delete the project, all its files, and all backups.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb
    )


async def delete_yes_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🗑️ Deleting…")
    proj_id = query.data.split(":", 1)[1]
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return

    uid = project.get("user_id")
    pname = project.get("name", "")

    # Stop the process
    pid = project.get("pid")
    await stop_project_process(proj_id, pid)

    # Delete files from disk
    proj_path = Path(project["path"])
    if proj_path.exists():
        shutil.rmtree(str(proj_path), ignore_errors=True)

    # Delete log file
    log_path = get_log_path(proj_id)
    if log_path.exists():
        log_path.unlink()

    # Delete meta file
    meta_file = PROJECTS_DIR / f"meta_{proj_id}.txt"
    if meta_file.exists():
        meta_file.unlink()

    # Delete from MongoDB
    await projects_col.delete_one({"_id": ObjectId(proj_id)})

    # Delete GridFS backup files
    try:
        deleted_count = await delete_project_backup(uid, pname)
        logger.info("delete_yes_cb: 🗑️ %s — deleted %d GridFS backup files", pname, deleted_count)
    except Exception as exc:
        logger.error("delete_yes_cb: GridFS cleanup failed for %s — %s", pname, exc)

    await query.edit_message_text("✅ Project deleted successfully (files + backups).", reply_markup=back_to_main())


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: NEW PROJECT CONVERSATION
# ═══════════════════════════════════════════════════════════════════════════════

async def new_project_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    if await is_user_banned(user.id):
        await query.answer("You are banned.", show_alert=True)
        return ConversationHandler.END
    await check_premium_expiry(user.id)
    doc = await get_or_create_user(user)
    max_proj = 10 if doc["plan"] == "premium" else 1
    count = await project_count(user.id)
    if count >= max_proj:
        await query.edit_message_text(
            f"❌ You've reached your project limit ({max_proj}).\n"
            f"Upgrade to Premium for up to 10 projects!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💎 Premium", callback_data="premium")],
                [InlineKeyboardButton("⬅️ Back", callback_data="main_menu")],
            ])
        )
        return ConversationHandler.END
    ctx.user_data["np_chat_id"] = query.message.chat_id
    ctx.user_data["np_msg_id"] = query.message.message_id
    await query.edit_message_text(
        "🆕 *New Project*\n\nEnter a name for your project:\n_(Letters, numbers, - and _ only. Max 32 chars.)_\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]])
    )
    return NP_NAME


async def np_got_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    user = update.effective_user
    if not is_valid_project_name(name):
        await update.message.reply_text(
            "❌ Invalid name. Use only letters, numbers, - and _. Max 32 chars.\n\nTry again or /cancel."
        )
        return NP_NAME
    existing = await projects_col.find_one({"user_id": user.id, "name": name})
    if existing:
        await update.message.reply_text(
            f"❌ You already have a project named *{name}*. Choose a different name.\n\n/cancel to abort.",
            parse_mode=ParseMode.MARKDOWN
        )
        return NP_NAME
    ctx.user_data["np_name"] = name
    ctx.user_data["np_files"] = []
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Done Uploading", callback_data="np_done_upload")]])
    await update.message.reply_text(
        f"✅ Project name: *{name}*\n\n"
        f"📤 Now upload your project files one by one, or send a single ZIP archive.\n"
        f"When finished, click *Done Uploading*.\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb
    )
    return NP_FILES


async def np_got_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Receives a file uploaded during the new-project wizard.
    Saves it to disk, then immediately backs it up to GridFS.
    """
    user = update.effective_user
    name = ctx.user_data.get("np_name", "project")
    proj_path = get_project_path(user.id, name)
    proj_path.mkdir(parents=True, exist_ok=True)
    doc = update.message.document
    if not doc:
        await update.message.reply_text("Please send a document/file.")
        return NP_FILES
    file = await doc.get_file()
    dest = proj_path / doc.file_name
    await file.download_to_drive(str(dest))
    ctx.user_data.setdefault("np_files", []).append(doc.file_name)

    # Backup single file to GridFS immediately after upload
    try:
        await backup_single_file(user.id, name, dest, proj_path)
        logger.debug("np_got_file: backed up %s to GridFS", doc.file_name)
    except Exception as exc:
        logger.error("np_got_file: backup failed for %s — %s", doc.file_name, exc)

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Done Uploading", callback_data="np_done_upload")]])
    await update.message.reply_text(
        f"📎 File received: *{doc.file_name}*\n\nSend more files or click *Done Uploading*.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb
    )
    return NP_FILES


async def np_done_upload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Called when user clicks Done Uploading.
    Extracts ZIP if present, installs requirements, then backs up all project files to GridFS.
    """
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    name = ctx.user_data.get("np_name", "project")
    files = ctx.user_data.get("np_files", [])
    if not files:
        await query.edit_message_text(
            "❌ No files uploaded yet. Please upload at least one file.\n\n/cancel to abort.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]])
        )
        return NP_FILES
    proj_path = get_project_path(user.id, name)

    # Extract ZIP if applicable
    for fname in files:
        fpath = proj_path / fname
        if fname.endswith(".zip"):
            await query.edit_message_text(f"📦 Extracting {fname}…")
            with zipfile.ZipFile(str(fpath), "r") as z:
                z.extractall(str(proj_path))
            fpath.unlink()

    # Install requirements
    req_file = proj_path / "requirements.txt"
    if req_file.exists():
        await query.edit_message_text("📦 Found requirements.txt — Installing packages…")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", str(req_file)],
                capture_output=True, text=True, timeout=120
            )
            if result.returncode == 0:
                await query.edit_message_text("✅ Packages installed successfully!\n\nNow enter the run command (e.g., `python3 main.py`) or /skip for default:")
            else:
                await query.edit_message_text(
                    f"⚠️ Some packages may have failed:\n```\n{result.stderr[-1000:]}\n```\n\nNow enter the run command:",
                    parse_mode=ParseMode.MARKDOWN
                )
        except subprocess.TimeoutExpired:
            await query.edit_message_text("⚠️ Installation timed out. Continuing…\n\nEnter the run command:")
    else:
        await query.edit_message_text(
            "ℹ️ No requirements.txt found.\n\nEnter the run command (e.g., `python3 main.py`) or send /skip for default:",
            parse_mode=ParseMode.MARKDOWN
        )

    # Backup all project files to GridFS after ZIP extraction
    try:
        summary = await backup_project_files(user.id, name, proj_path)
        logger.info("np_done_upload: backup summary for %s — %s", name, summary)
    except Exception as exc:
        logger.error("np_done_upload: backup failed for %s — %s", name, exc)

    return NP_CMD


async def np_got_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text.strip()
    if text.lower() in ("/skip", "skip"):
        cmd = "python3 main.py"
    else:
        cmd = text
    name = ctx.user_data.get("np_name", "project")
    proj_path = get_project_path(user.id, name)
    doc = {
        "user_id": user.id,
        "name": name,
        "path": str(proj_path),
        "run_command": cmd,
        "status": "stopped",
        "pid": None,
        "created_at": datetime.utcnow(),
        "last_run": None,
        "exit_code": None,
        "admin_stopped": False,
        "auto_restart": False,
    }
    result = await projects_col.insert_one(doc)
    proj_id = str(result.inserted_id)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Run Now", callback_data=f"run_proj:{proj_id}"),
         InlineKeyboardButton("📁 My Projects", callback_data="my_projects")],
        [InlineKeyboardButton("⬅️ Main Menu", callback_data="main_menu")],
    ])
    await update.message.reply_text(
        f"🎉 *Project created!*\n\n"
        f"📁 Name: *{name}*\n"
        f"🔹 Run Command: `{cmd}`\n"
        f"📅 Created: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        f"What would you like to do?",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb
    )
    ctx.user_data.clear()
    return ConversationHandler.END


async def cancel_conv(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    if update.message:
        await update.message.reply_text("❌ Cancelled.", reply_markup=back_to_main())
    elif update.callback_query:
        await update.callback_query.edit_message_text("❌ Cancelled.", reply_markup=back_to_main())
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: EDIT RUN COMMAND CONVERSATION
# ═══════════════════════════════════════════════════════════════════════════════

async def editcmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    proj_id = query.data.split(":", 1)[1]
    ctx.user_data["editcmd_proj_id"] = proj_id
    await query.edit_message_text(
        "✏️ *Edit Run Command*\n\nEnter the new run command (e.g., `python3 main.py`):\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"proj_dash:{proj_id}")]])
    )
    return EDIT_CMD


async def editcmd_got(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    new_cmd = update.message.text.strip()
    proj_id = ctx.user_data.get("editcmd_proj_id")
    await projects_col.update_one(
        {"_id": ObjectId(proj_id)},
        {"$set": {"run_command": new_cmd}}
    )
    await update.message.reply_text(
        f"✅ Run command updated to:\n`{new_cmd}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Project", callback_data=f"proj_dash:{proj_id}")]])
    )
    ctx.user_data.clear()
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: ADMIN PANEL
# ═══════════════════════════════════════════════════════════════════════════════

async def admin_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("🚫 You are not the owner.")
        return
    await update.message.reply_text(
        "🔐 *Admin Panel*\n\nWelcome, Owner!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=admin_keyboard()
    )


async def admin_panel_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        await query.answer("Not authorized.", show_alert=True)
        return
    await query.edit_message_text(
        "🔐 *Admin Panel*\n\nWelcome, Owner!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=admin_keyboard()
    )


async def admin_users_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    page = int(query.data.split(":", 1)[1])
    per_page = 10
    total = await users_col.count_documents({})
    users_list = await users_col.find({}).skip(page * per_page).limit(per_page).to_list(length=per_page)
    buttons = []
    for u in users_list:
        uname = u.get("username") or u.get("first_name") or str(u["user_id"])
        plan_icon = "💎" if u.get("plan") == "premium" else "👤"
        ban_icon = "🚫" if u.get("banned") else ""
        buttons.append([InlineKeyboardButton(
            f"{plan_icon}{ban_icon} {uname} ({u['user_id']})",
            callback_data=f"admin_user_detail:{u['user_id']}"
        )])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"admin_users:{page - 1}"))
    if (page + 1) * per_page < total:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"admin_users:{page + 1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_panel")])
    await query.edit_message_text(
        f"👥 *All Users* (Page {page + 1}, {total} total):",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def admin_user_detail_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    target_id = int(query.data.split(":", 1)[1])
    u = await users_col.find_one({"user_id": target_id})
    if not u:
        await query.answer("User not found.", show_alert=True)
        return
    proj_count = await projects_col.count_documents({"user_id": target_id})
    prem_exp = u.get("premium_expiry")
    prem_exp_str = prem_exp.strftime("%Y-%m-%d") if prem_exp else "N/A"
    text = (
        f"👤 *User Detail*\n\n"
        f"ID: `{u['user_id']}`\n"
        f"Name: {u.get('first_name', 'N/A')}\n"
        f"Username: @{u.get('username', 'N/A')}\n"
        f"Plan: {u.get('plan', 'free')}\n"
        f"Premium Expiry: {prem_exp_str}\n"
        f"Banned: {'Yes 🚫' if u.get('banned') else 'No'}\n"
        f"Projects: {proj_count}\n"
        f"Joined: {u['joined_at'].strftime('%Y-%m-%d') if u.get('joined_at') else 'N/A'}"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_users:0")]])
    )


async def admin_running_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    lines = []
    buttons = []
    async for p in projects_col.find({"pid": {"$ne": None}}):
        pid = p.get("pid")
        if not (pid and psutil.pid_exists(pid)):
            continue
        u = await users_col.find_one({"user_id": p["user_id"]})
        uname = (u.get("username") or u.get("first_name") or str(p["user_id"])) if u else str(p["user_id"])
        dur = uptime_str(p.get("started_at"))
        proj_id = str(p["_id"])
        lines.append(
            f"👤 Username: @{uname}\n"
            f"🆔 User ID: {p['user_id']}\n"
            f"📊 Project PID: {pid}\n"
            f"⏱️ Running Time: {dur}\n"
            f"📜 Project: {p['name']}\n"
        )
        buttons.append([
            InlineKeyboardButton("⏹️ Stop", callback_data=f"admin_stop_proj:{proj_id}"),
        ])
    if not lines:
        await query.edit_message_text(
            "🟢 No projects currently running.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_panel")]])
        )
        return
    text = "🟢 *Running Projects:*\n\n" + "\n---\n".join(lines)
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_panel")])
    if len(text) > 4000:
        text = text[:4000] + "\n…"
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


async def admin_stop_proj_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⏹️ Stopping…")
    if update.effective_user.id != OWNER_ID:
        return
    proj_id = query.data.split(":", 1)[1]
    project = await projects_col.find_one({"_id": ObjectId(proj_id)})
    if project:
        await stop_project_process(proj_id, project.get("pid"))
        await projects_col.update_one(
            {"_id": ObjectId(proj_id)},
            {"$set": {"status": "stopped", "pid": None, "admin_stopped": True}}
        )
    await query.answer("✅ Project stopped.", show_alert=True)
    update.callback_query.data = "admin_running"
    await admin_running_cb(update, ctx)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: ADMIN FORCE BACKUP
# ═══════════════════════════════════════════════════════════════════════════════

async def admin_force_backup_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Admin panel: Force Backup button.
    Iterates over all projects and backs them up immediately, then shows a summary.
    """
    query = update.callback_query
    await query.answer("⏳ Running backup…")

    if update.effective_user.id != OWNER_ID:
        await query.answer("Not authorized.", show_alert=True)
        return

    await query.edit_message_text("⏳ *Running full backup…*\n\nThis may take a moment.", parse_mode=ParseMode.MARKDOWN)

    total_backed_up = 0
    total_skipped = 0
    total_failed = 0
    projects_done = 0

    try:
        async for project_doc in projects_col.find({}):
            uid = int(project_doc.get("user_id", 0))
            pname = project_doc.get("name", "")
            if not pname:
                continue
            ppath = PROJECTS_DIR / str(uid) / pname
            if not ppath.is_dir():
                continue
            summary = await backup_project_files(uid, pname, ppath)
            total_backed_up += summary["backed_up"]
            total_skipped   += summary["skipped"]
            total_failed    += summary["failed"]
            projects_done   += 1

        await backup_meta_col.update_one(
            {"_id": "global"},
            {"$set": {
                "last_backup_time": datetime.now(timezone.utc),
                "last_projects_count": projects_done,
                "last_files_count": total_backed_up,
            }},
            upsert=True,
        )

        result_text = (
            f"💾 *Backup Complete*\n\n"
            f"✅ Projects scanned: {projects_done}\n"
            f"📤 Uploaded: {total_backed_up} files\n"
            f"⏭️ Skipped (unchanged): {total_skipped} files\n"
            f"❌ Failed: {total_failed} files"
        )

    except Exception as exc:
        logger.error("admin_force_backup_cb: error — %s", exc, exc_info=True)
        result_text = f"❌ *Backup failed*\n\n`{exc}`"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔃 Refresh Stats", callback_data="bot_status"),
         InlineKeyboardButton("⬅️ Admin Panel", callback_data="admin_panel")],
    ])
    await query.edit_message_text(result_text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: ADMIN CONVERSATIONS — GIVE/REMOVE/TEMP PREMIUM, BAN/UNBAN, BROADCAST
# ═══════════════════════════════════════════════════════════════════════════════

async def admin_give_prem_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text(
        "💎 *Give Premium*\n\nEnter the user ID to grant Premium:\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADMIN_GIVE_PREM


async def admin_give_prem_got(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Try again or /cancel.")
        return ADMIN_GIVE_PREM
    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"plan": "premium", "premium_expiry": None}},
        upsert=True
    )
    await update.message.reply_text(f"✅ Premium granted to user {uid}.", reply_markup=back_to_main())
    return ConversationHandler.END


async def admin_rem_prem_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text(
        "❌ *Remove Premium*\n\nEnter the user ID to remove Premium:\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADMIN_REM_PREM


async def admin_rem_prem_got(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Try again or /cancel.")
        return ADMIN_REM_PREM
    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"plan": "free", "premium_expiry": None}}
    )
    await update.message.reply_text(f"✅ Premium removed from user {uid}.", reply_markup=back_to_main())
    return ConversationHandler.END


async def admin_temp_prem_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text(
        "⏰ *Temp Premium — Step 1*\n\nEnter the user ID:\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADMIN_TEMP_PREM_ID


async def admin_temp_prem_got_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Try again or /cancel.")
        return ADMIN_TEMP_PREM_ID
    ctx.user_data["temp_prem_uid"] = uid
    await update.message.reply_text(
        "⏰ *Temp Premium — Step 2*\n\nEnter the number of days:\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADMIN_TEMP_PREM_DAYS


async def admin_temp_prem_got_days(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        days = int(update.message.text.strip())
        if days <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Invalid number. Try again or /cancel.")
        return ADMIN_TEMP_PREM_DAYS
    uid = ctx.user_data.get("temp_prem_uid")
    expiry = datetime.utcnow() + timedelta(days=days)
    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"plan": "premium", "premium_expiry": expiry}},
        upsert=True
    )
    await update.message.reply_text(
        f"✅ Temp Premium ({days} days) granted to user {uid}. Expires: {expiry.strftime('%Y-%m-%d')}",
        reply_markup=back_to_main()
    )
    ctx.user_data.clear()
    return ConversationHandler.END


async def admin_ban_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text(
        "🚫 *Ban User*\n\nEnter the user ID to ban:\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADMIN_BAN


async def admin_ban_got(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Try again or /cancel.")
        return ADMIN_BAN
    await users_col.update_one({"user_id": uid}, {"$set": {"banned": True}}, upsert=True)
    await update.message.reply_text(f"✅ User {uid} has been banned.", reply_markup=back_to_main())
    return ConversationHandler.END


async def admin_unban_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text(
        "✅ *Unban User*\n\nEnter the user ID to unban:\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADMIN_UNBAN


async def admin_unban_got(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Try again or /cancel.")
        return ADMIN_UNBAN
    await users_col.update_one({"user_id": uid}, {"$set": {"banned": False}})
    await update.message.reply_text(f"✅ User {uid} has been unbanned.", reply_markup=back_to_main())
    return ConversationHandler.END


async def admin_broadcast_menu_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    await query.edit_message_text(
        "📢 *Broadcast*\n\nChoose broadcast type:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Broadcast All", callback_data="admin_bc_all"),
             InlineKeyboardButton("📨 Send to User", callback_data="admin_bc_user")],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin_panel")],
        ])
    )


async def admin_bc_all_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    ctx.user_data["bc_type"] = "all"
    await query.edit_message_text(
        "📢 *Broadcast All*\n\nEnter the message to send to all users:\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADMIN_BROADCAST_MSG


async def admin_bc_all_got(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg_text = update.message.text.strip()
    all_users = await users_col.find({}).to_list(length=10000)
    sent = 0
    failed = 0
    for u in all_users:
        try:
            await update.get_bot().send_message(u["user_id"], msg_text)
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(
        f"✅ Broadcast done!\nSent: {sent} | Failed: {failed}",
        reply_markup=back_to_main()
    )
    ctx.user_data.clear()
    return ConversationHandler.END


async def admin_bc_user_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    ctx.user_data["bc_type"] = "user"
    await query.edit_message_text(
        "📨 *Send to User — Step 1*\n\nEnter the target user ID:\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADMIN_SEND_USER_ID


async def admin_bc_user_got_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Try again or /cancel.")
        return ADMIN_SEND_USER_ID
    ctx.user_data["bc_target_id"] = uid
    await update.message.reply_text(
        "📨 *Send to User — Step 2*\n\nEnter the message:\n\n/cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ADMIN_SEND_USER_MSG


async def admin_bc_user_got_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg_text = update.message.text.strip()
    uid = ctx.user_data.get("bc_target_id")
    try:
        await update.get_bot().send_message(uid, msg_text)
        await update.message.reply_text(f"✅ Message sent to user {uid}.", reply_markup=back_to_main())
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to send: {e}", reply_markup=back_to_main())
    ctx.user_data.clear()
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: APPLICATION SETUP & post_init
# ═══════════════════════════════════════════════════════════════════════════════

async def post_init(app: Application) -> None:
    """
    Called by PTB after the Application is initialised but before polling starts.
    Execution order:
        1. Set bot commands
        2. restore_all_projects()           ← recreate /projects/ from GridFS
        3. auto_restart_running_projects()  ← restart previously-running bots
        4. Launch background tasks:
               • auto_backup_loop()
               • process_watcher()
    """
    # 1. Bot commands
    await app.bot.set_my_commands([
        BotCommand("start",  "Open main menu"),
        BotCommand("admin",  "Admin panel (owner only)"),
        BotCommand("cancel", "Cancel current operation"),
    ])
    logger.info("post_init: ✅ bot commands set.")

    # 2. Restore project files from GridFS
    logger.info("post_init: 🔄 restoring projects from GridFS…")
    try:
        restored = await restore_all_projects()
        logger.info("post_init: ✅ restored %d project(s): %s", len(restored), restored)
    except Exception as exc:
        logger.error("post_init: ❌ restore_all_projects failed — %s", exc, exc_info=True)

    # 3. Auto-restart previously running projects
    logger.info("post_init: 🚀 restarting previously running projects…")
    try:
        restarted = await auto_restart_running_projects()
        logger.info("post_init: ✅ auto-restarted %d project(s): %s", len(restarted), restarted)
    except Exception as exc:
        logger.error("post_init: ❌ auto_restart_running_projects failed — %s", exc, exc_info=True)

    # 4. Launch background tasks
    asyncio.create_task(auto_backup_loop(interval_minutes=10))
    logger.info("post_init: ✅ auto_backup_loop started (10 min interval).")

    asyncio.create_task(process_watcher(interval_seconds=60))
    logger.info("post_init: ✅ process_watcher started (60 s interval).")


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # ── New Project conversation ──────────────────────────────────────────────
    new_proj_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(new_project_start, pattern="^new_project$")],
        states={
            NP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, np_got_name)],
            NP_FILES: [
                MessageHandler(filters.Document.ALL, np_got_file),
                CallbackQueryHandler(np_done_upload, pattern="^np_done_upload$"),
            ],
            NP_CMD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, np_got_cmd),
                CommandHandler("skip", np_got_cmd),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_user=True, per_chat=True,
    )

    # ── Edit run command conversation ─────────────────────────────────────────
    editcmd_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(editcmd_start, pattern=r"^editcmd_proj:")],
        states={
            EDIT_CMD: [MessageHandler(filters.TEXT & ~filters.COMMAND, editcmd_got)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_user=True, per_chat=True,
    )

    # ── Admin conversations ───────────────────────────────────────────────────
    admin_give_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_give_prem_start, pattern="^admin_give_prem$")],
        states={ADMIN_GIVE_PREM: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_give_prem_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_user=True, per_chat=True,
    )
    admin_rem_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_rem_prem_start, pattern="^admin_rem_prem$")],
        states={ADMIN_REM_PREM: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rem_prem_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_user=True, per_chat=True,
    )
    admin_temp_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_temp_prem_start, pattern="^admin_temp_prem$")],
        states={
            ADMIN_TEMP_PREM_ID:   [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_prem_got_id)],
            ADMIN_TEMP_PREM_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_prem_got_days)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_user=True, per_chat=True,
    )
    admin_ban_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_ban_start, pattern="^admin_ban$")],
        states={ADMIN_BAN: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ban_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_user=True, per_chat=True,
    )
    admin_unban_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_unban_start, pattern="^admin_unban$")],
        states={ADMIN_UNBAN: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_unban_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_user=True, per_chat=True,
    )
    admin_bc_all_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_bc_all_start, pattern="^admin_bc_all$")],
        states={ADMIN_BROADCAST_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_bc_all_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_user=True, per_chat=True,
    )
    admin_bc_user_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_bc_user_start, pattern="^admin_bc_user$")],
        states={
            ADMIN_SEND_USER_ID:  [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_bc_user_got_id)],
            ADMIN_SEND_USER_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_bc_user_got_msg)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_user=True, per_chat=True,
    )

    # ── Register command handlers ─────────────────────────────────────────────
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_cmd))

    # ── Register conversation handlers ────────────────────────────────────────
    app.add_handler(new_proj_conv)
    app.add_handler(editcmd_conv)
    app.add_handler(admin_give_conv)
    app.add_handler(admin_rem_conv)
    app.add_handler(admin_temp_conv)
    app.add_handler(admin_ban_conv)
    app.add_handler(admin_unban_conv)
    app.add_handler(admin_bc_all_conv)
    app.add_handler(admin_bc_user_conv)

    # ── Register callback query handlers (non-conversation) ───────────────────
    app.add_handler(CallbackQueryHandler(main_menu_cb,              pattern="^main_menu$"))
    app.add_handler(CallbackQueryHandler(bot_status_cb,             pattern="^bot_status$"))
    app.add_handler(CallbackQueryHandler(premium_cb,                pattern="^premium$"))
    app.add_handler(CallbackQueryHandler(my_projects_cb,            pattern="^my_projects$"))
    app.add_handler(CallbackQueryHandler(project_dashboard_cb,      pattern=r"^proj_dash:"))
    app.add_handler(CallbackQueryHandler(run_project_cb,            pattern=r"^run_proj:"))
    app.add_handler(CallbackQueryHandler(stop_project_cb,           pattern=r"^stop_proj:"))
    app.add_handler(CallbackQueryHandler(restart_project_cb,        pattern=r"^restart_proj:"))
    app.add_handler(CallbackQueryHandler(logs_project_cb,           pattern=r"^logs_proj:"))
    app.add_handler(CallbackQueryHandler(refresh_project_cb,        pattern=r"^refresh_proj:"))
    app.add_handler(CallbackQueryHandler(fm_project_cb,             pattern=r"^fm_proj:"))
    app.add_handler(CallbackQueryHandler(delete_confirm_cb,         pattern=r"^delete_proj:"))
    app.add_handler(CallbackQueryHandler(delete_yes_cb,             pattern=r"^delete_confirm_yes:"))
    app.add_handler(CallbackQueryHandler(admin_panel_cb,            pattern="^admin_panel$"))
    app.add_handler(CallbackQueryHandler(admin_users_cb,            pattern=r"^admin_users:"))
    app.add_handler(CallbackQueryHandler(admin_user_detail_cb,      pattern=r"^admin_user_detail:"))
    app.add_handler(CallbackQueryHandler(admin_running_cb,          pattern="^admin_running$"))
    app.add_handler(CallbackQueryHandler(admin_stop_proj_cb,        pattern=r"^admin_stop_proj:"))
    app.add_handler(CallbackQueryHandler(admin_broadcast_menu_cb,   pattern="^admin_broadcast_menu$"))
    app.add_handler(CallbackQueryHandler(admin_force_backup_cb,     pattern="^admin_force_backup$"))

    return app


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    app = build_app()
    logger.info("🚀 God Madara Hosting Bot starting…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
