# main.py — God Madara Hosting Bot (FINAL MERGED)
# Combines: Telegram Bot (bot.py) + Web File Manager (file_manager.py)
# Production-ready, async Python Telegram bot + Flask file manager in ONE file
# Python 3.11+ | python-telegram-bot==21.3 | motor | Flask | psutil
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
import tempfile
import threading
import time
import uuid
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import psutil
from bson import ObjectId
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket
from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from flask import (
    Flask,
    abort,
    flash,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_from_directory,
    url_for,
)
from werkzeug.utils import secure_filename

load_dotenv()

# ═══ LOGGING ════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ═══ ENVIRONMENT CONFIG ════════════════════════════════════════════════════

BOT_TOKEN    = os.environ["BOT_TOKEN"]
OWNER_ID     = int(os.environ["OWNER_ID"])
MONGODB_URI  = os.environ["MONGODB_URI"]
FM_SECRET    = os.environ.get("FM_SECRET", "changeme_secret")
PROJECTS_DIR = Path(os.environ.get("PROJECTS_DIR", "projects"))
TOKENS_DIR   = Path("fm_tokens")
PORT         = int(os.environ.get("PORT", 8080))

# FIX #4: Renamed FM_BASE_URL → FILE_MANAGER_URL
FILE_MANAGER_URL = os.environ.get("FILE_MANAGER_URL", f"http://localhost:{PORT}")

# FIX #11: Only "free" and "premium" plan types
PLAN_LIMITS = {
    "free":    1,
    "premium": 10,
}

MAX_LOG_LINES = 200

# ═══ MONGODB SETUP ═════════════════════════════════════════════════════════

_mongo_client    = AsyncIOMotorClient(MONGODB_URI)
_db              = _mongo_client["god_madara"]
users_col        = _db["users"]
projects_col     = _db["projects"]
backup_meta_col  = _db["backup_meta"]
fs_bucket        = AsyncIOMotorGridFSBucket(_db, bucket_name="project_files")

# ═══ IN-MEMORY PROCESS TRACKER ═════════════════════════════════════════════

# {project_id: {"proc": subprocess.Popen, "started_at": datetime, "pid": int}}
running_processes: dict[str, dict] = {}

# ═══ CONVERSATION STATES ════════════════════════════════════════════════════

(
    NP_NAME,
    NP_FILE,
    NP_CMD,
    EDIT_CMD,
    GIVE_PREM_ID,
    REM_PREM_ID,
    TEMP_PREM_ID,
    TEMP_PREM_DAYS,
    BAN_ID,
    UNBAN_ID,
    BC_ALL_MSG,
    BC_USER_ID,
    BC_USER_MSG,
) = range(13)

# ═══ HELPER FUNCTIONS ══════════════════════════════════════════════════════

def is_valid_project_name(name: str) -> bool:
    """Alphanumeric, underscores, hyphens, 2–32 chars."""
    return bool(re.match(r'^[a-zA-Z0-9_\-]{2,32}$', name))


# FIX #13: Use "user_id" as regular field, NOT "_id". Let MongoDB auto-generate _id.
async def get_or_create_user(user_id: int, username: str = "", full_name: str = "") -> dict:
    """Fetch or create a user document in MongoDB."""
    doc = await users_col.find_one({"user_id": user_id})
    if doc is None:
        doc = {
            "user_id":         user_id,
            "username":        username,
            "full_name":       full_name,
            "plan":            "free",
            "premium_expiry":  None,
            "banned":          False,
            "joined_at":       datetime.utcnow(),
        }
        await users_col.insert_one(doc)
    else:
        update_fields = {}
        if username and doc.get("username") != username:
            update_fields["username"] = username
        if full_name and doc.get("full_name") != full_name:
            update_fields["full_name"] = full_name
        if update_fields:
            await users_col.update_one({"user_id": user_id}, {"$set": update_fields})
            doc.update(update_fields)
    return doc


async def is_user_banned(user_id: int) -> bool:
    doc = await users_col.find_one({"user_id": user_id}, {"banned": 1})
    return bool(doc and doc.get("banned"))


async def check_premium_expiry(user_id: int) -> None:
    """Downgrade user to free if their premium has expired."""
    doc = await users_col.find_one({"user_id": user_id}, {"plan": 1, "premium_expiry": 1})
    if not doc:
        return
    if doc.get("plan") == "premium" and doc.get("premium_expiry"):
        if datetime.utcnow() > doc["premium_expiry"]:
            await users_col.update_one(
                {"user_id": user_id},
                {"$set": {"plan": "free", "premium_expiry": None}},
            )


# FIX #11: Only "free" and "premium" labels
def get_plan_label(plan: str) -> str:
    labels = {
        "free":    "🆓 Free",
        "premium": "💎 Premium",
    }
    return labels.get(plan, plan.capitalize())


def get_project_path(user_id: int, project_name: str) -> Path:
    return PROJECTS_DIR / str(user_id) / project_name


def get_log_path(user_id: int, project_name: str) -> Path:
    return get_project_path(user_id, project_name) / "bot.log"


def uptime_str(started_at: datetime) -> str:
    delta = datetime.utcnow() - started_at
    total_seconds = int(delta.total_seconds())
    h, rem = divmod(total_seconds, 3600)
    m, s   = divmod(rem, 60)
    return f"{h}h {m}m {s}s"


def bytes_to_gb(b: int) -> float:
    return round(b / (1024 ** 3), 2)


def bytes_human(b: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def get_system_stats() -> dict:
    cpu    = psutil.cpu_percent(interval=0.5)
    mem    = psutil.virtual_memory()
    disk   = psutil.disk_usage("/")
    return {
        "cpu_pct":    cpu,
        "mem_used":   bytes_human(mem.used),
        "mem_total":  bytes_human(mem.total),
        "mem_pct":    mem.percent,
        "disk_used":  bytes_human(disk.used),
        "disk_total": bytes_human(disk.total),
        "disk_pct":   disk.percent,
        "os":         platform.system(),
        "python":     platform.python_version(),
    }


async def project_count(user_id: int) -> int:
    return await projects_col.count_documents({"user_id": user_id})


def generate_fm_token(project_id: str, user_id: int, ttl_minutes: int = 60) -> str:
    """Generate a time-limited file-manager token and persist to disk."""
    token = uuid.uuid4().hex
    TOKENS_DIR.mkdir(exist_ok=True)
    expiry = datetime.utcnow() + timedelta(minutes=ttl_minutes)
    token_file = TOKENS_DIR / f"{token}.txt"
    token_file.write_text(f"{project_id}\n{user_id}\n{expiry.isoformat()}")
    return token


def start_project_process(user_id: int, project_id: str, project_name: str, cmd: str) -> Optional[subprocess.Popen]:
    """Start a project subprocess. Returns Popen or None on failure."""
    project_path = get_project_path(user_id, project_name)
    if not project_path.exists():
        logger.error("Project path does not exist: %s", project_path)
        return None
    log_path = get_log_path(user_id, project_name)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        log_file = open(str(log_path), "a", encoding="utf-8")
        log_file.write(f"\n{'='*50}\n[{datetime.utcnow().isoformat()}] Starting: {cmd}\nCWD: {project_path}\n{'='*50}\n")
        log_file.flush()

        # Replace python/python3 with sys.executable for correct path on Render
        actual_cmd = cmd
        if actual_cmd.startswith("python3 "):
            actual_cmd = f"{sys.executable} {actual_cmd[8:]}"
        elif actual_cmd.startswith("python "):
            actual_cmd = f"{sys.executable} {actual_cmd[7:]}"

        # Use shell=True for reliable command execution
        proc = subprocess.Popen(
            actual_cmd,
            shell=True,
            cwd=str(project_path),
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.DEVNULL,
            preexec_fn=os.setsid if os.name != "nt" else None,
            env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": str(project_path)},
        )
        running_processes[project_id] = {
            "proc": proc,
            "started_at": datetime.utcnow(),
            "pid": proc.pid,
            "user_id": user_id,
            "name": project_name,
        }
        logger.info("Started project %s with PID %d, cmd: %s", project_name, proc.pid, actual_cmd)
        return proc
    except Exception as exc:
        logger.error("Failed to start project %s: %s", project_name, exc, exc_info=True)
        return None


def stop_project_process(project_id: str) -> bool:
    """Stop a running project process. Returns True if stopped."""
    entry = running_processes.pop(project_id, None)
    if not entry:
        return False
    proc = entry["proc"]
    try:
        if os.name != "nt":
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        else:
            proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    return True


def read_logs(user_id: int, project_name: str, lines: int = MAX_LOG_LINES) -> str:
    log_path = get_log_path(user_id, project_name)
    if not log_path.exists():
        return "(no logs yet)"
    try:
        with open(str(log_path), "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        return "".join(all_lines[-lines:]).strip() or "(log is empty)"
    except Exception as exc:
        return f"(error reading log: {exc})"


def _is_process_alive(project_id: str) -> bool:
    entry = running_processes.get(project_id)
    if not entry:
        return False
    return entry["proc"].poll() is None


def _compute_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(str(path), "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _relative_path(base: Path, file: Path) -> str:
    try:
        return str(file.relative_to(base))
    except ValueError:
        return file.name


async def backup_single_file(user_id: int, project_id: str, project_name: str, rel_path: str) -> bool:
    """Back up a single file to GridFS. Returns True on success."""
    project_path = get_project_path(user_id, project_name)
    abs_path = project_path / rel_path
    if not abs_path.is_file():
        return False
    try:
        md5 = _compute_md5(abs_path)
        existing = await backup_meta_col.find_one(
            {"project_id": project_id, "rel_path": rel_path, "md5": md5}
        )
        if existing:
            return True  # unchanged
        file_data = abs_path.read_bytes()
        filename_meta = f"{project_id}/{rel_path}"
        # Delete old GridFS entry for this file
        async for old in fs_bucket.find({"filename": filename_meta}):
            await fs_bucket.delete(old._id)
        grid_id = await fs_bucket.upload_from_stream(
            filename_meta,
            io.BytesIO(file_data),
            metadata={"project_id": project_id, "user_id": user_id, "rel_path": rel_path},
        )
        await backup_meta_col.update_one(
            {"project_id": project_id, "rel_path": rel_path},
            {"$set": {
                "project_id":   project_id,
                "user_id":      user_id,
                "project_name": project_name,
                "rel_path":     rel_path,
                "md5":          md5,
                "grid_id":      grid_id,
                "size":         len(file_data),
                "backed_up_at": datetime.utcnow(),
            }},
            upsert=True,
        )
        return True
    except Exception as exc:
        logger.error("Backup failed for %s/%s: %s", project_name, rel_path, exc)
        return False


async def backup_project_files(user_id: int, project_id: str, project_name: str) -> dict:
    """Back up all files in a project. Returns summary dict."""
    project_path = get_project_path(user_id, project_name)
    if not project_path.exists():
        return {"ok": False, "error": "Project path not found"}
    results = {"ok": True, "backed_up": 0, "skipped": 0, "failed": 0, "total": 0}
    for file_path in project_path.rglob("*"):
        if not file_path.is_file():
            continue
        rel = _relative_path(project_path, file_path)
        if any(p in rel for p in ("venv/", "__pycache__/", ".git/", ".pyc")):
            results["skipped"] += 1
            continue
        results["total"] += 1
        success = await backup_single_file(user_id, project_id, project_name, rel)
        if success:
            results["backed_up"] += 1
        else:
            results["failed"] += 1
    return results


async def auto_backup_loop(interval_seconds: int = 3600) -> None:
    """Background coroutine: periodically back up all projects."""
    await asyncio.sleep(60)  # initial delay
    while True:
        try:
            async for proj in projects_col.find({}):
                try:
                    await backup_project_files(proj["user_id"], str(proj["_id"]), proj["name"])
                except Exception as exc:
                    logger.error("Auto-backup error for %s: %s", proj.get("name"), exc)
        except Exception as exc:
            logger.error("Auto-backup loop error: %s", exc)
        await asyncio.sleep(interval_seconds)


async def restore_project(project_id: str) -> bool:
    """Restore all GridFS files for a project to disk."""
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        return False
    project_path = get_project_path(proj["user_id"], proj["name"])
    project_path.mkdir(parents=True, exist_ok=True)
    try:
        async for meta in backup_meta_col.find({"project_id": project_id}):
            rel   = meta["rel_path"]
            gid   = meta["grid_id"]
            dest  = project_path / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            stream = await fs_bucket.open_download_stream(gid)
            data   = await stream.read()
            dest.write_bytes(data)
        return True
    except Exception as exc:
        logger.error("Restore error for %s: %s", project_id, exc)
        return False


async def restore_all_projects() -> int:
    """Restore every project from GridFS on startup. Returns count."""
    count = 0
    async for proj in projects_col.find({}):
        pid = str(proj["_id"])
        ok  = await restore_project(pid)
        if ok:
            count += 1
    return count


# FIX #13: Use "run_command" field consistently
async def auto_restart_running_projects() -> int:
    """Auto-restart projects that were marked running before shutdown."""
    count = 0
    async for proj in projects_col.find({"status": "running", "admin_stopped": {"$ne": True}}):
        try:
            proc = start_project_process(
                proj["user_id"],
                str(proj["_id"]),
                proj["name"],
                proj.get("run_command", "python main.py"),
            )
            if proc:
                count += 1
        except Exception as exc:
            logger.error("Auto-restart failed for %s: %s", proj.get("name"), exc)
    return count


async def delete_project_backup(project_id: str) -> None:
    """Remove all GridFS backups for a project."""
    async for meta in backup_meta_col.find({"project_id": project_id}):
        try:
            await fs_bucket.delete(meta["grid_id"])
        except Exception:
            pass
    await backup_meta_col.delete_many({"project_id": project_id})


async def get_backup_stats() -> dict:
    """Return aggregate backup statistics."""
    total_files = await backup_meta_col.count_documents({})
    pipeline = [{"$group": {"_id": None, "total_size": {"$sum": "$size"}}}]
    result = await backup_meta_col.aggregate(pipeline).to_list(1)
    total_size = result[0]["total_size"] if result else 0
    last_meta = await backup_meta_col.find_one({}, sort=[("backed_up_at", -1)])
    last_backup = last_meta["backed_up_at"].strftime("%Y-%m-%d %H:%M") if last_meta and last_meta.get("backed_up_at") else "Never"
    return {
        "total_files":   total_files,
        "total_size":    bytes_human(total_size),
        "last_backup":   last_backup,
    }


async def process_watcher() -> None:
    """Background coroutine: detect crashed projects and update DB status."""
    while True:
        await asyncio.sleep(30)
        dead = []
        for pid, entry in list(running_processes.items()):
            if entry["proc"].poll() is not None:
                dead.append(pid)
        for pid in dead:
            running_processes.pop(pid, None)
            try:
                await projects_col.update_one(
                    {"_id": ObjectId(pid)},
                    {"$set": {"status": "stopped"}},
                )
            except Exception:
                pass

# ═══ KEYBOARDS ══════════════════════════════════════════════════════════════

# FIX #1 & #2: Corrected main menu keyboard layout
def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆕 New Project",  callback_data="new_project_start"),
         InlineKeyboardButton("📁 My Projects",  callback_data="my_projects")],
        [InlineKeyboardButton("💎 Premium",      callback_data="premium"),
         InlineKeyboardButton("📊 Bot Status",   callback_data="bot_status")],
    ])


def back_to_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])


def project_dashboard_keyboard(project_id: str, is_running: bool) -> InlineKeyboardMarkup:
    run_btn = (
        InlineKeyboardButton("⏹ Stop",    callback_data=f"stop_project:{project_id}")
        if is_running else
        InlineKeyboardButton("▶️ Run",     callback_data=f"run_project:{project_id}")
    )
    return InlineKeyboardMarkup([
        [run_btn,
         InlineKeyboardButton("🔄 Restart", callback_data=f"restart_project:{project_id}")],
        [InlineKeyboardButton("📜 Logs",    callback_data=f"logs_project:{project_id}"),
         InlineKeyboardButton("🔃 Refresh", callback_data=f"refresh_project:{project_id}")],
        [InlineKeyboardButton("📂 File Mgr", callback_data=f"fm_project:{project_id}"),
         InlineKeyboardButton("✏️ Edit Cmd", callback_data=f"editcmd_start:{project_id}")],
        [InlineKeyboardButton("🗑 Delete",   callback_data=f"delete_confirm:{project_id}"),
         InlineKeyboardButton("🏠 Main",    callback_data="main_menu")],
    ])


# FIX #9: Updated admin keyboard with correct button labels and layout
def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 All Users",      callback_data="admin_users:0"),
         InlineKeyboardButton("🟢 Running Scripts", callback_data="admin_running")],
        [InlineKeyboardButton("💎 Give Premium",   callback_data="admin_give_prem_start"),
         InlineKeyboardButton("❌ Remove Premium",  callback_data="admin_rem_prem_start")],
        [InlineKeyboardButton("⏰ Temp Premium",   callback_data="admin_temp_prem_start"),
         InlineKeyboardButton("🚫 Ban User",        callback_data="admin_ban_start")],
        [InlineKeyboardButton("✅ Unban User",      callback_data="admin_unban_start"),
         InlineKeyboardButton("📢 Broadcast",       callback_data="admin_broadcast_menu")],
        [InlineKeyboardButton("💾 Force Backup",   callback_data="admin_force_backup"),
         InlineKeyboardButton("⬅️ Back",            callback_data="main_menu")],
    ])

# ═══ BOT COMMAND HANDLERS ══════════════════════════════════════════════════

# FIX #1: /start welcome message with exact format
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    doc  = await get_or_create_user(user.id, user.username or "", user.full_name or "")
    if await is_user_banned(user.id):
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return
    await check_premium_expiry(user.id)
    doc   = await users_col.find_one({"user_id": user.id})
    plan  = doc.get("plan", "free") if doc else "free"
    limit = PLAN_LIMITS.get(plan, 1)
    count = await project_count(user.id)
    plan_label = get_plan_label(plan)
    text = (
        f"🌟 Welcome to God Madara Hosting Bot!\n\n"
        f"👋 Hello {user.first_name}!\n\n"
        f"🚀 What I can do:\n"
        f"• Host Python projects 24/7\n"
        f"• Web File Manager — Edit files in browser\n"
        f"• Auto-install requirements.txt\n"
        f"• Real-time logs & monitoring\n"
        f"• Free: 1 project | Premium: 10 projects\n\n"
        f"📊 Your Status:\n"
        f"👤 ID: {user.id}\n"
        f"💎 Plan: {plan_label}\n"
        f"📁 Projects: {count}/{limit}\n\n"
        f"Choose an option below:"
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


# FIX #2: main_menu callback with same format as /start
async def main_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    await check_premium_expiry(user.id)
    doc   = await users_col.find_one({"user_id": user.id})
    plan  = doc.get("plan", "free") if doc else "free"
    limit = PLAN_LIMITS.get(plan, 1)
    count = await project_count(user.id)
    plan_label = get_plan_label(plan)
    text = (
        f"🌟 Welcome to God Madara Hosting Bot!\n\n"
        f"👋 Hello {user.first_name}!\n\n"
        f"🚀 What I can do:\n"
        f"• Host Python projects 24/7\n"
        f"• Web File Manager — Edit files in browser\n"
        f"• Auto-install requirements.txt\n"
        f"• Real-time logs & monitoring\n"
        f"• Free: 1 project | Premium: 10 projects\n\n"
        f"📊 Your Status:\n"
        f"👤 ID: {user.id}\n"
        f"💎 Plan: {plan_label}\n"
        f"📁 Projects: {count}/{limit}\n\n"
        f"Choose an option below:"
    )
    await query.edit_message_text(text, reply_markup=main_menu_keyboard())


# FIX #8: Bot status with exact required format
async def bot_status_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    t0    = time.monotonic()
    stats = get_system_stats()
    backup_stats  = await get_backup_stats()
    total_users   = await users_col.count_documents({})
    premium_users = await users_col.count_documents({"plan": "premium"})
    total_projects   = await projects_col.count_documents({})
    running_count    = len(running_processes)
    ping_ms = round((time.monotonic() - t0) * 1000)
    text = (
        f"📊 Bot Dashboard\n\n"
        f"👥 Total Users: {total_users}\n"
        f"💎 Premium Users: {premium_users}\n"
        f"📁 Total Projects: {total_projects}\n"
        f"🟢 Running Projects: {running_count}\n"
        f"🗄️ Database: MongoDB ✅\n"
        f"🐍 Python: {stats['python']}\n\n"
        f"💻 System:\n"
        f"├ CPU: {stats['cpu_pct']}%\n"
        f"├ RAM: {stats['mem_used']}/{stats['mem_total']} ({stats['mem_pct']}%)\n"
        f"└ Disk: {stats['disk_used']}/{stats['disk_total']} ({stats['disk_pct']}%)\n\n"
        f"🏓 Response Ping: {ping_ms}ms\n\n"
        f"💾 Backup:\n"
        f"├ Files: {backup_stats['total_files']}\n"
        f"├ Size: {backup_stats['total_size']}\n"
        f"└ Last: {backup_stats['last_backup']}"
    )
    await query.edit_message_text(text, reply_markup=back_to_main())


# FIX #7: Premium section with exact required format
async def premium_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_doc = await users_col.find_one({"user_id": update.effective_user.id})
    plan     = user_doc.get("plan", "free") if user_doc else "free"
    is_premium = plan == "premium"

    if is_premium:
        status_line = "✨ You are Premium! ✨"
        action_line = "Premium is active!"
    else:
        status_line = "🔒 You are on Free Plan"
        action_line = "Contact owner for Premium:"

    text = (
        f"💎 Premium Membership\n\n"
        f"{status_line}\n\n"
        f"Free Plan:\n"
        f"• 1 Project only\n"
        f"• File Manager (10 min session)\n\n"
        f"Premium Plan:\n"
        f"• ✅ Unlimited projects (up to 10)\n"
        f"• ✅ Priority support\n"
        f"• ✅ Extended file manager\n"
        f"• ✅ Advanced monitoring\n\n"
        f"{action_line}"
    )

    if is_premium:
        kb = back_to_main()
    else:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Contact Owner", url=f"tg://user?id={OWNER_ID}")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ])
    await query.edit_message_text(text, reply_markup=kb)


async def my_projects_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id  = update.effective_user.id
    projects = await projects_col.find({"user_id": user_id}).to_list(50)
    if not projects:
        text = "📁 *My Projects*\n\nYou have no projects yet.\nTap 🆕 New Project to create one."
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🆕 New Project", callback_data="new_project_start")],
            [InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu")],
        ])
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return
    buttons = []
    for p in projects:
        pid    = str(p["_id"])
        status = "▶️" if _is_process_alive(pid) else "⏹"
        buttons.append([InlineKeyboardButton(f"{status} {p['name']}", callback_data=f"project_dashboard:{pid}")])
    buttons.append([InlineKeyboardButton("🆕 New Project", callback_data="new_project_start")])
    buttons.append([InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu")])
    await query.edit_message_text(
        f"📁 *My Projects* ({len(projects)} total):",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


# FIX #6: Project dashboard with exact required format
async def project_dashboard_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    is_running   = _is_process_alive(project_id)
    entry        = running_processes.get(project_id, {})
    uptime       = uptime_str(entry["started_at"]) if is_running and "started_at" in entry else "N/A"
    pid_display  = str(entry.get("pid", "N/A")) if is_running else "N/A"
    status_icon  = "🟢 Running" if is_running else "🔴 Stopped"
    last_run     = proj.get("last_run")
    last_run_str = last_run.strftime("%Y-%m-%d %H:%M") if last_run else "Never"
    exit_code    = proj.get("exit_code")
    exit_str     = str(exit_code) if exit_code is not None else "None"
    created_at   = proj.get("created_at")
    created_str  = created_at.strftime("%Y-%m-%d %H:%M") if created_at else "N/A"
    run_cmd      = proj.get("run_command", "python main.py")
    text = (
        f"📊 Project: {proj['name']}\n\n"
        f"🔹 Status: {status_icon}\n"
        f"🔹 PID: {pid_display}\n"
        f"🔹 Uptime: {uptime}\n"
        f"🔹 Last Run: {last_run_str}\n"
        f"🔹 Exit Code: {exit_str}\n"
        f"🔹 Run Command: {run_cmd}\n"
        f"📅 Created: {created_str}"
    )
    await query.edit_message_text(
        text,
        reply_markup=project_dashboard_keyboard(project_id, is_running),
    )


# FIX #13: Use "run_command" field
async def run_project_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    # FIX #12: Admin-stopped message with contact owner button
    if proj.get("admin_stopped"):
        await query.edit_message_text(
            "⚠️ Your project was stopped by admin. Contact owner.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📩 Contact Owner", url=f"tg://user?id={OWNER_ID}")],
                [InlineKeyboardButton("⬅️ Back", callback_data=f"project_dashboard:{project_id}")],
            ]),
        )
        return
    if _is_process_alive(project_id):
        await query.answer("⚠️ Already running!", show_alert=True)
        return
    proc = start_project_process(proj["user_id"], project_id, proj["name"], proj.get("run_command", "python main.py"))
    if proc:
        await projects_col.update_one(
            {"_id": ObjectId(project_id)},
            {"$set": {"status": "running", "admin_stopped": False, "last_run": datetime.utcnow()}},
        )
        await query.edit_message_text(
            f"▶️ *{proj['name']}* started! PID: `{proc.pid}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=project_dashboard_keyboard(project_id, True),
        )
    else:
        await query.edit_message_text("❌ Failed to start project. Check logs.", reply_markup=back_to_main())


async def stop_project_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    stopped = stop_project_process(project_id)
    await projects_col.update_one({"_id": ObjectId(project_id)}, {"$set": {"status": "stopped"}})
    msg = f"⏹ *{proj['name']}* stopped." if stopped else f"⚠️ *{proj['name']}* was not running."
    await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=project_dashboard_keyboard(project_id, False))


async def restart_project_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    # FIX #12: Admin-stopped message with contact owner button
    if proj.get("admin_stopped"):
        await query.edit_message_text(
            "⚠️ Your project was stopped by admin. Contact owner.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📩 Contact Owner", url=f"tg://user?id={OWNER_ID}")],
                [InlineKeyboardButton("⬅️ Back", callback_data=f"project_dashboard:{project_id}")],
            ]),
        )
        return
    stop_project_process(project_id)
    await asyncio.sleep(1)
    proc = start_project_process(proj["user_id"], project_id, proj["name"], proj.get("run_command", "python main.py"))
    if proc:
        await projects_col.update_one(
            {"_id": ObjectId(project_id)},
            {"$set": {"status": "running", "admin_stopped": False, "last_run": datetime.utcnow()}},
        )
        await query.edit_message_text(
            f"🔄 *{proj['name']}* restarted! PID: `{proc.pid}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=project_dashboard_keyboard(project_id, True),
        )
    else:
        await query.edit_message_text("❌ Failed to restart project.", reply_markup=back_to_main())


async def logs_project_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    logs = read_logs(proj["user_id"], proj["name"])
    if len(logs) > 3500:
        logs = "...(truncated)\n" + logs[-3500:]
    text = f"📜 *Logs — {proj['name']}*\n\n```\n{logs}\n```"
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔃 Refresh Logs", callback_data=f"logs_project:{project_id}"),
        InlineKeyboardButton("◀️ Back",         callback_data=f"project_dashboard:{project_id}"),
    ]])
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


async def refresh_project_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("🔃 Refreshed")
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    entry = running_processes.get(project_id)
    if entry and entry["proc"].poll() is not None:
        running_processes.pop(project_id, None)
        await projects_col.update_one({"_id": ObjectId(project_id)}, {"$set": {"status": "stopped"}})
    is_running   = _is_process_alive(project_id)
    status_icon  = "🟢 Running" if is_running else "🔴 Stopped"
    entry        = running_processes.get(project_id, {})
    uptime       = uptime_str(entry["started_at"]) if is_running and "started_at" in entry else "N/A"
    pid_display  = str(entry.get("pid", "N/A")) if is_running else "N/A"
    last_run     = proj.get("last_run")
    last_run_str = last_run.strftime("%Y-%m-%d %H:%M") if last_run else "Never"
    exit_code    = proj.get("exit_code")
    exit_str     = str(exit_code) if exit_code is not None else "None"
    created_at   = proj.get("created_at")
    created_str  = created_at.strftime("%Y-%m-%d %H:%M") if created_at else "N/A"
    run_cmd      = proj.get("run_command", "python main.py")
    text = (
        f"📊 Project: {proj['name']}\n\n"
        f"🔹 Status: {status_icon}\n"
        f"🔹 PID: {pid_display}\n"
        f"🔹 Uptime: {uptime}\n"
        f"🔹 Last Run: {last_run_str}\n"
        f"🔹 Exit Code: {exit_str}\n"
        f"🔹 Run Command: {run_cmd}\n"
        f"📅 Created: {created_str}"
    )
    await query.edit_message_text(
        text,
        reply_markup=project_dashboard_keyboard(project_id, is_running),
    )


# FIX #3: fm_project_cb with clickable URL button + exact text format
async def fm_project_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    # Write meta file so file manager can find project path
    meta_file = PROJECTS_DIR / f"meta_{project_id}.txt"
    PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
    project_path = get_project_path(proj["user_id"], proj["name"])
    meta_file.write_text(str(project_path))
    # Trigger backup
    asyncio.create_task(backup_project_files(proj["user_id"], project_id, proj["name"]))
    token  = generate_fm_token(project_id, proj["user_id"], ttl_minutes=60)
    fm_url = f"{FILE_MANAGER_URL}/fm/{token}"
    text = (
        f"📂 File Manager — {proj['name']}\n\n"
        f"Your temporary file manager link (valid 1 hour):\n\n"
        f"⚠️ Do not share this link."
    )
    # FIX #3: Two buttons — clickable URL button + Back button
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 Open File Manager", url=fm_url)],
            [InlineKeyboardButton("⬅️ Back", callback_data=f"project_dashboard:{project_id}")],
        ]),
    )


async def delete_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    await query.edit_message_text(
        f"🗑 Are you sure you want to delete *{proj['name']}*?\n\n"
        f"This will remove all files and backups permanently.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, Delete", callback_data=f"delete_yes:{project_id}"),
             InlineKeyboardButton("❌ Cancel",      callback_data=f"project_dashboard:{project_id}")],
        ]),
    )


async def delete_yes_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return
    stop_project_process(project_id)
    project_path = get_project_path(proj["user_id"], proj["name"])
    if project_path.exists():
        shutil.rmtree(str(project_path), ignore_errors=True)
    await delete_project_backup(project_id)
    meta_file = PROJECTS_DIR / f"meta_{project_id}.txt"
    meta_file.unlink(missing_ok=True)
    await projects_col.delete_one({"_id": ObjectId(project_id)})
    await query.edit_message_text(
        f"🗑 Project *{proj['name']}* deleted successfully.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_to_main(),
    )

# ═══ NEW PROJECT CONVERSATION ══════════════════════════════════════════════

async def new_project_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id  = update.effective_user.id
    user_doc = await users_col.find_one({"user_id": user_id})
    plan     = user_doc.get("plan", "free") if user_doc else "free"
    limit    = PLAN_LIMITS.get(plan, 1)
    count    = await project_count(user_id)
    if count >= limit:
        await query.edit_message_text(
            f"❌ You have reached your project limit ({limit}) for the {get_plan_label(plan)} plan.\n"
            f"Upgrade to create more projects.",
            reply_markup=back_to_main(),
        )
        return ConversationHandler.END
    await query.edit_message_text(
        "📁 *New Project*\n\nEnter a name for your project:\n_(Alphanumeric, underscores, hyphens, 2–32 chars)_",
        parse_mode=ParseMode.MARKDOWN,
    )
    return NP_NAME


async def np_got_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = update.message.text.strip()
    if not is_valid_project_name(name):
        await update.message.reply_text(
            "❌ Invalid name. Use 2–32 alphanumeric characters, underscores, or hyphens."
        )
        return NP_NAME
    user_id  = update.effective_user.id
    existing = await projects_col.find_one({"user_id": user_id, "name": name})
    if existing:
        await update.message.reply_text(f"❌ You already have a project named *{name}*. Choose another.", parse_mode=ParseMode.MARKDOWN)
        return NP_NAME
    context.user_data["np_name"]  = name
    context.user_data["np_files"] = []
    await update.message.reply_text(
        f"✅ Name: *{name}*\n\nNow upload your project files.\n"
        f"You can send:\n• A ZIP file (recommended)\n• A single `.py` file\n• Multiple files one by one\n\n"
        f"When finished, tap the button below or type /done:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Done Uploading", callback_data="np_done_uploading")],
        ]),
    )
    return NP_FILE


# FIX #5: After each file received, show "✅ Done Uploading" inline button
async def np_got_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    name    = context.user_data.get("np_name", "")
    if not name:
        await update.message.reply_text("❌ Session expired. Please start over.")
        return ConversationHandler.END
    project_path = get_project_path(user_id, name)
    project_path.mkdir(parents=True, exist_ok=True)
    doc = update.message.document
    if not doc:
        if update.message.text and update.message.text.strip() == "/done":
            return await np_done_upload(update, context)
        await update.message.reply_text(
            "Please send a file or tap the button below:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Done Uploading", callback_data="np_done_uploading")],
            ]),
        )
        return NP_FILE
    file_obj = await doc.get_file()
    dest = project_path / doc.file_name
    await file_obj.download_to_drive(str(dest))
    context.user_data.setdefault("np_files", []).append(doc.file_name)
    extra = "\n\n🗜 ZIP detected. Tap Done when ready to extract." if doc.file_name.lower().endswith(".zip") else ""
    # FIX #5: Show "✅ Done Uploading" inline button after each received file
    await update.message.reply_text(
        f"📥 Received: `{doc.file_name}`\n\nSend more files or tap Done when finished.{extra}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Done Uploading", callback_data="np_done_uploading")],
        ]),
    )
    return NP_FILE


# FIX #5: Callback handler for "✅ Done Uploading" button
async def np_done_uploading_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    # Delegate to the done logic by building a fake-compatible call
    return await _np_finalize(update, context, reply_func=query.edit_message_text)


async def np_done_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Called when user types /done."""
    return await _np_finalize(update, context, reply_func=update.message.reply_text)


async def _np_finalize(update: Update, context: ContextTypes.DEFAULT_TYPE, reply_func) -> int:
    """Shared finalization logic for Done Uploading."""
    user_id = update.effective_user.id
    name = context.user_data.get("np_name", "")
    if not name:
        await reply_func("❌ Session expired. Please start over.")
        return ConversationHandler.END
    project_path = get_project_path(user_id, name)
    files = context.user_data.get("np_files", [])
    if not files:
        await reply_func("❌ No files received. Please upload at least one file.")
        return NP_FILE

    # Extract ZIP files
    for fname in files:
        fpath = project_path / fname
        if fname.lower().endswith(".zip") and fpath.exists():
            try:
                with zipfile.ZipFile(str(fpath), "r") as zf:
                    zf.extractall(str(project_path))
                fpath.unlink(missing_ok=True)
                await reply_func(f"📦 Extracted: {fname}")
            except Exception as exc:
                await reply_func(f"⚠️ ZIP extraction failed: {exc}")

    # List all files in project
    all_files = [f.name for f in project_path.iterdir() if f.is_file()]
    await reply_func(f"📁 Project files: {', '.join(all_files[:20])}")

    # Install requirements.txt if exists
    req_file = project_path / "requirements.txt"
    if req_file.exists():
        await reply_func("📦 Found requirements.txt — Installing packages...\nThis may take a minute...")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", str(req_file), "--no-warn-script-location"],
                cwd=str(project_path),
                capture_output=True,
                text=True,
                timeout=300,  # 5 minutes timeout
            )
            if result.returncode == 0:
                # Count installed packages
                installed = [l for l in result.stdout.splitlines() if "Successfully installed" in l]
                if installed:
                    await reply_func(f"✅ {installed[0]}")
                else:
                    await reply_func("✅ All requirements already satisfied!")
            else:
                error_msg = result.stderr[-800:] if result.stderr else "Unknown error"
                await reply_func(
                    f"⚠️ Some packages may have failed:\n```\n{error_msg}\n```\n\nContinuing anyway...",
                    parse_mode=ParseMode.MARKDOWN,
                )
        except subprocess.TimeoutExpired:
            await reply_func("⚠️ Installation timed out (5 min). Some packages may not be installed.")
        except Exception as exc:
            await reply_func(f"⚠️ pip install error: {exc}")
    else:
        await reply_func("ℹ️ No requirements.txt found. Skipping package installation.")

    # Create project in DB with run_command field
    doc = {
        "user_id": user_id,
        "name": name,
        "run_command": "python main.py",
        "status": "stopped",
        "admin_stopped": False,
        "last_run": None,
        "exit_code": None,
        "created_at": datetime.utcnow(),
    }
    result = await projects_col.insert_one(doc)
    project_id = str(result.inserted_id)

    # Backup project files
    asyncio.create_task(backup_project_files(user_id, project_id, name))

    await reply_func(
        f"✅ Project *{name}* created!\n\n"
        f"Now enter the run command (e.g. `python main.py` or `python bot.py`):",
        parse_mode=ParseMode.MARKDOWN,
    )
    context.user_data["np_project_id"] = project_id
    return NP_CMD


# FIX #13: Use "run_command" field
async def np_got_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cmd        = update.message.text.strip()
    project_id = context.user_data.get("np_project_id")
    if not project_id:
        await update.message.reply_text("❌ Session lost. Please start over.")
        return ConversationHandler.END
    await projects_col.update_one({"_id": ObjectId(project_id)}, {"$set": {"run_command": cmd}})
    await update.message.reply_text(
        f"✅ Run command set: `{cmd}`\n\nYour project is ready!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📁 Go to Project", callback_data=f"project_dashboard:{project_id}")],
            [InlineKeyboardButton("🏠 Main Menu",     callback_data="main_menu")],
        ]),
    )
    return ConversationHandler.END


async def cancel_conv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("❌ Cancelled.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END

# ═══ EDIT COMMAND CONVERSATION ═════════════════════════════════════════════

async def editcmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main())
        return ConversationHandler.END
    context.user_data["editcmd_project_id"] = project_id
    await query.edit_message_text(
        f"✏️ *Edit Run Command — {proj['name']}*\n\nCurrent: `{proj.get('run_command', 'N/A')}`\n\nEnter new run command:",
        parse_mode=ParseMode.MARKDOWN,
    )
    return EDIT_CMD


# FIX #13: Use "run_command" field
async def editcmd_got(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cmd        = update.message.text.strip()
    project_id = context.user_data.get("editcmd_project_id")
    if not project_id:
        await update.message.reply_text("❌ Session lost.")
        return ConversationHandler.END
    await projects_col.update_one({"_id": ObjectId(project_id)}, {"$set": {"run_command": cmd}})
    await update.message.reply_text(
        f"✅ Run command updated: `{cmd}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📁 Go to Project", callback_data=f"project_dashboard:{project_id}")],
        ]),
    )
    return ConversationHandler.END

# ═══ ADMIN COMMAND ═════════════════════════════════════════════════════════

# FIX #9: Admin panel with exact required format
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("🚫 Owner only.")
        return
    text = (
        "🔐 Admin Panel\n\n"
        "Welcome, Owner!"
    )
    await update.message.reply_text(text, reply_markup=admin_keyboard())


async def admin_panel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        await query.answer("🚫 Owner only.", show_alert=True)
        return
    text = (
        "🔐 Admin Panel\n\n"
        "Welcome, Owner!"
    )
    await query.edit_message_text(text, reply_markup=admin_keyboard())


async def admin_users_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    page     = int(query.data.split(":", 1)[1])
    per_page = 10
    skip     = page * per_page
    users    = await users_col.find({}).skip(skip).limit(per_page).to_list(per_page)
    total    = await users_col.count_documents({})
    lines    = [f"👥 *Users* (page {page + 1}, total {total})\n"]
    buttons  = []
    for u in users:
        plan   = get_plan_label(u.get("plan", "free"))
        banned = " 🚫" if u.get("banned") else ""
        name   = u.get("full_name") or u.get("username") or str(u.get("user_id", u.get("_id")))
        uid    = u.get("user_id", u.get("_id"))
        lines.append(f"• [{name}](tg://user?id={uid}) — {plan}{banned}")
        buttons.append([InlineKeyboardButton(f"{name}", callback_data=f"admin_user_detail:{uid}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"admin_users:{page - 1}"))
    if skip + per_page < total:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"admin_users:{page + 1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("◀️ Admin Panel", callback_data="admin_panel")])
    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def admin_user_detail_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    uid  = int(query.data.split(":", 1)[1])
    user = await users_col.find_one({"user_id": uid})
    if not user:
        await query.answer("User not found.", show_alert=True)
        return
    pcount  = await project_count(uid)
    expiry  = user.get("premium_expiry")
    exp_str = expiry.strftime("%Y-%m-%d") if expiry else "N/A"
    text = (
        f"👤 *User Detail*\n\n"
        f"ID: `{uid}`\n"
        f"Name: {user.get('full_name', 'N/A')}\n"
        f"Username: @{user.get('username', 'N/A')}\n"
        f"Plan: {get_plan_label(user.get('plan', 'free'))}\n"
        f"Premium Expiry: `{exp_str}`\n"
        f"Banned: `{user.get('banned', False)}`\n"
        f"Projects: `{pcount}`\n"
        f"Joined: `{user.get('joined_at', 'N/A')}`"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="admin_users:0")]]),
    )


# FIX #10: Running scripts with required per-project format and buttons
async def admin_running_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    if not running_processes:
        await query.edit_message_text(
            "🟢 *Running Scripts*\n\nNo projects currently running.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Admin", callback_data="admin_panel")]]),
        )
        return
    lines   = ["🟢 *Running Scripts*\n"]
    buttons = []
    for pid, entry in running_processes.items():
        proj = await projects_col.find_one({"_id": ObjectId(pid)})
        if not proj:
            continue
        user = await users_col.find_one({"user_id": proj["user_id"]})
        uname    = user.get("username") or str(proj["user_id"]) if user else str(proj["user_id"])
        duration = uptime_str(entry["started_at"])
        pid_num  = entry.get("pid", "?")
        # FIX #10: Exact format per project
        lines.append(
            f"👤 Username: @{uname}\n"
            f"🆔 User ID: {proj['user_id']}\n"
            f"📊 Project PID: {pid_num}\n"
            f"⏱️ Running Time: {duration}\n"
            f"📜 Project: {proj['name']}\n"
        )
        # FIX #10: [▶️ Run] [⏹️ Stop] [📥 Download Script] buttons
        buttons.append([
            InlineKeyboardButton("▶️ Run",              callback_data=f"admin_run_proj:{pid}"),
            InlineKeyboardButton("⏹️ Stop",             callback_data=f"admin_stop_proj:{pid}"),
            InlineKeyboardButton("📥 Download Script",  callback_data=f"admin_dl_script:{pid}"),
        ])
    buttons.append([InlineKeyboardButton("◀️ Admin", callback_data="admin_panel")])
    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def admin_run_proj_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: run a stopped project."""
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.answer("Project not found.", show_alert=True)
        return
    if _is_process_alive(project_id):
        await query.answer("Already running!", show_alert=True)
        return
    proc = start_project_process(proj["user_id"], project_id, proj["name"], proj.get("run_command", "python main.py"))
    if proc:
        await projects_col.update_one(
            {"_id": ObjectId(project_id)},
            {"$set": {"status": "running", "admin_stopped": False, "last_run": datetime.utcnow()}},
        )
        await query.answer(f"✅ {proj['name']} started. PID: {proc.pid}", show_alert=True)
    else:
        await query.answer("❌ Failed to start project.", show_alert=True)
    # Refresh running panel
    query.data = "admin_running"
    await admin_running_cb(update, context)


async def admin_stop_proj_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    project_id = query.data.split(":", 1)[1]
    stop_project_process(project_id)
    await projects_col.update_one(
        {"_id": ObjectId(project_id)},
        {"$set": {"status": "stopped", "admin_stopped": True}},
    )
    await query.answer("✅ Project stopped and marked admin_stopped.", show_alert=True)
    query.data = "admin_running"
    await admin_running_cb(update, context)


async def admin_dl_script_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: download the main script file of a project."""
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    project_id = query.data.split(":", 1)[1]
    proj = await projects_col.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await query.answer("Project not found.", show_alert=True)
        return
    project_path = get_project_path(proj["user_id"], proj["name"])
    # Try to find main script
    run_cmd  = proj.get("run_command", "python main.py")
    parts    = shlex.split(run_cmd)
    script   = parts[1] if len(parts) > 1 else "main.py"
    script_path = project_path / script
    if not script_path.exists():
        # fallback: first .py file
        py_files = list(project_path.glob("*.py"))
        if py_files:
            script_path = py_files[0]
        else:
            await query.answer("No script file found.", show_alert=True)
            return
    try:
        await context.bot.send_document(
            chat_id=update.effective_user.id,
            document=script_path.open("rb"),
            filename=script_path.name,
            caption=f"📥 Script: {proj['name']} / {script_path.name}",
        )
    except Exception as exc:
        await query.answer(f"Failed: {exc}", show_alert=True)


async def admin_force_backup_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("💾 Running force backup…")
    if update.effective_user.id != OWNER_ID:
        return
    total  = 0
    backed = 0
    failed = 0
    async for proj in projects_col.find({}):
        total += 1
        result = await backup_project_files(proj["user_id"], str(proj["_id"]), proj["name"])
        if result.get("ok"):
            backed += 1
        else:
            failed += 1
    await query.edit_message_text(
        f"💾 *Force Backup Complete*\n\n"
        f"Total projects: `{total}`\n"
        f"Backed up: `{backed}`\n"
        f"Failed: `{failed}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Admin", callback_data="admin_panel")]]),
    )

# ═══ ADMIN PREMIUM CONVERSATIONS ═══════════════════════════════════════════

async def admin_give_prem_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text("💎 Enter user ID to give *premium* plan:", parse_mode=ParseMode.MARKDOWN)
    return GIVE_PREM_ID


async def admin_give_prem_got(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return GIVE_PREM_ID
    await users_col.update_one({"user_id": uid}, {"$set": {"plan": "premium", "premium_expiry": None}}, upsert=True)
    await update.message.reply_text(f"✅ User `{uid}` given *premium* plan.", parse_mode=ParseMode.MARKDOWN, reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_rem_prem_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text("🚫 Enter user ID to *remove premium* (downgrade to free):", parse_mode=ParseMode.MARKDOWN)
    return REM_PREM_ID


async def admin_rem_prem_got(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return REM_PREM_ID
    await users_col.update_one({"user_id": uid}, {"$set": {"plan": "free", "premium_expiry": None}})
    await update.message.reply_text(f"✅ User `{uid}` downgraded to free.", parse_mode=ParseMode.MARKDOWN, reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_temp_prem_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text("⏳ Enter user ID for *temporary premium*:", parse_mode=ParseMode.MARKDOWN)
    return TEMP_PREM_ID


async def admin_temp_prem_got_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return TEMP_PREM_ID
    context.user_data["temp_prem_uid"] = uid
    await update.message.reply_text("⏳ How many days of premium?")
    return TEMP_PREM_DAYS


async def admin_temp_prem_got_days(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        days = int(update.message.text.strip())
        if days <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Invalid number of days.")
        return TEMP_PREM_DAYS
    uid    = context.user_data["temp_prem_uid"]
    expiry = datetime.utcnow() + timedelta(days=days)
    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"plan": "premium", "premium_expiry": expiry}},
        upsert=True,
    )
    await update.message.reply_text(
        f"✅ User `{uid}` given premium for {days} days (expires `{expiry.strftime('%Y-%m-%d')}`).",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


async def admin_ban_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text("🔨 Enter user ID to *ban*:", parse_mode=ParseMode.MARKDOWN)
    return BAN_ID


async def admin_ban_got(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return BAN_ID
    await users_col.update_one({"user_id": uid}, {"$set": {"banned": True}}, upsert=True)
    await update.message.reply_text(f"✅ User `{uid}` banned.", parse_mode=ParseMode.MARKDOWN, reply_markup=admin_keyboard())
    return ConversationHandler.END


async def admin_unban_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text("✅ Enter user ID to *unban*:", parse_mode=ParseMode.MARKDOWN)
    return UNBAN_ID


async def admin_unban_got(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return UNBAN_ID
    await users_col.update_one({"user_id": uid}, {"$set": {"banned": False}})
    await update.message.reply_text(f"✅ User `{uid}` unbanned.", parse_mode=ParseMode.MARKDOWN, reply_markup=admin_keyboard())
    return ConversationHandler.END

# ═══ ADMIN BROADCAST CONVERSATIONS ═════════════════════════════════════════

async def admin_broadcast_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return
    await query.edit_message_text(
        "📢 *Broadcast*\n\nChoose broadcast type:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📣 Broadcast to All", callback_data="admin_bc_all_start")],
            [InlineKeyboardButton("📨 Message to User",  callback_data="admin_bc_user_start")],
            [InlineKeyboardButton("◀️ Admin Panel",      callback_data="admin_panel")],
        ]),
    )


async def admin_bc_all_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text("📣 Enter the message to broadcast to *all users*:", parse_mode=ParseMode.MARKDOWN)
    return BC_ALL_MSG


async def admin_bc_all_got(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    msg    = update.message.text
    bot    = context.bot
    sent   = 0
    failed = 0
    async for user in users_col.find({"banned": {"$ne": True}}):
        uid = user.get("user_id") or user.get("_id")
        try:
            await bot.send_message(uid, f"📢 *Broadcast*\n\n{msg}", parse_mode=ParseMode.MARKDOWN)
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(
        f"✅ Broadcast complete.\nSent: `{sent}` | Failed: `{failed}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=admin_keyboard(),
    )
    return ConversationHandler.END


async def admin_bc_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    await query.edit_message_text("📨 Enter the target user ID:", parse_mode=ParseMode.MARKDOWN)
    return BC_USER_ID


async def admin_bc_user_got_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return BC_USER_ID
    context.user_data["bc_user_uid"] = uid
    await update.message.reply_text(f"📨 Enter the message to send to user `{uid}`:", parse_mode=ParseMode.MARKDOWN)
    return BC_USER_MSG


async def admin_bc_user_got_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = context.user_data.get("bc_user_uid")
    msg = update.message.text
    try:
        await context.bot.send_message(uid, f"📨 *Message from Admin*\n\n{msg}", parse_mode=ParseMode.MARKDOWN)
        await update.message.reply_text(f"✅ Message sent to user `{uid}`.", parse_mode=ParseMode.MARKDOWN, reply_markup=admin_keyboard())
    except Exception as exc:
        await update.message.reply_text(f"❌ Failed: {exc}", reply_markup=admin_keyboard())
    return ConversationHandler.END

# ═══ POST INIT & APP BUILDER ════════════════════════════════════════════════

async def post_init(application: Application) -> None:
    """Called after bot initializes: set commands, restore, restart, launch loops."""
    await application.bot.set_my_commands([
        ("start",  "Main menu"),
        ("admin",  "Admin panel (owner only)"),
        ("cancel", "Cancel current operation"),
    ])
    logger.info("Restoring projects from backup…")
    restored = await restore_all_projects()
    logger.info("Restored %d projects.", restored)
    logger.info("Auto-restarting running projects…")
    restarted = await auto_restart_running_projects()
    logger.info("Restarted %d projects.", restarted)
    asyncio.create_task(auto_backup_loop())
    asyncio.create_task(process_watcher())
    logger.info("Background tasks started.")


def build_app() -> Application:
    """Build and configure the Telegram Application with all handlers."""
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # ── New Project conversation ──────────────────────────────────────────
    new_project_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(new_project_start, pattern="^new_project_start$")],
        states={
            NP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, np_got_name)],
            NP_FILE: [
                MessageHandler(filters.Document.ALL, np_got_file),
                MessageHandler(filters.Regex(r"^/done$"), np_done_upload),
                MessageHandler(filters.TEXT & ~filters.COMMAND, np_got_file),
                # FIX #5: Handle "✅ Done Uploading" inline button inside conversation
                CallbackQueryHandler(np_done_uploading_cb, pattern="^np_done_uploading$"),
            ],
            NP_CMD:  [MessageHandler(filters.TEXT & ~filters.COMMAND, np_got_cmd)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        allow_reentry=True,
    )

    # ── Edit Command conversation ─────────────────────────────────────────
    editcmd_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(editcmd_start, pattern=r"^editcmd_start:")],
        states={
            EDIT_CMD: [MessageHandler(filters.TEXT & ~filters.COMMAND, editcmd_got)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        allow_reentry=True,
    )

    # ── Give Premium conversation ─────────────────────────────────────────
    give_prem_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_give_prem_start, pattern="^admin_give_prem_start$")],
        states={GIVE_PREM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_give_prem_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        allow_reentry=True,
    )

    # ── Remove Premium conversation ───────────────────────────────────────
    rem_prem_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_rem_prem_start, pattern="^admin_rem_prem_start$")],
        states={REM_PREM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_rem_prem_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        allow_reentry=True,
    )

    # ── Temp Premium conversation ─────────────────────────────────────────
    temp_prem_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_temp_prem_start, pattern="^admin_temp_prem_start$")],
        states={
            TEMP_PREM_ID:   [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_prem_got_id)],
            TEMP_PREM_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_prem_got_days)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        allow_reentry=True,
    )

    # ── Ban conversation ──────────────────────────────────────────────────
    ban_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_ban_start, pattern="^admin_ban_start$")],
        states={BAN_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ban_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        allow_reentry=True,
    )

    # ── Unban conversation ────────────────────────────────────────────────
    unban_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_unban_start, pattern="^admin_unban_start$")],
        states={UNBAN_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_unban_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        allow_reentry=True,
    )

    # ── Broadcast All conversation ────────────────────────────────────────
    bc_all_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_bc_all_start, pattern="^admin_bc_all_start$")],
        states={BC_ALL_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_bc_all_got)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        allow_reentry=True,
    )

    # ── Broadcast User conversation ───────────────────────────────────────
    bc_user_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_bc_user_start, pattern="^admin_bc_user_start$")],
        states={
            BC_USER_ID:  [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_bc_user_got_id)],
            BC_USER_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_bc_user_got_msg)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        allow_reentry=True,
    )

    # ── Register all conversation handlers ───────────────────────────────
    for conv in (
        new_project_conv, editcmd_conv, give_prem_conv, rem_prem_conv,
        temp_prem_conv, ban_conv, unban_conv, bc_all_conv, bc_user_conv,
    ):
        app.add_handler(conv)

    # ── Register command handlers ─────────────────────────────────────────
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("admin",  admin_cmd))
    app.add_handler(CommandHandler("cancel", cancel_conv))

    # ── Register callback query handlers ─────────────────────────────────
    app.add_handler(CallbackQueryHandler(main_menu_cb,           pattern="^main_menu$"))
    app.add_handler(CallbackQueryHandler(bot_status_cb,          pattern="^bot_status$"))
    app.add_handler(CallbackQueryHandler(premium_cb,             pattern="^premium$"))
    app.add_handler(CallbackQueryHandler(my_projects_cb,         pattern="^my_projects$"))
    app.add_handler(CallbackQueryHandler(project_dashboard_cb,   pattern=r"^project_dashboard:"))
    app.add_handler(CallbackQueryHandler(run_project_cb,         pattern=r"^run_project:"))
    app.add_handler(CallbackQueryHandler(stop_project_cb,        pattern=r"^stop_project:"))
    app.add_handler(CallbackQueryHandler(restart_project_cb,     pattern=r"^restart_project:"))
    app.add_handler(CallbackQueryHandler(logs_project_cb,        pattern=r"^logs_project:"))
    app.add_handler(CallbackQueryHandler(refresh_project_cb,     pattern=r"^refresh_project:"))
    app.add_handler(CallbackQueryHandler(fm_project_cb,          pattern=r"^fm_project:"))
    app.add_handler(CallbackQueryHandler(delete_confirm_cb,      pattern=r"^delete_confirm:"))
    app.add_handler(CallbackQueryHandler(delete_yes_cb,          pattern=r"^delete_yes:"))
    app.add_handler(CallbackQueryHandler(admin_panel_cb,         pattern="^admin_panel$"))
    app.add_handler(CallbackQueryHandler(admin_users_cb,         pattern=r"^admin_users:"))
    app.add_handler(CallbackQueryHandler(admin_user_detail_cb,   pattern=r"^admin_user_detail:"))
    app.add_handler(CallbackQueryHandler(admin_running_cb,       pattern="^admin_running$"))
    app.add_handler(CallbackQueryHandler(admin_run_proj_cb,      pattern=r"^admin_run_proj:"))
    app.add_handler(CallbackQueryHandler(admin_stop_proj_cb,     pattern=r"^admin_stop_proj:"))
    app.add_handler(CallbackQueryHandler(admin_dl_script_cb,     pattern=r"^admin_dl_script:"))
    app.add_handler(CallbackQueryHandler(admin_force_backup_cb,  pattern="^admin_force_backup$"))
    app.add_handler(CallbackQueryHandler(admin_broadcast_menu_cb,pattern="^admin_broadcast_menu$"))

    return app

# ═══ FLASK FILE MANAGER (continued from Part 1) ═══

# ──────────────────────────────────────────────
# FILE MANAGER — Flask app
# ──────────────────────────────────────────────

import os
import json
import shutil
from pathlib import Path
from datetime import datetime, timezone

from flask import (
    Flask, request, redirect, url_for, flash,
    render_template_string, send_file, jsonify, abort,
)

# ── Templates ──────────────────────────────────────────────────────────────────

BROWSER_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>File Manager — {{ project_name }}</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', system-ui, sans-serif; font-size: 14px; min-height: 100vh; }
  a { color: #58a6ff; text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* Header */
  .header { background: #161b22; border-bottom: 1px solid #30363d; padding: 12px 24px; display: flex; align-items: center; justify-content: space-between; }
  .header-title { font-size: 16px; font-weight: 600; color: #f0f6fc; }
  .header-expiry { font-size: 12px; color: #8b949e; }

  /* Container */
  .container { max-width: 1100px; margin: 0 auto; padding: 24px 16px; }

  /* Breadcrumb */
  .breadcrumb { display: flex; align-items: center; flex-wrap: wrap; gap: 4px; margin-bottom: 16px; font-size: 13px; color: #8b949e; }
  .breadcrumb a { color: #58a6ff; }
  .breadcrumb .sep { color: #484f58; }

  /* Toolbar */
  .toolbar { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
  .btn { display: inline-flex; align-items: center; gap: 6px; padding: 6px 14px; border-radius: 6px; border: 1px solid #30363d; background: #21262d; color: #c9d1d9; cursor: pointer; font-size: 13px; transition: background 0.15s; }
  .btn:hover { background: #30363d; color: #f0f6fc; }
  .btn-primary { background: #238636; border-color: #2ea043; color: #fff; }
  .btn-primary:hover { background: #2ea043; }
  .btn-danger { background: #b91c1c; border-color: #ef4444; color: #fff; }
  .btn-danger:hover { background: #dc2626; }
  .btn-sm { padding: 4px 10px; font-size: 12px; }

  /* Flash messages */
  .flash { padding: 10px 16px; border-radius: 6px; margin-bottom: 12px; font-size: 13px; }
  .flash-success { background: #0d4429; border: 1px solid #2ea043; color: #3fb950; }
  .flash-error { background: #3d1212; border: 1px solid #ef4444; color: #f87171; }
  .flash-warning { background: #3d2d00; border: 1px solid #d29922; color: #e3b341; }
  .flash-info { background: #0c2d6b; border: 1px solid #388bfd; color: #79c0ff; }

  /* File table */
  .file-table { width: 100%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; }
  .file-table th { background: #1c2128; color: #8b949e; font-weight: 600; text-align: left; padding: 10px 16px; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; border-bottom: 1px solid #30363d; }
  .file-table td { padding: 10px 16px; border-bottom: 1px solid #21262d; vertical-align: middle; }
  .file-table tr:last-child td { border-bottom: none; }
  .file-table tr:hover td { background: #1c2128; }
  .file-icon { margin-right: 6px; }
  .file-name { display: flex; align-items: center; }
  .col-size { width: 100px; color: #8b949e; }
  .col-modified { width: 160px; color: #8b949e; }
  .col-actions { width: 220px; text-align: right; }
  .actions-cell { display: flex; gap: 6px; justify-content: flex-end; flex-wrap: wrap; }
  .empty-dir { text-align: center; padding: 48px 16px; color: #484f58; font-size: 15px; }

  /* Upload drop zone */
  .upload-zone { border: 2px dashed #30363d; border-radius: 8px; padding: 24px; text-align: center; color: #8b949e; margin-bottom: 16px; cursor: pointer; transition: border-color 0.2s, background 0.2s; }
  .upload-zone.drag-over { border-color: #58a6ff; background: #0c1a2e; color: #79c0ff; }
  .upload-zone input[type=file] { display: none; }

  /* Modals */
  .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); z-index: 1000; align-items: center; justify-content: center; }
  .modal-overlay.active { display: flex; }
  .modal { background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 24px; width: 100%; max-width: 440px; box-shadow: 0 16px 48px rgba(0,0,0,0.6); }
  .modal h2 { font-size: 16px; font-weight: 600; color: #f0f6fc; margin-bottom: 16px; }
  .modal label { display: block; font-size: 12px; color: #8b949e; margin-bottom: 4px; }
  .modal input[type=text] { width: 100%; padding: 8px 12px; border-radius: 6px; border: 1px solid #30363d; background: #0d1117; color: #c9d1d9; font-size: 14px; outline: none; margin-bottom: 16px; }
  .modal input[type=text]:focus { border-color: #58a6ff; }
  .modal-actions { display: flex; gap: 8px; justify-content: flex-end; }
  .modal-body { color: #c9d1d9; font-size: 14px; margin-bottom: 16px; }
  .modal-body strong { color: #f0f6fc; }
</style>
</head>
<body>

<div class="header">
  <span class="header-title">📁 {{ project_name }}</span>
  <span class="header-expiry">Session expires: {{ expiry_str }}</span>
</div>

<div class="container">

  <!-- Flash messages -->
  {% for category, message in get_flashed_messages(with_categories=true) %}
  <div class="flash flash-{{ category }}">{{ message }}</div>
  {% endfor %}

  <!-- Breadcrumb -->
  <div class="breadcrumb">
    {% for crumb in breadcrumbs %}
      {% if not loop.last %}
        <a href="{{ crumb.url }}">{{ crumb.label }}</a>
        <span class="sep">/</span>
      {% else %}
        <span>{{ crumb.label }}</span>
      {% endif %}
    {% endfor %}
  </div>

  <!-- Toolbar -->
  <div class="toolbar">
    <button class="btn btn-primary" onclick="showModal('modal-new-file')">➕ New File</button>
    <button class="btn" onclick="showModal('modal-new-folder')">📂 New Folder</button>
    <button class="btn" onclick="showModal('modal-upload')">⬆️ Upload</button>
  </div>

  <!-- File table -->
  {% if entries %}
  <table class="file-table">
    <thead>
      <tr>
        <th>Name</th>
        <th class="col-size">Size</th>
        <th class="col-modified">Modified</th>
        <th class="col-actions">Actions</th>
      </tr>
    </thead>
    <tbody>
      {% if parent_path is not none %}
      <tr>
        <td colspan="4"><a href="{{ parent_path }}">⬆️ ..</a></td>
      </tr>
      {% endif %}
      {% for entry in entries %}
      <tr>
        <td class="file-name">
          {% if entry.is_dir %}
            <span class="file-icon">📁</span>
            <a href="{{ entry.browse_url }}">{{ entry.name }}</a>
          {% else %}
            <span class="file-icon">📄</span>
            {{ entry.name }}
          {% endif %}
        </td>
        <td class="col-size">{{ entry.size }}</td>
        <td class="col-modified">{{ entry.modified }}</td>
        <td class="col-actions">
          <div class="actions-cell">
            {% if entry.editable %}
              <a class="btn btn-sm" href="{{ entry.edit_url }}">✏️ Edit</a>
            {% endif %}
            {% if not entry.is_dir %}
              <a class="btn btn-sm" href="{{ entry.download_url }}">⬇️</a>
            {% endif %}
            <button class="btn btn-sm" onclick="renameItem('{{ entry.name }}', '{{ current_rel }}')">✏️ Rename</button>
            <button class="btn btn-sm btn-danger" onclick="deleteItem('{{ entry.name }}', {{ 'true' if entry.is_dir else 'false' }}, '{{ current_rel }}')">🗑️</button>
          </div>
        </td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
  {% if parent_path is not none %}
  <div style="margin-bottom:8px;"><a href="{{ parent_path }}">⬆️ ..</a></div>
  {% endif %}
  <div class="empty-dir">This directory is empty.</div>
  {% endif %}

</div><!-- /container -->

<!-- ── Modal: New File ── -->
<div class="modal-overlay" id="modal-new-file">
  <div class="modal">
    <h2>New File</h2>
    <form method="POST" action="{{ create_file_url }}">
      <label>File name</label>
      <input type="text" name="filename" placeholder="example.py" autofocus>
      <div class="modal-actions">
        <button type="button" class="btn" onclick="hideModal('modal-new-file')">Cancel</button>
        <button type="submit" class="btn btn-primary">Create</button>
      </div>
    </form>
  </div>
</div>

<!-- ── Modal: New Folder ── -->
<div class="modal-overlay" id="modal-new-folder">
  <div class="modal">
    <h2>New Folder</h2>
    <form method="POST" action="{{ create_folder_url }}">
      <label>Folder name</label>
      <input type="text" name="foldername" placeholder="my-folder" autofocus>
      <div class="modal-actions">
        <button type="button" class="btn" onclick="hideModal('modal-new-folder')">Cancel</button>
        <button type="submit" class="btn btn-primary">Create</button>
      </div>
    </form>
  </div>
</div>

<!-- ── Modal: Rename ── -->
<div class="modal-overlay" id="modal-rename">
  <div class="modal">
    <h2>Rename</h2>
    <form method="POST" action="{{ rename_url }}">
      <input type="hidden" name="old_name" id="rename-old">
      <input type="hidden" name="rel" id="rename-rel">
      <label>New name</label>
      <input type="text" name="new_name" id="rename-new" autofocus>
      <div class="modal-actions">
        <button type="button" class="btn" onclick="hideModal('modal-rename')">Cancel</button>
        <button type="submit" class="btn btn-primary">Rename</button>
      </div>
    </form>
  </div>
</div>

<!-- ── Modal: Delete Confirm ── -->
<div class="modal-overlay" id="modal-delete">
  <div class="modal">
    <h2>Confirm Delete</h2>
    <div class="modal-body">Are you sure you want to delete <strong id="delete-name-label"></strong>? This cannot be undone.</div>
    <form method="POST" action="{{ delete_url }}">
      <input type="hidden" name="name" id="delete-name">
      <input type="hidden" name="rel" id="delete-rel">
      <div class="modal-actions">
        <button type="button" class="btn" onclick="hideModal('modal-delete')">Cancel</button>
        <button type="submit" class="btn btn-danger">Delete</button>
      </div>
    </form>
  </div>
</div>

<!-- ── Modal: Upload ── -->
<div class="modal-overlay" id="modal-upload">
  <div class="modal">
    <h2>Upload Files</h2>
    <form method="POST" action="{{ upload_url }}" enctype="multipart/form-data" id="upload-form">
      <div class="upload-zone" id="drop-zone" onclick="document.getElementById('file-input').click()">
        <input type="file" id="file-input" name="files" multiple onchange="uploadFiles()">
        <p>Click or drag &amp; drop files here</p>
      </div>
      <div id="upload-list" style="font-size:12px;color:#8b949e;margin-bottom:12px;"></div>
      <div class="modal-actions">
        <button type="button" class="btn" onclick="hideModal('modal-upload')">Cancel</button>
        <button type="submit" class="btn btn-primary">Upload</button>
      </div>
    </form>
  </div>
</div>

<script>
function showModal(id) {
  document.getElementById(id).classList.add('active');
}
function hideModal(id) {
  document.getElementById(id).classList.remove('active');
}

// Close modal on overlay click
document.querySelectorAll('.modal-overlay').forEach(function(overlay) {
  overlay.addEventListener('click', function(e) {
    if (e.target === overlay) overlay.classList.remove('active');
  });
});

function renameItem(name, rel) {
  document.getElementById('rename-old').value = name;
  document.getElementById('rename-new').value = name;
  document.getElementById('rename-rel').value = rel;
  showModal('modal-rename');
  setTimeout(function(){ document.getElementById('rename-new').focus(); }, 50);
}

function deleteItem(name, isDir, rel) {
  document.getElementById('delete-name').value = name;
  document.getElementById('delete-rel').value = rel;
  document.getElementById('delete-name-label').textContent = name + (isDir ? ' (folder)' : '');
  showModal('modal-delete');
}

function uploadFiles() {
  var input = document.getElementById('file-input');
  var list = document.getElementById('upload-list');
  var names = Array.from(input.files).map(function(f){ return f.name; });
  list.textContent = names.length ? 'Selected: ' + names.join(', ') : '';
}

// Drag-and-drop
var dropZone = document.getElementById('drop-zone');
if (dropZone) {
  dropZone.addEventListener('dragover', function(e) {
    e.preventDefault();
    dropZone.classList.add('drag-over');
  });
  dropZone.addEventListener('dragleave', function() {
    dropZone.classList.remove('drag-over');
  });
  dropZone.addEventListener('drop', function(e) {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    document.getElementById('file-input').files = e.dataTransfer.files;
    uploadFiles();
  });
}
</script>
</body>
</html>"""

EDITOR_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Edit — {{ filename }} — {{ project_name }}</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', system-ui, sans-serif; font-size: 14px; display: flex; flex-direction: column; height: 100vh; overflow: hidden; }
  a { color: #58a6ff; text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* Header */
  .header { background: #161b22; border-bottom: 1px solid #30363d; padding: 12px 24px; display: flex; align-items: center; justify-content: space-between; flex-shrink: 0; }
  .header-title { font-size: 16px; font-weight: 600; color: #f0f6fc; }
  .header-expiry { font-size: 12px; color: #8b949e; }

  /* Editor container */
  .editor-container { flex: 1; display: flex; flex-direction: column; overflow: hidden; padding: 16px; gap: 12px; }

  /* Breadcrumb */
  .breadcrumb { display: flex; align-items: center; flex-wrap: wrap; gap: 4px; font-size: 13px; color: #8b949e; flex-shrink: 0; }
  .breadcrumb a { color: #58a6ff; }
  .breadcrumb .sep { color: #484f58; }

  /* Toolbar */
  .editor-toolbar { display: flex; gap: 8px; align-items: center; flex-shrink: 0; }
  .btn { display: inline-flex; align-items: center; gap: 6px; padding: 6px 14px; border-radius: 6px; border: 1px solid #30363d; background: #21262d; color: #c9d1d9; cursor: pointer; font-size: 13px; transition: background 0.15s; }
  .btn:hover { background: #30363d; color: #f0f6fc; }
  .btn-primary { background: #238636; border-color: #2ea043; color: #fff; }
  .btn-primary:hover { background: #2ea043; }
  .save-status { font-size: 12px; color: #8b949e; margin-left: 8px; }

  /* Flash messages */
  .flash { padding: 10px 16px; border-radius: 6px; font-size: 13px; flex-shrink: 0; }
  .flash-success { background: #0d4429; border: 1px solid #2ea043; color: #3fb950; }
  .flash-error { background: #3d1212; border: 1px solid #ef4444; color: #f87171; }
  .flash-warning { background: #3d2d00; border: 1px solid #d29922; color: #e3b341; }
  .flash-info { background: #0c2d6b; border: 1px solid #388bfd; color: #79c0ff; }

  /* Textarea */
  .editor-area { flex: 1; background: #0d1117; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; display: flex; flex-direction: column; }
  #editor { flex: 1; background: #0d1117; color: #c9d1d9; border: none; outline: none; resize: none; font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', 'Consolas', monospace; font-size: 13px; line-height: 1.6; padding: 16px; width: 100%; height: 100%; tab-size: 4; }
</style>
</head>
<body>

<div class="header">
  <span class="header-title">✏️ {{ project_name }}</span>
  <span class="header-expiry">Session expires: {{ expiry_str }}</span>
</div>

<div class="editor-container">

  <!-- Flash messages -->
  {% for category, message in get_flashed_messages(with_categories=true) %}
  <div class="flash flash-{{ category }}">{{ message }}</div>
  {% endfor %}

  <!-- Breadcrumb -->
  <div class="breadcrumb">
    {% for crumb in breadcrumbs %}
      {% if not loop.last %}
        <a href="{{ crumb.url }}">{{ crumb.label }}</a>
        <span class="sep">/</span>
      {% else %}
        <span>{{ crumb.label }}</span>
      {% endif %}
    {% endfor %}
  </div>

  <!-- Toolbar -->
  <div class="editor-toolbar">
    <button class="btn btn-primary" onclick="saveFile()">💾 Save</button>
    <a class="btn" href="{{ back_url }}">⬅️ Back</a>
    <span class="save-status" id="save-status"></span>
  </div>

  <!-- Editor area -->
  <div class="editor-area">
    <textarea id="editor" spellcheck="false">{{ content }}</textarea>
  </div>

</div>

<script>
var saveUrl = {{ save_url | tojson }};
var csrfToken = null; // No CSRF needed — token in URL is the auth

function saveFile() {
  var content = document.getElementById('editor').value;
  var status = document.getElementById('save-status');
  status.textContent = 'Saving…';
  status.style.color = '#8b949e';

  fetch(saveUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ content: content })
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.ok) {
      status.textContent = '✓ Saved';
      status.style.color = '#3fb950';
    } else {
      status.textContent = '✗ Error: ' + (data.error || 'Unknown error');
      status.style.color = '#f87171';
    }
    setTimeout(function(){ status.textContent = ''; }, 3000);
  })
  .catch(function(err) {
    status.textContent = '✗ Network error';
    status.style.color = '#f87171';
    setTimeout(function(){ status.textContent = ''; }, 3000);
  });
}

// Tab key support
document.getElementById('editor').addEventListener('keydown', function(e) {
  if (e.key === 'Tab') {
    e.preventDefault();
    var ta = this;
    var start = ta.selectionStart;
    var end = ta.selectionEnd;
    if (e.shiftKey) {
      // Dedent: remove up to 4 leading spaces on current line
      var lineStart = ta.value.lastIndexOf('\\n', start - 1) + 1;
      var linePrefix = ta.value.slice(lineStart, start);
      var spaces = linePrefix.match(/^ {1,4}/);
      if (spaces) {
        ta.value = ta.value.slice(0, lineStart) + ta.value.slice(lineStart + spaces[0].length);
        ta.selectionStart = ta.selectionEnd = start - spaces[0].length;
      }
    } else {
      ta.value = ta.value.slice(0, start) + '    ' + ta.value.slice(end);
      ta.selectionStart = ta.selectionEnd = start + 4;
    }
  }

  // Ctrl+S / Cmd+S to save
  if ((e.ctrlKey || e.metaKey) && e.key === 's') {
    e.preventDefault();
    saveFile();
  }
});
</script>
</body>
</html>"""


# ── Flask app setup ────────────────────────────────────────────────────────────

flask_app = Flask(__name__)
flask_app.secret_key = FM_SECRET


@flask_app.errorhandler(500)
def handle_500(e):
    logger.error("Flask 500: %s", e, exc_info=True)
    return f"<h1>500 Error</h1><p>{e}</p>", 500


@flask_app.errorhandler(Exception)
def handle_exception(e):
    logger.error("Flask error: %s", e, exc_info=True)
    return f"<h1>Error</h1><p>{e}</p>", 500


# ── Helper functions ───────────────────────────────────────────────────────────

def validate_token(token: str) -> dict | None:
    """Return token data dict if token is valid and not expired, else None."""
    try:
        token_file = TOKENS_DIR / f"{token}.json"
        if not token_file.exists():
            return None
        data = json.loads(token_file.read_text())
        expires_at = datetime.fromisoformat(data["expires_at"])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > expires_at:
            return None
        return data
    except Exception:
        return None





def safe_path(base: Path, rel: str) -> Path | None:
    """Resolve a relative path safely within base. Returns None on traversal attempt."""
    if not rel or rel == ".":
        return base
    try:
        clean_rel = rel.replace("\\", "/").strip("/")
        resolved = (base / clean_rel).resolve()
        base_resolved = base.resolve()
        if not str(resolved).startswith(str(base_resolved)):
            return None
        return resolved
    except Exception:
        return None


def human_size(size_bytes: int) -> str:
    """Convert bytes to human-readable size string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / 1024 ** 2:.1f} MB"
    else:
        return f"{size_bytes / 1024 ** 3:.1f} GB"


EDITABLE_EXTENSIONS = {
    ".py", ".txt", ".md", ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg",
    ".env", ".sh", ".bash", ".zsh", ".html", ".css", ".js", ".ts", ".jsx",
    ".tsx", ".xml", ".csv", ".log", ".sql", ".r", ".rb", ".php", ".go",
    ".java", ".c", ".cpp", ".h", ".hpp", ".rs", ".swift", ".kt", ".scala",
    ".tf", ".tfvars", ".conf", ".config", ".dockerfile", "",
}


def is_editable(path: Path) -> bool:
    """Return True if the file can be opened in the text editor."""
    suffix = path.suffix.lower()
    if suffix in EDITABLE_EXTENSIONS:
        return True
    # Also allow files with no extension (like Dockerfile, Makefile)
    if not suffix:
        return True
    return False


def get_token_and_base(token: str):
    """Validate token and return (token_data, base_path). Aborts 403 on failure."""
    token_data = validate_token(token)
    if not token_data:
        abort(403)
    project_id = token_data.get("project_id", "")
    base = PROJECTS_DIR / project_id
    if not base.exists():
        abort(404)
    return token_data, base


def build_breadcrumbs(token: str, rel: str, base_label: str = "Home"):
    """Build breadcrumb list of {label, url} dicts for the given relative path."""
    crumbs = [{"label": base_label, "url": url_for("browse", token=token)}]
    if not rel or rel == ".":
        return crumbs
    parts = rel.replace("\\", "/").strip("/").split("/")
    accumulated = ""
    for part in parts:
        accumulated = f"{accumulated}/{part}".lstrip("/")
        crumbs.append({
            "label": part,
            "url": url_for("browse", token=token, rel=accumulated),
        })
    return crumbs


def list_dir(directory: Path, token: str, rel: str) -> list:
    """Return sorted list of entry dicts for directory listing."""
    entries = []
    try:
        items = sorted(directory.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    except PermissionError:
        return entries

    for item in items:
        try:
            stat = item.stat()
            item_rel = f"{rel}/{item.name}".lstrip("/") if rel else item.name
            entry = {
                "name": item.name,
                "is_dir": item.is_dir(),
                "size": "—" if item.is_dir() else human_size(stat.st_size),
                "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                "browse_url": url_for("browse", token=token, rel=item_rel) if item.is_dir() else "",
                "edit_url": url_for("edit_file", token=token, rel=item_rel) if not item.is_dir() else "",
                "download_url": url_for("download_file", token=token, rel=item_rel) if not item.is_dir() else "",
                "editable": is_editable(item) if not item.is_dir() else False,
            }
            entries.append(entry)
        except Exception:
            continue
    return entries


def token_expiry_str(token_data: dict) -> str:
    """Return human-readable expiry string from token data."""
    try:
        expires_at = datetime.fromisoformat(token_data["expires_at"])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        local_dt = expires_at.astimezone()
        return local_dt.strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        return "Unknown"


def get_project_name(token_data: dict) -> str:
    """Extract a display name for the project from token data."""
    return token_data.get("project_name") or token_data.get("project_id") or "Project"


# ── Routes ─────────────────────────────────────────────────────────────────────

@flask_app.route("/")
def health_check():
    return "OK", 200


@flask_app.route("/health")
def health():
    return "OK", 200


@flask_app.route("/fm/<token>")
def browse(token):
    try:
        token_data, base = get_token_and_base(token)
        rel = request.args.get("rel", "").strip().strip("/")
        current_dir = safe_path(base, rel)
        if current_dir is None or not current_dir.exists():
            flash("Directory not found.", "error")
            return redirect(url_for("browse", token=token))

        if not current_dir.is_dir():
            # If someone navigates to a file path, redirect to edit
            return redirect(url_for("edit_file", token=token, rel=rel))

        project_name = get_project_name(token_data)
        expiry_str = token_expiry_str(token_data)
        breadcrumbs = build_breadcrumbs(token, rel, base_label=project_name)
        entries = list_dir(current_dir, token, rel)

        # Parent path for ".." link
        parent_path = None
        if rel:
            parent_rel = "/".join(rel.replace("\\", "/").strip("/").split("/")[:-1])
            parent_path = url_for("browse", token=token, rel=parent_rel) if parent_rel else url_for("browse", token=token)

        return render_template_string(
            BROWSER_TEMPLATE,
            project_name=project_name,
            expiry_str=expiry_str,
            breadcrumbs=breadcrumbs,
            entries=entries,
            current_rel=rel,
            parent_path=parent_path,
            create_file_url=url_for("create_file", token=token),
            create_folder_url=url_for("create_folder", token=token),
            rename_url=url_for("rename_item", token=token),
            delete_url=url_for("delete_item", token=token),
            upload_url=url_for("upload_files", token=token),
        )
    except Exception as e:
        logger.error("browse error: %s", e, exc_info=True)
        flash(f"Error: {e}", "error")
        return f"<h1>Error</h1><p>{e}</p>", 500


@flask_app.route("/fm/<token>/edit")
def edit_file(token):
    try:
        token_data, base = get_token_and_base(token)
        rel = request.args.get("rel", "").strip().strip("/")
        if not rel:
            flash("No file specified.", "error")
            return redirect(url_for("browse", token=token))

        file_path = safe_path(base, rel)
        if file_path is None or not file_path.exists() or not file_path.is_file():
            flash("File not found.", "error")
            return redirect(url_for("browse", token=token))

        if not is_editable(file_path):
            flash("This file type cannot be edited in the browser.", "warning")
            return redirect(url_for("browse", token=token, rel=str(Path(rel).parent)))

        try:
            content = file_path.read_text(encoding="utf-8", errors="replace")
        except Exception as read_err:
            flash(f"Could not read file: {read_err}", "error")
            return redirect(url_for("browse", token=token, rel=str(Path(rel).parent)))

        project_name = get_project_name(token_data)
        expiry_str = token_expiry_str(token_data)
        breadcrumbs = build_breadcrumbs(token, rel, base_label=project_name)
        parent_rel = "/".join(rel.replace("\\", "/").strip("/").split("/")[:-1])
        back_url = url_for("browse", token=token, rel=parent_rel) if parent_rel else url_for("browse", token=token)

        return render_template_string(
            EDITOR_TEMPLATE,
            project_name=project_name,
            expiry_str=expiry_str,
            breadcrumbs=breadcrumbs,
            filename=file_path.name,
            content=content,
            save_url=url_for("save_file", token=token, rel=rel),
            back_url=back_url,
        )
    except Exception as e:
        logger.error("edit_file error: %s", e, exc_info=True)
        return f"<h1>Error</h1><p>{e}</p>", 500


@flask_app.route("/fm/<token>/save", methods=["POST"])
def save_file(token):
    try:
        token_data, base = get_token_and_base(token)
        rel = request.args.get("rel", "").strip().strip("/")
        if not rel:
            return jsonify({"ok": False, "error": "No file specified"}), 400

        file_path = safe_path(base, rel)
        if file_path is None:
            return jsonify({"ok": False, "error": "Invalid path"}), 400

        data = request.get_json(force=True, silent=True)
        if data is None or "content" not in data:
            return jsonify({"ok": False, "error": "No content provided"}), 400

        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(data["content"], encoding="utf-8")
        logger.info("Saved file: %s", file_path)
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("save_file error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@flask_app.route("/fm/<token>/download")
def download_file(token):
    try:
        token_data, base = get_token_and_base(token)
        rel = request.args.get("rel", "").strip().strip("/")
        if not rel:
            abort(400)
        file_path = safe_path(base, rel)
        if file_path is None or not file_path.exists() or not file_path.is_file():
            abort(404)
        return send_file(file_path, as_attachment=True, download_name=file_path.name)
    except Exception as e:
        logger.error("download_file error: %s", e, exc_info=True)
        return f"<h1>Error</h1><p>{e}</p>", 500


@flask_app.route("/fm/<token>/create_file", methods=["POST"])
def create_file(token):
    try:
        token_data, base = get_token_and_base(token)
        rel = request.form.get("rel", "").strip().strip("/")
        filename = request.form.get("filename", "").strip()
        if not filename:
            flash("Filename cannot be empty.", "error")
            return redirect(url_for("browse", token=token, rel=rel))
        if "/" in filename or "\\" in filename:
            flash("Filename must not contain slashes.", "error")
            return redirect(url_for("browse", token=token, rel=rel))

        current_dir = safe_path(base, rel) if rel else base
        if current_dir is None:
            flash("Invalid path.", "error")
            return redirect(url_for("browse", token=token))

        new_file = current_dir / filename
        if new_file.exists():
            flash(f"'{filename}' already exists.", "error")
        else:
            new_file.parent.mkdir(parents=True, exist_ok=True)
            new_file.touch()
            flash(f"File '{filename}' created.", "success")

        return redirect(url_for("browse", token=token, rel=rel))
    except Exception as e:
        logger.error("create_file error: %s", e, exc_info=True)
        flash(f"Error: {e}", "error")
        return redirect(url_for("browse", token=token))


@flask_app.route("/fm/<token>/create_folder", methods=["POST"])
def create_folder(token):
    try:
        token_data, base = get_token_and_base(token)
        rel = request.form.get("rel", "").strip().strip("/")
        foldername = request.form.get("foldername", "").strip()
        if not foldername:
            flash("Folder name cannot be empty.", "error")
            return redirect(url_for("browse", token=token, rel=rel))
        if "/" in foldername or "\\" in foldername:
            flash("Folder name must not contain slashes.", "error")
            return redirect(url_for("browse", token=token, rel=rel))

        current_dir = safe_path(base, rel) if rel else base
        if current_dir is None:
            flash("Invalid path.", "error")
            return redirect(url_for("browse", token=token))

        new_folder = current_dir / foldername
        if new_folder.exists():
            flash(f"'{foldername}' already exists.", "error")
        else:
            new_folder.mkdir(parents=True, exist_ok=True)
            flash(f"Folder '{foldername}' created.", "success")

        return redirect(url_for("browse", token=token, rel=rel))
    except Exception as e:
        logger.error("create_folder error: %s", e, exc_info=True)
        flash(f"Error: {e}", "error")
        return redirect(url_for("browse", token=token))


@flask_app.route("/fm/<token>/rename", methods=["POST"])
def rename_item(token):
    try:
        token_data, base = get_token_and_base(token)
        rel = request.form.get("rel", "").strip().strip("/")
        old_name = request.form.get("old_name", "").strip()
        new_name = request.form.get("new_name", "").strip()

        if not old_name or not new_name:
            flash("Names cannot be empty.", "error")
            return redirect(url_for("browse", token=token, rel=rel))
        if "/" in new_name or "\\" in new_name:
            flash("New name must not contain slashes.", "error")
            return redirect(url_for("browse", token=token, rel=rel))

        current_dir = safe_path(base, rel) if rel else base
        if current_dir is None:
            flash("Invalid path.", "error")
            return redirect(url_for("browse", token=token))

        old_path = current_dir / old_name
        new_path = current_dir / new_name

        if not old_path.exists():
            flash(f"'{old_name}' not found.", "error")
        elif new_path.exists():
            flash(f"'{new_name}' already exists.", "error")
        else:
            old_path.rename(new_path)
            flash(f"Renamed '{old_name}' → '{new_name}'.", "success")

        return redirect(url_for("browse", token=token, rel=rel))
    except Exception as e:
        logger.error("rename_item error: %s", e, exc_info=True)
        flash(f"Error: {e}", "error")
        return redirect(url_for("browse", token=token))


@flask_app.route("/fm/<token>/delete", methods=["POST"])
def delete_item(token):
    try:
        token_data, base = get_token_and_base(token)
        rel = request.form.get("rel", "").strip().strip("/")
        name = request.form.get("name", "").strip()

        if not name:
            flash("No item specified.", "error")
            return redirect(url_for("browse", token=token, rel=rel))

        current_dir = safe_path(base, rel) if rel else base
        if current_dir is None:
            flash("Invalid path.", "error")
            return redirect(url_for("browse", token=token))

        target = current_dir / name
        safe_target = safe_path(base, f"{rel}/{name}".lstrip("/"))
        if safe_target is None:
            flash("Invalid path.", "error")
            return redirect(url_for("browse", token=token, rel=rel))

        if not target.exists():
            flash(f"'{name}' not found.", "error")
        elif target.is_dir():
            shutil.rmtree(target)
            flash(f"Folder '{name}' deleted.", "success")
        else:
            target.unlink()
            flash(f"File '{name}' deleted.", "success")

        return redirect(url_for("browse", token=token, rel=rel))
    except Exception as e:
        logger.error("delete_item error: %s", e, exc_info=True)
        flash(f"Error: {e}", "error")
        return redirect(url_for("browse", token=token))


@flask_app.route("/fm/<token>/upload", methods=["POST"])
def upload_files(token):
    try:
        token_data, base = get_token_and_base(token)
        rel = request.form.get("rel", "").strip().strip("/")
        current_dir = safe_path(base, rel) if rel else base
        if current_dir is None:
            flash("Invalid path.", "error")
            return redirect(url_for("browse", token=token))

        files = request.files.getlist("files")
        if not files or all(f.filename == "" for f in files):
            flash("No files selected.", "warning")
            return redirect(url_for("browse", token=token, rel=rel))

        uploaded = 0
        skipped = 0
        for f in files:
            if f.filename == "":
                continue
            # Sanitize filename — strip directory components
            safe_name = os.path.basename(f.filename.replace("\\", "/"))
            if not safe_name:
                skipped += 1
                continue
            dest = current_dir / safe_name
            # Confirm destination is still within base
            if safe_path(base, f"{rel}/{safe_name}".lstrip("/")) is None:
                skipped += 1
                continue
            current_dir.mkdir(parents=True, exist_ok=True)
            f.save(str(dest))
            uploaded += 1

        if uploaded:
            flash(f"Uploaded {uploaded} file(s)." + (f" Skipped {skipped}." if skipped else ""), "success")
        else:
            flash("No valid files were uploaded.", "warning")

        return redirect(url_for("browse", token=token, rel=rel))
    except Exception as e:
        logger.error("upload_files error: %s", e, exc_info=True)
        flash(f"Upload error: {e}", "error")
        return redirect(url_for("browse", token=token))


# ═══ ENTRY POINT ════════════════════════════════════════════════════════════

def run_flask() -> None:
    TOKENS_DIR.mkdir(exist_ok=True)
    PROJECTS_DIR.mkdir(exist_ok=True)
    logger.info("Starting Flask file manager on port %d…", PORT)
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
    TOKENS_DIR.mkdir(parents=True, exist_ok=True)

    flask_thread = threading.Thread(target=run_flask, daemon=True, name="FlaskFileManager")
    flask_thread.start()
    logger.info("Flask file manager thread started.")

    telegram_app = build_app()
    logger.info("Starting Telegram bot polling…")
    telegram_app.run_polling(allowed_updates=["message", "callback_query"])

