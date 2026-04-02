# main.py — God Madara Hosting Bot (FINAL MERGED — bot + file manager)
# Production-ready: Telegram bot + Flask file manager in ONE process
# Python 3.11+ | python-telegram-bot v20+ | Flask 3.0 | Motor 3.4
# Deploy directly to Render as a single Web Service.
# Threading: Flask runs in a daemon background thread; bot polls in the main thread.

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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import psutil
from dotenv import load_dotenv

# Flask / file manager
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

# Telegram
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

# MongoDB / GridFS
import motor.motor_asyncio
from bson import ObjectId
from pymongo import ASCENDING

load_dotenv()

# ═══ ENVIRONMENT & CONFIG ════════════════════════════════════════════════════

BOT_TOKEN      = os.environ["BOT_TOKEN"]
OWNER_ID       = int(os.environ["OWNER_ID"])
MONGODB_URI    = os.environ["MONGODB_URI"]
FM_SECRET      = os.environ.get("FM_SECRET", "changeme_secret")
PROJECTS_DIR   = Path(os.environ.get("PROJECTS_DIR", "projects"))
LOGS_DIR       = Path(os.environ.get("LOGS_DIR", "logs"))
TOKENS_DIR     = Path("fm_tokens")
FM_PORT        = int(os.environ.get("PORT", os.environ.get("FILE_MANAGER_PORT", 8080)))
FM_URL         = os.environ.get("FILE_MANAGER_URL", f"http://localhost:{FM_PORT}")
MAX_PROJECTS   = int(os.environ.get("MAX_PROJECTS", 5))
BACKUP_INTERVAL = int(os.environ.get("BACKUP_INTERVAL_MINUTES", 30))

# Ensure base directories exist
PROJECTS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
TOKENS_DIR.mkdir(exist_ok=True)

# ═══ LOGGING ════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("madara")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ═══ MONGODB & GRIDFS ════════════════════════════════════════════════════════

_mongo_client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
_db = None


def get_db():
    global _mongo_client, _db
    if _db is None:
        _mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URI)
        _db = _mongo_client["madara_hosting"]
    return _db


def get_gridfs_bucket():
    db = get_db()
    return motor.motor_asyncio.AsyncIOMotorGridFSBucket(db)


async def ensure_indexes():
    db = get_db()
    await db.users.create_index([("user_id", ASCENDING)], unique=True)
    await db.projects.create_index([("user_id", ASCENDING), ("name", ASCENDING)])
    await db.backup_meta.create_index([("project_id", ASCENDING)])


# ═══ PROCESS TRACKER ════════════════════════════════════════════════════════

# Maps project_id -> {"process": subprocess.Popen, "log_file": Path}
_running_processes: dict[str, dict] = {}
_process_lock = threading.Lock()


def get_process(project_id: str) -> Optional[dict]:
    with _process_lock:
        return _running_processes.get(project_id)


def set_process(project_id: str, data: dict):
    with _process_lock:
        _running_processes[project_id] = data


def remove_process(project_id: str):
    with _process_lock:
        _running_processes.pop(project_id, None)


# ═══ CONVERSATION STATES ════════════════════════════════════════════════════

(
    STATE_MAIN_MENU,
    STATE_CREATE_PROJECT_NAME,
    STATE_UPLOAD_FILE,
    STATE_ENTER_START_CMD,
    STATE_ADMIN_BROADCAST,
    STATE_ADMIN_BAN,
    STATE_ADMIN_UNBAN,
    STATE_ENTER_ENV_KEY,
    STATE_ENTER_ENV_VALUE,
    STATE_CONFIRM_DELETE_PROJECT,
) = range(10)

# ═══ GENERAL HELPERS ════════════════════════════════════════════════════════


def human_size(size: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def human_uptime(seconds: float) -> str:
    seconds = int(seconds)
    h, remainder = divmod(seconds, 3600)
    m, s = divmod(remainder, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def sanitize_project_name(name: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_\-]", "_", name.strip())
    return name[:40] if name else "project"


def escape_md(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    return re.sub(r"([_*\[\]()~`>#+\-=|{}.!\\])", r"\\\1", text)


async def reply(update: Update, text: str, **kwargs):
    """Send or edit message depending on update type."""
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, **kwargs)
        except Exception:
            await update.callback_query.message.reply_text(text, **kwargs)
    else:
        await update.message.reply_text(text, **kwargs)


def get_user_id(update: Update) -> int:
    if update.callback_query:
        return update.callback_query.from_user.id
    return update.effective_user.id


def get_user_name(update: Update) -> str:
    user = update.effective_user
    return user.full_name or user.username or str(user.id)


# ═══ PROCESS MANAGEMENT ══════════════════════════════════════════════════════


def start_project_process(project_id: str, project_path: Path, start_cmd: str, log_path: Path) -> subprocess.Popen:
    """Start a project subprocess and return the Popen object."""
    log_file = open(str(log_path), "a", encoding="utf-8")
    log_file.write(f"\n{'='*60}\n[{datetime.utcnow().isoformat()}] STARTING: {start_cmd}\n{'='*60}\n")
    log_file.flush()

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    try:
        args = shlex.split(start_cmd)
    except ValueError:
        args = start_cmd.split()

    proc = subprocess.Popen(
        args,
        cwd=str(project_path),
        stdout=log_file,
        stderr=log_file,
        env=env,
        start_new_session=True,
    )
    return proc


def stop_project_process(project_id: str) -> bool:
    """Stop a running project process. Returns True if stopped."""
    entry = get_process(project_id)
    if not entry:
        return False
    proc: subprocess.Popen = entry["process"]
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except Exception:
        try:
            proc.terminate()
        except Exception:
            pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except Exception:
            pass
    remove_process(project_id)
    # Close log file handle if open
    lf = entry.get("log_file_handle")
    if lf:
        try:
            lf.close()
        except Exception:
            pass
    return True


def is_process_alive(project_id: str) -> bool:
    entry = get_process(project_id)
    if not entry:
        return False
    proc: subprocess.Popen = entry["process"]
    return proc.poll() is None


def read_log_tail(log_path: Path, lines: int = 30) -> str:
    if not log_path.exists():
        return "(no log file yet)"
    try:
        text = log_path.read_text(errors="replace")
        tail = text.splitlines()[-lines:]
        return "\n".join(tail) or "(log is empty)"
    except Exception as e:
        return f"(error reading log: {e})"


# ═══ GRIDFS PERSISTENCE (backup / restore / watcher) ════════════════════════


async def backup_project_to_gridfs(project_id: str, project_path: Path) -> bool:
    """Zip the project directory and store it in GridFS."""
    try:
        bucket = get_gridfs_bucket()
        db = get_db()

        zip_buffer = io.BytesIO()
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        shutil.make_archive(str(tmp_path.with_suffix("")), "zip", str(project_path))
        zip_path = tmp_path.with_suffix(".zip")

        with open(str(zip_path), "rb") as f:
            data = f.read()

        zip_path.unlink(missing_ok=True)
        tmp_path.unlink(missing_ok=True)

        # Delete old backup
        old = await db.backup_meta.find_one({"project_id": project_id})
        if old and old.get("gridfs_id"):
            try:
                await bucket.delete(old["gridfs_id"])
            except Exception:
                pass

        # Upload new backup
        gridfs_id = await bucket.upload_from_stream(
            f"{project_id}.zip",
            io.BytesIO(data),
            metadata={"project_id": project_id, "backed_up_at": datetime.utcnow().isoformat()},
        )

        await db.backup_meta.update_one(
            {"project_id": project_id},
            {"$set": {"gridfs_id": gridfs_id, "backed_up_at": datetime.utcnow(), "size": len(data)}},
            upsert=True,
        )
        logger.info("✅ Backed up project %s (%s)", project_id, human_size(len(data)))
        return True
    except Exception as e:
        logger.error("❌ Backup failed for %s: %s", project_id, e)
        return False


async def restore_project_from_gridfs(project_id: str, project_path: Path) -> bool:
    """Restore a project from GridFS backup."""
    try:
        db = get_db()
        bucket = get_gridfs_bucket()
        meta = await db.backup_meta.find_one({"project_id": project_id})
        if not meta or not meta.get("gridfs_id"):
            return False

        zip_buffer = io.BytesIO()
        await bucket.download_to_stream(meta["gridfs_id"], zip_buffer)
        zip_buffer.seek(0)

        project_path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp.write(zip_buffer.read())
            tmp_path = Path(tmp.name)

        shutil.unpack_archive(str(tmp_path), str(project_path), "zip")
        tmp_path.unlink(missing_ok=True)
        logger.info("✅ Restored project %s to %s", project_id, project_path)
        return True
    except Exception as e:
        logger.error("❌ Restore failed for %s: %s", project_id, e)
        return False


async def restore_all_projects_on_startup():
    """Restore all projects from GridFS on bot startup (handles Render ephemeral disk)."""
    try:
        db = get_db()
        projects = await db.projects.find({}).to_list(length=None)
        for proj in projects:
            pid = str(proj["_id"])
            user_id = proj["user_id"]
            name = proj.get("name", pid)
            project_path = PROJECTS_DIR / str(user_id) / name
            if not project_path.exists():
                logger.info("🔄 Restoring project '%s' from GridFS…", name)
                await restore_project_from_gridfs(pid, project_path)
                # Write meta file for file manager
                meta_file = PROJECTS_DIR / f"meta_{pid}.txt"
                meta_file.write_text(str(project_path))
        logger.info("✅ Startup restore complete")
    except Exception as e:
        logger.error("❌ Startup restore error: %s", e)


async def periodic_backup_watcher(app_bot):
    """Background coroutine: backs up all running/modified projects every BACKUP_INTERVAL minutes."""
    while True:
        await asyncio.sleep(BACKUP_INTERVAL * 60)
        try:
            db = get_db()
            projects = await db.projects.find({}).to_list(length=None)
            for proj in projects:
                pid = str(proj["_id"])
                user_id = proj["user_id"]
                name = proj.get("name", pid)
                project_path = PROJECTS_DIR / str(user_id) / name
                if project_path.exists():
                    await backup_project_to_gridfs(pid, project_path)
        except Exception as e:
            logger.error("Backup watcher error: %s", e)


# ═══ KEYBOARDS ══════════════════════════════════════════════════════════════


def main_menu_keyboard(is_owner: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📁 My Projects", callback_data="menu_projects"),
            InlineKeyboardButton("➕ New Project", callback_data="menu_new_project"),
        ],
        [
            InlineKeyboardButton("📊 System Stats", callback_data="menu_stats"),
            InlineKeyboardButton("❓ Help", callback_data="menu_help"),
        ],
    ]
    if is_owner:
        rows.append([InlineKeyboardButton("🛡️ Admin Panel", callback_data="menu_admin")])
    return InlineKeyboardMarkup(rows)


def projects_keyboard(projects: list) -> InlineKeyboardMarkup:
    rows = []
    for proj in projects:
        pid = str(proj["_id"])
        name = proj.get("name", pid)
        status = "🟢" if is_process_alive(pid) else "🔴"
        rows.append([InlineKeyboardButton(f"{status} {name}", callback_data=f"proj_{pid}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="menu_main")])
    return InlineKeyboardMarkup(rows)


def project_keyboard(project_id: str, alive: bool) -> InlineKeyboardMarkup:
    toggle = InlineKeyboardButton("⏹ Stop", callback_data=f"proj_stop_{project_id}") if alive else InlineKeyboardButton("▶️ Start", callback_data=f"proj_start_{project_id}")
    rows = [
        [toggle, InlineKeyboardButton("📋 Logs", callback_data=f"proj_logs_{project_id}")],
        [
            InlineKeyboardButton("📤 Upload File", callback_data=f"proj_upload_{project_id}"),
            InlineKeyboardButton("🌐 File Manager", callback_data=f"proj_fm_{project_id}"),
        ],
        [
            InlineKeyboardButton("⚙️ Set Start Cmd", callback_data=f"proj_cmd_{project_id}"),
            InlineKeyboardButton("🔑 Env Vars", callback_data=f"proj_env_{project_id}"),
        ],
        [
            InlineKeyboardButton("💾 Backup Now", callback_data=f"proj_backup_{project_id}"),
            InlineKeyboardButton("🗑️ Delete", callback_data=f"proj_delete_{project_id}"),
        ],
        [InlineKeyboardButton("⬅️ Back to Projects", callback_data="menu_projects")],
    ]
    return InlineKeyboardMarkup(rows)


def env_keyboard(project_id: str, env_vars: dict) -> InlineKeyboardMarkup:
    rows = []
    for key in list(env_vars.keys())[:10]:
        rows.append([
            InlineKeyboardButton(f"🗑 {key}", callback_data=f"env_del_{project_id}_{key}"),
        ])
    rows.append([InlineKeyboardButton("➕ Add Var", callback_data=f"env_add_{project_id}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=f"proj_{project_id}")])
    return InlineKeyboardMarkup(rows)


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👥 List Users", callback_data="admin_users"),
            InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
        ],
        [
            InlineKeyboardButton("🚫 Ban User", callback_data="admin_ban"),
            InlineKeyboardButton("✅ Unban User", callback_data="admin_unban"),
        ],
        [
            InlineKeyboardButton("📊 Full Stats", callback_data="admin_stats"),
            InlineKeyboardButton("🔄 Force Backup All", callback_data="admin_backup_all"),
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data="menu_main")],
    ])


# ═══ BOT HANDLERS ════════════════════════════════════════════════════════════


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = get_user_id(update)
    db = get_db()

    # Register user if not exists
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$setOnInsert": {
                "user_id": user_id,
                "username": update.effective_user.username,
                "full_name": update.effective_user.full_name,
                "joined_at": datetime.utcnow(),
                "is_banned": False,
            }
        },
        upsert=True,
    )

    user = await db.users.find_one({"user_id": user_id})
    if user and user.get("is_banned"):
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return STATE_MAIN_MENU

    is_owner = user_id == OWNER_ID
    name = get_user_name(update)
    text = (
        f"👋 Welcome, *{escape_md(name)}*\\!\n\n"
        f"I'm *God Madara Hosting Bot* 🚀\n"
        f"I can host your Python projects right here\\.\n\n"
        f"Use the menu below to get started\\."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_keyboard(is_owner))
    return STATE_MAIN_MENU


async def menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    is_owner = user_id == OWNER_ID
    await reply(update, "🏠 *Main Menu* — Choose an option:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_keyboard(is_owner))
    return STATE_MAIN_MENU


async def menu_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    db = get_db()
    projects = await db.projects.find({"user_id": user_id}).to_list(length=None)
    if not projects:
        await reply(update, "📭 You have no projects yet\\. Use *➕ New Project* to create one\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ New Project", callback_data="menu_new_project"), InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]]))
    else:
        await reply(update, f"📁 *Your Projects* \\({len(projects)}/{MAX_PROJECTS}\\):", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=projects_keyboard(projects))
    return STATE_MAIN_MENU


async def menu_new_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    db = get_db()

    # Check ban
    user = await db.users.find_one({"user_id": user_id})
    if user and user.get("is_banned"):
        await reply(update, "🚫 You are banned.")
        return STATE_MAIN_MENU

    count = await db.projects.count_documents({"user_id": user_id})
    if count >= MAX_PROJECTS:
        await reply(update, f"❌ You've reached the limit of *{MAX_PROJECTS}* projects\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu_projects")]]))
        return STATE_MAIN_MENU

    await reply(update, "✏️ Send me a *project name* \\(letters, numbers, dashes only\\):", parse_mode=ParseMode.MARKDOWN_V2)
    return STATE_CREATE_PROJECT_NAME


async def receive_project_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    raw_name = update.message.text.strip()
    name = sanitize_project_name(raw_name)
    if not name:
        await update.message.reply_text("❌ Invalid name. Use letters, numbers, dashes. Try again:")
        return STATE_CREATE_PROJECT_NAME

    db = get_db()
    existing = await db.projects.find_one({"user_id": user_id, "name": name})
    if existing:
        await update.message.reply_text(f"❌ You already have a project named `{name}`\\. Choose a different name:", parse_mode=ParseMode.MARKDOWN_V2)
        return STATE_CREATE_PROJECT_NAME

    result = await db.projects.insert_one({
        "user_id": user_id,
        "name": name,
        "start_cmd": "",
        "env_vars": {},
        "created_at": datetime.utcnow(),
    })
    project_id = str(result.inserted_id)

    project_path = PROJECTS_DIR / str(user_id) / name
    project_path.mkdir(parents=True, exist_ok=True)

    # Write meta file for file manager
    meta_file = PROJECTS_DIR / f"meta_{project_id}.txt"
    meta_file.write_text(str(project_path))

    log_path = LOGS_DIR / f"{project_id}.log"

    await update.message.reply_text(
        f"✅ Project *{escape_md(name)}* created\\!\n\nNow send me the files to upload, or set a start command\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=project_keyboard(project_id, False),
    )
    context.user_data["current_project_id"] = project_id
    return STATE_MAIN_MENU


async def open_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_"):]
    db = get_db()
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await reply(update, "❌ Project not found.")
        return STATE_MAIN_MENU

    alive = is_process_alive(project_id)
    status = "🟢 Running" if alive else "🔴 Stopped"
    name = escape_md(proj.get("name", project_id))
    cmd = escape_md(proj.get("start_cmd", "Not set"))
    env_count = len(proj.get("env_vars", {}))
    text = (
        f"📁 *{name}*\n"
        f"Status: {status}\n"
        f"Start cmd: `{cmd}`\n"
        f"Env vars: {env_count}"
    )
    await reply(update, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=project_keyboard(project_id, alive))
    context.user_data["current_project_id"] = project_id
    return STATE_MAIN_MENU


async def proj_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_start_"):]
    db = get_db()
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await reply(update, "❌ Project not found.")
        return STATE_MAIN_MENU
    if not proj.get("start_cmd"):
        await reply(update, "❌ No start command set\\. Use *⚙️ Set Start Cmd* first\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=project_keyboard(project_id, False))
        return STATE_MAIN_MENU
    if is_process_alive(project_id):
        await reply(update, "⚠️ Project is already running\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=project_keyboard(project_id, True))
        return STATE_MAIN_MENU

    user_id = proj["user_id"]
    name = proj["name"]
    project_path = PROJECTS_DIR / str(user_id) / name
    log_path = LOGS_DIR / f"{project_id}.log"

    # Inject env vars
    env_vars: dict = proj.get("env_vars", {})
    for key, val in env_vars.items():
        os.environ[key] = val

    try:
        log_handle = open(str(log_path), "a", encoding="utf-8")
        log_handle.write(f"\n{'='*60}\n[{datetime.utcnow().isoformat()}] STARTING: {proj['start_cmd']}\n{'='*60}\n")
        log_handle.flush()

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        for key, val in env_vars.items():
            env[key] = val

        try:
            args = shlex.split(proj["start_cmd"])
        except ValueError:
            args = proj["start_cmd"].split()

        proc = subprocess.Popen(
            args,
            cwd=str(project_path),
            stdout=log_handle,
            stderr=log_handle,
            env=env,
            start_new_session=True,
        )
        set_process(project_id, {"process": proc, "log_file_handle": log_handle, "log_path": log_path})
        await reply(update, f"▶️ *{escape_md(name)}* started \\(PID {proc.pid}\\)\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=project_keyboard(project_id, True))
    except Exception as e:
        await reply(update, f"❌ Failed to start: `{escape_md(str(e))}`", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=project_keyboard(project_id, False))
    return STATE_MAIN_MENU


async def proj_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_stop_"):]
    db = get_db()
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    name = proj.get("name", project_id) if proj else project_id
    if stop_project_process(project_id):
        await reply(update, f"⏹ *{escape_md(name)}* stopped\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=project_keyboard(project_id, False))
    else:
        await reply(update, f"⚠️ *{escape_md(name)}* was not running\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=project_keyboard(project_id, False))
    return STATE_MAIN_MENU


async def proj_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_logs_"):]
    log_path = LOGS_DIR / f"{project_id}.log"
    tail = read_log_tail(log_path, lines=30)
    tail_escaped = escape_md(tail)
    alive = is_process_alive(project_id)
    await reply(
        update,
        f"📋 *Last 30 lines:*\n```\n{tail_escaped}\n```",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data=f"proj_logs_{project_id}"), InlineKeyboardButton("⬅️ Back", callback_data=f"proj_{project_id}")]]),
    )
    return STATE_MAIN_MENU


async def proj_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_upload_"):]
    context.user_data["upload_project_id"] = project_id
    await reply(update, "📤 Send me a file to upload to this project\\. \\(Send any document/file\\)", parse_mode=ParseMode.MARKDOWN_V2)
    return STATE_UPLOAD_FILE


async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    project_id = context.user_data.get("upload_project_id") or context.user_data.get("current_project_id")
    if not project_id:
        await update.message.reply_text("❌ No project selected. Go back and pick a project first.")
        return STATE_MAIN_MENU

    db = get_db()
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await update.message.reply_text("❌ Project not found.")
        return STATE_MAIN_MENU

    document = update.message.document
    if not document:
        await update.message.reply_text("❌ Please send a document file.")
        return STATE_UPLOAD_FILE

    user_id = proj["user_id"]
    name = proj["name"]
    project_path = PROJECTS_DIR / str(user_id) / name
    project_path.mkdir(parents=True, exist_ok=True)

    file_name = secure_filename(document.file_name or "uploaded_file")
    dest_path = project_path / file_name

    tg_file = await document.get_file()
    await tg_file.download_to_drive(str(dest_path))

    await update.message.reply_text(
        f"✅ Uploaded *{escape_md(file_name)}* to *{escape_md(name)}*\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=project_keyboard(project_id, is_process_alive(project_id)),
    )
    context.user_data.pop("upload_project_id", None)
    return STATE_MAIN_MENU


async def proj_fm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate a file manager access token and send the link."""
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_fm_"):]
    user_id = query.from_user.id
    db = get_db()
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await reply(update, "❌ Project not found.")
        return STATE_MAIN_MENU

    # Generate token
    token = uuid.uuid4().hex
    expiry = datetime.utcnow() + timedelta(hours=2)
    TOKENS_DIR.mkdir(exist_ok=True)
    token_file = TOKENS_DIR / f"{token}.txt"
    token_file.write_text(f"{project_id}\n{user_id}\n{expiry.isoformat()}")

    fm_link = f"{FM_URL}/fm/{token}"
    name = escape_md(proj.get("name", project_id))
    text = (
        f"🌐 *File Manager* for *{name}*\n\n"
        f"[Click here to open]({fm_link})\n\n"
        f"⏳ Link expires in 2 hours\\."
    )
    await reply(update, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"proj_{project_id}")]]))
    return STATE_MAIN_MENU


async def proj_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_cmd_"):]
    context.user_data["cmd_project_id"] = project_id
    await reply(update, "⚙️ Send me the *start command* for this project\\.\nExample: `python bot\\.py` or `node index\\.js`", parse_mode=ParseMode.MARKDOWN_V2)
    return STATE_ENTER_START_CMD


async def receive_start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    project_id = context.user_data.get("cmd_project_id")
    if not project_id:
        await update.message.reply_text("❌ No project selected.")
        return STATE_MAIN_MENU

    cmd = update.message.text.strip()
    if not cmd:
        await update.message.reply_text("❌ Empty command. Try again:")
        return STATE_ENTER_START_CMD

    db = get_db()
    await db.projects.update_one({"_id": ObjectId(project_id)}, {"$set": {"start_cmd": cmd}})
    await update.message.reply_text(
        f"✅ Start command set to:\n`{escape_md(cmd)}`",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=project_keyboard(project_id, is_process_alive(project_id)),
    )
    context.user_data.pop("cmd_project_id", None)
    return STATE_MAIN_MENU


async def proj_env(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_env_"):]
    db = get_db()
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    env_vars = proj.get("env_vars", {}) if proj else {}
    if env_vars:
        lines = "\n".join([f"`{escape_md(k)}` \\= `{escape_md(v)}`" for k, v in list(env_vars.items())[:10]])
        text = f"🔑 *Env Vars*:\n{lines}"
    else:
        text = "🔑 *Env Vars*: \\(none set\\)"
    await reply(update, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=env_keyboard(project_id, env_vars))
    context.user_data["env_project_id"] = project_id
    return STATE_MAIN_MENU


async def env_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("env_add_"):]
    context.user_data["env_project_id"] = project_id
    await reply(update, "🔑 Send the *ENV KEY* \\(e\\.g\\. `API_KEY`\\):", parse_mode=ParseMode.MARKDOWN_V2)
    return STATE_ENTER_ENV_KEY


async def receive_env_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = update.message.text.strip().upper()
    if not re.match(r"^[A-Z0-9_]+$", key):
        await update.message.reply_text("❌ Invalid key name. Use letters, numbers, underscores. Try again:")
        return STATE_ENTER_ENV_KEY
    context.user_data["env_key"] = key
    await update.message.reply_text(f"🔑 Now send the *value* for `{escape_md(key)}`:", parse_mode=ParseMode.MARKDOWN_V2)
    return STATE_ENTER_ENV_VALUE


async def receive_env_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    project_id = context.user_data.get("env_project_id")
    key = context.user_data.get("env_key")
    if not project_id or not key:
        await update.message.reply_text("❌ Session expired. Start over.")
        return STATE_MAIN_MENU

    value = update.message.text.strip()
    db = get_db()
    await db.projects.update_one(
        {"_id": ObjectId(project_id)},
        {"$set": {f"env_vars.{key}": value}},
    )
    await update.message.reply_text(
        f"✅ Set `{escape_md(key)}` \\= `{escape_md(value)}`",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=project_keyboard(project_id, is_process_alive(project_id)),
    )
    context.user_data.pop("env_project_id", None)
    context.user_data.pop("env_key", None)
    return STATE_MAIN_MENU


async def env_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # callback_data = env_del_{project_id}_{KEY}
    parts = query.data[len("env_del_"):].split("_", 1)
    if len(parts) != 2:
        return STATE_MAIN_MENU
    project_id, key = parts
    db = get_db()
    await db.projects.update_one(
        {"_id": ObjectId(project_id)},
        {"$unset": {f"env_vars.{key}": ""}},
    )
    await query.answer(f"✅ Deleted {key}")
    # Refresh env view
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    env_vars = proj.get("env_vars", {}) if proj else {}
    if env_vars:
        lines = "\n".join([f"`{escape_md(k)}` \\= `{escape_md(v)}`" for k, v in list(env_vars.items())[:10]])
        text = f"🔑 *Env Vars*:\n{lines}"
    else:
        text = "🔑 *Env Vars*: \\(none set\\)"
    await reply(update, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=env_keyboard(project_id, env_vars))
    return STATE_MAIN_MENU


async def proj_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Backing up…")
    project_id = query.data[len("proj_backup_"):]
    db = get_db()
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await reply(update, "❌ Project not found.")
        return STATE_MAIN_MENU

    user_id = proj["user_id"]
    name = proj["name"]
    project_path = PROJECTS_DIR / str(user_id) / name

    ok = await backup_project_to_gridfs(project_id, project_path)
    if ok:
        await reply(update, f"✅ *{escape_md(name)}* backed up to GridFS\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=project_keyboard(project_id, is_process_alive(project_id)))
    else:
        await reply(update, f"❌ Backup failed for *{escape_md(name)}*\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=project_keyboard(project_id, is_process_alive(project_id)))
    return STATE_MAIN_MENU


async def proj_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_delete_"):]
    db = get_db()
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    name = proj.get("name", project_id) if proj else project_id
    context.user_data["delete_project_id"] = project_id
    await reply(
        update,
        f"⚠️ Are you sure you want to *delete* project *{escape_md(name)}*?\n\nThis will stop the process, delete all files, and remove the backup\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Yes, Delete", callback_data=f"proj_delete_confirmed_{project_id}"), InlineKeyboardButton("❌ Cancel", callback_data=f"proj_{project_id}")]
        ]),
    )
    return STATE_CONFIRM_DELETE_PROJECT


async def proj_delete_confirmed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    project_id = query.data[len("proj_delete_confirmed_"):]
    db = get_db()
    proj = await db.projects.find_one({"_id": ObjectId(project_id)})
    if not proj:
        await reply(update, "❌ Project not found.")
        return STATE_MAIN_MENU

    # Stop process
    stop_project_process(project_id)

    # Delete files
    user_id = proj["user_id"]
    name = proj["name"]
    project_path = PROJECTS_DIR / str(user_id) / name
    if project_path.exists():
        shutil.rmtree(str(project_path), ignore_errors=True)

    # Delete GridFS backup
    bucket = get_gridfs_bucket()
    meta = await db.backup_meta.find_one({"project_id": project_id})
    if meta and meta.get("gridfs_id"):
        try:
            await bucket.delete(meta["gridfs_id"])
        except Exception:
            pass
    await db.backup_meta.delete_one({"project_id": project_id})

    # Delete log
    log_path = LOGS_DIR / f"{project_id}.log"
    log_path.unlink(missing_ok=True)

    # Delete meta file
    meta_file = PROJECTS_DIR / f"meta_{project_id}.txt"
    meta_file.unlink(missing_ok=True)

    # Delete from DB
    await db.projects.delete_one({"_id": ObjectId(project_id)})

    await reply(update, f"✅ Project *{escape_md(name)}* deleted\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ My Projects", callback_data="menu_projects")]]))
    return STATE_MAIN_MENU


async def menu_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    uptime = human_uptime(time.time() - psutil.boot_time())
    text = (
        f"📊 *System Stats*\n\n"
        f"🖥 CPU: `{cpu}%`\n"
        f"🧠 RAM: `{mem.percent}%` \\({human_size(mem.used)}/{human_size(mem.total)}\\)\n"
        f"💾 Disk: `{disk.percent}%` \\({human_size(disk.used)}/{human_size(disk.total)}\\)\n"
        f"⏱ Uptime: `{escape_md(uptime)}`\n"
        f"🐍 Python: `{escape_md(platform.python_version())}`"
    )
    await reply(update, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="menu_stats"), InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]]))
    return STATE_MAIN_MENU


async def menu_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = (
        "❓ *God Madara Hosting Bot — Help*\n\n"
        "📁 *My Projects* — view your hosted projects\n"
        "➕ *New Project* — create a new project slot\n"
        "▶️ *Start* — run your project with the start command\n"
        "⏹ *Stop* — stop a running project\n"
        "📋 *Logs* — see last 30 lines of output\n"
        "📤 *Upload File* — upload files directly via Telegram\n"
        "🌐 *File Manager* — web browser for project files\n"
        "⚙️ *Set Start Cmd* — e\\.g\\. `python bot\\.py`\n"
        "🔑 *Env Vars* — set environment variables\n"
        "💾 *Backup Now* — manual backup to MongoDB GridFS\n"
        "🗑️ *Delete* — remove a project permanently\n\n"
        "Files are auto\\-restored from GridFS on restart\\."
    )
    await reply(update, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]]))
    return STATE_MAIN_MENU


# ═══ ADMIN HANDLERS ══════════════════════════════════════════════════════════


async def menu_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        await reply(update, "🚫 Owner only.")
        return STATE_MAIN_MENU
    await reply(update, "🛡️ *Admin Panel*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=admin_keyboard())
    return STATE_MAIN_MENU


async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return STATE_MAIN_MENU
    db = get_db()
    users = await db.users.find({}).to_list(length=50)
    lines = []
    for u in users:
        banned = "🚫" if u.get("is_banned") else "✅"
        uid = u.get("user_id", "?")
        name = escape_md(u.get("full_name") or u.get("username") or str(uid))
        lines.append(f"{banned} `{uid}` — {name}")
    text = "👥 *Users* \\(max 50\\):\n\n" + "\n".join(lines) if lines else "No users yet\\."
    await reply(update, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu_admin")]]))
    return STATE_MAIN_MENU


async def admin_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return STATE_MAIN_MENU
    await reply(update, "📢 Send the *broadcast message* \\(plain text\\):", parse_mode=ParseMode.MARKDOWN_V2)
    return STATE_ADMIN_BROADCAST


async def admin_broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return STATE_MAIN_MENU
    text = update.message.text.strip()
    db = get_db()
    users = await db.users.find({"is_banned": {"$ne": True}}).to_list(length=None)
    sent = 0
    failed = 0
    for u in users:
        try:
            await context.bot.send_message(u["user_id"], f"📢 *Broadcast:*\n\n{escape_md(text)}", parse_mode=ParseMode.MARKDOWN_V2)
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(f"✅ Broadcast sent to {sent} users\\. Failed: {failed}\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=admin_keyboard())
    return STATE_MAIN_MENU


async def admin_ban_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return STATE_MAIN_MENU
    await reply(update, "🚫 Send the *User ID* to ban:")
    return STATE_ADMIN_BAN


async def admin_ban_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return STATE_MAIN_MENU
    try:
        target_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID. Send a numeric user ID:")
        return STATE_ADMIN_BAN
    db = get_db()
    await db.users.update_one({"user_id": target_id}, {"$set": {"is_banned": True}}, upsert=True)
    await update.message.reply_text(f"✅ User `{target_id}` banned\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=admin_keyboard())
    return STATE_MAIN_MENU


async def admin_unban_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return STATE_MAIN_MENU
    await reply(update, "✅ Send the *User ID* to unban:")
    return STATE_ADMIN_UNBAN


async def admin_unban_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return STATE_MAIN_MENU
    try:
        target_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID. Send a numeric user ID:")
        return STATE_ADMIN_UNBAN
    db = get_db()
    await db.users.update_one({"user_id": target_id}, {"$set": {"is_banned": False}})
    await update.message.reply_text(f"✅ User `{target_id}` unbanned\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=admin_keyboard())
    return STATE_MAIN_MENU


async def admin_stats_full(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return STATE_MAIN_MENU
    db = get_db()
    user_count = await db.users.count_documents({})
    proj_count = await db.projects.count_documents({})
    banned_count = await db.users.count_documents({"is_banned": True})
    running_count = sum(1 for pid in list(_running_processes.keys()) if is_process_alive(pid))
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    text = (
        f"📊 *Full Admin Stats*\n\n"
        f"👥 Users: `{user_count}` \\(banned: `{banned_count}`\\)\n"
        f"📁 Projects: `{proj_count}`\n"
        f"🟢 Running: `{running_count}`\n"
        f"🖥 CPU: `{cpu}%`\n"
        f"🧠 RAM: `{mem.percent}%`"
    )
    await reply(update, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu_admin")]]))
    return STATE_MAIN_MENU


async def admin_backup_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Backing up all projects…")
    if query.from_user.id != OWNER_ID:
        return STATE_MAIN_MENU
    db = get_db()
    projects = await db.projects.find({}).to_list(length=None)
    ok_count = 0
    fail_count = 0
    for proj in projects:
        pid = str(proj["_id"])
        user_id = proj["user_id"]
        name = proj.get("name", pid)
        project_path = PROJECTS_DIR / str(user_id) / name
        if project_path.exists():
            ok = await backup_project_to_gridfs(pid, project_path)
            if ok:
                ok_count += 1
            else:
                fail_count += 1
    await reply(update, f"✅ Backup complete: {ok_count} succeeded, {fail_count} failed\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=admin_keyboard())
    return STATE_MAIN_MENU


# ═══ CONVERSATION HANDLERS ════════════════════════════════════════════════════


def build_app() -> Application:
    app_bot = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            STATE_MAIN_MENU: [
                CallbackQueryHandler(menu_main, pattern="^menu_main$"),
                CallbackQueryHandler(menu_projects, pattern="^menu_projects$"),
                CallbackQueryHandler(menu_new_project, pattern="^menu_new_project$"),
                CallbackQueryHandler(menu_stats, pattern="^menu_stats$"),
                CallbackQueryHandler(menu_help, pattern="^menu_help$"),
                CallbackQueryHandler(menu_admin, pattern="^menu_admin$"),
                # project open
                CallbackQueryHandler(open_project, pattern=r"^proj_[0-9a-f]{24}$"),
                CallbackQueryHandler(proj_start, pattern=r"^proj_start_"),
                CallbackQueryHandler(proj_stop, pattern=r"^proj_stop_"),
                CallbackQueryHandler(proj_logs, pattern=r"^proj_logs_"),
                CallbackQueryHandler(proj_upload, pattern=r"^proj_upload_"),
                CallbackQueryHandler(proj_fm, pattern=r"^proj_fm_"),
                CallbackQueryHandler(proj_cmd, pattern=r"^proj_cmd_"),
                CallbackQueryHandler(proj_env, pattern=r"^proj_env_"),
                CallbackQueryHandler(proj_backup, pattern=r"^proj_backup_"),
                CallbackQueryHandler(proj_delete_confirm, pattern=r"^proj_delete_[0-9a-f]{24}$"),
                CallbackQueryHandler(proj_delete_confirmed, pattern=r"^proj_delete_confirmed_"),
                # env
                CallbackQueryHandler(env_add, pattern=r"^env_add_"),
                CallbackQueryHandler(env_del, pattern=r"^env_del_"),
                # admin
                CallbackQueryHandler(admin_users, pattern="^admin_users$"),
                CallbackQueryHandler(admin_broadcast_start, pattern="^admin_broadcast$"),
                CallbackQueryHandler(admin_ban_start, pattern="^admin_ban$"),
                CallbackQueryHandler(admin_unban_start, pattern="^admin_unban$"),
                CallbackQueryHandler(admin_stats_full, pattern="^admin_stats$"),
                CallbackQueryHandler(admin_backup_all, pattern="^admin_backup_all$"),
            ],
            STATE_CREATE_PROJECT_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_project_name),
            ],
            STATE_UPLOAD_FILE: [
                MessageHandler(filters.Document.ALL, receive_file),
                CallbackQueryHandler(menu_main, pattern="^menu_main$"),
            ],
            STATE_ENTER_START_CMD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_start_cmd),
            ],
            STATE_ADMIN_BROADCAST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_send),
            ],
            STATE_ADMIN_BAN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ban_do),
            ],
            STATE_ADMIN_UNBAN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_unban_do),
            ],
            STATE_ENTER_ENV_KEY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_env_key),
            ],
            STATE_ENTER_ENV_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_env_value),
            ],
            STATE_CONFIRM_DELETE_PROJECT: [
                CallbackQueryHandler(proj_delete_confirmed, pattern=r"^proj_delete_confirmed_"),
                CallbackQueryHandler(open_project, pattern=r"^proj_[0-9a-f]{24}$"),
            ],
        },
        fallbacks=[CommandHandler("start", cmd_start)],
        allow_reentry=True,
        per_message=False,
    )

    app_bot.add_handler(conv_handler)

    # Standalone commands (work outside conversation)
    app_bot.add_handler(CommandHandler("start", cmd_start))

    return app_bot


# ═══ FLASK FILE MANAGER ══════════════════════════════════════════════════════

SUPPORTED_EDITABLE = {
    ".py", ".txt", ".md", ".json", ".yaml", ".yml", ".toml",
    ".cfg", ".ini", ".env", ".sh", ".js", ".ts", ".html",
    ".css", ".xml", ".csv", ".log", ".conf", ".dockerfile",
}


def is_editable(filename: str) -> bool:
    return Path(filename).suffix.lower() in SUPPORTED_EDITABLE


flask_app = Flask(__name__)
flask_app.secret_key = FM_SECRET


# ─── Token helpers ────────────────────────────────────────────────────────────

def validate_token(token: str) -> dict | None:
    """Returns {project_id, user_id} or None if invalid/expired."""
    token = re.sub(r"[^a-f0-9]", "", token)
    token_file = TOKENS_DIR / f"{token}.txt"
    if not token_file.exists():
        return None
    try:
        lines = token_file.read_text().strip().splitlines()
        project_id = lines[0]
        user_id = int(lines[1])
        expiry = datetime.fromisoformat(lines[2])
        if datetime.utcnow() > expiry:
            token_file.unlink(missing_ok=True)
            return None
        return {"project_id": project_id, "user_id": user_id}
    except Exception:
        return None


def safe_path(base: Path, rel: str) -> Path | None:
    """Resolve a relative path safely, preventing directory traversal."""
    try:
        resolved = (base / rel).resolve()
        if not str(resolved).startswith(str(base.resolve())):
            return None
        return resolved
    except Exception:
        return None


def token_expiry_str(token: str) -> str:
    token_file = TOKENS_DIR / f"{token}.txt"
    if not token_file.exists():
        return "unknown"
    try:
        lines = token_file.read_text().strip().splitlines()
        expiry = datetime.fromisoformat(lines[2])
        remaining = expiry - datetime.utcnow()
        mins = int(remaining.total_seconds() / 60)
        return f"in {mins} min" if mins > 0 else "expired"
    except Exception:
        return "unknown"


def get_project_name(base_path: Path) -> str:
    return base_path.name


# ─── Route helpers ────────────────────────────────────────────────────────────

def get_token_and_base(token: str):
    """Returns (token_data, base_path) or aborts 403/404."""
    data = validate_token(token)
    if not data:
        abort(403)
    meta_file = PROJECTS_DIR / f"meta_{data['project_id']}.txt"
    if meta_file.exists():
        base_path = Path(meta_file.read_text().strip())
    else:
        user_dir = PROJECTS_DIR / str(data["user_id"])
        if not user_dir.exists():
            abort(404)
        candidates = [d for d in user_dir.iterdir() if d.is_dir()]
        if not candidates:
            abort(404)
        base_path = candidates[0]
    if not base_path.exists():
        abort(404)
    return data, base_path


def build_breadcrumbs(rel: str) -> list[dict]:
    if not rel:
        return []
    parts = Path(rel).parts
    crumbs = []
    for i, part in enumerate(parts):
        crumbs.append({"name": part, "rel": "/".join(parts[: i + 1])})
    return crumbs


def list_dir(base: Path, rel: str) -> list[dict]:
    target = safe_path(base, rel) if rel else base
    if not target or not target.is_dir():
        return []
    items = []
    for entry in sorted(target.iterdir(), key=lambda e: (not e.is_dir(), e.name.lower())):
        entry_rel = str(entry.relative_to(base))
        stat = entry.stat()
        items.append({
            "name": entry.name,
            "rel": entry_rel,
            "is_dir": entry.is_dir(),
            "size": human_size(stat.st_size) if not entry.is_dir() else "—",
            "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
            "editable": is_editable(entry.name) if not entry.is_dir() else False,
        })
    return items


# ─── Dark-themed HTML template ────────────────────────────────────────────────

BASE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>📂 God Madara File Manager</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
<style>
  :root {
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #c9d1d9; --muted: #8b949e; --accent: #58a6ff;
    --danger: #f85149; --success: #3fb950; --warning: #d29922;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 14px 24px; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 1.2rem; color: var(--accent); }
  header span { color: var(--muted); font-size: .85rem; }
  .container { max-width: 1100px; margin: 0 auto; padding: 20px; }
  .breadcrumb { display: flex; flex-wrap: wrap; gap: 4px; align-items: center; margin-bottom: 16px; font-size: .9rem; }
  .breadcrumb a { color: var(--accent); text-decoration: none; } .breadcrumb a:hover { text-decoration: underline; }
  .breadcrumb span { color: var(--muted); }
  .toolbar { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
  .btn { padding: 7px 14px; border: none; border-radius: 6px; cursor: pointer; font-size: .85rem; font-weight: 600; text-decoration: none; display: inline-flex; align-items: center; gap: 6px; transition: opacity .15s; }
  .btn:hover { opacity: .85; }
  .btn-primary { background: var(--accent); color: #0d1117; }
  .btn-danger  { background: var(--danger); color: #fff; }
  .btn-success { background: var(--success); color: #0d1117; }
  .btn-muted   { background: var(--border); color: var(--text); }
  .file-table { width: 100%; border-collapse: collapse; background: var(--surface); border-radius: 8px; overflow: hidden; }
  .file-table th { background: #1c2128; padding: 10px 14px; text-align: left; font-size: .8rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid var(--border); }
  .file-table td { padding: 10px 14px; border-bottom: 1px solid var(--border); font-size: .9rem; }
  .file-table tr:last-child td { border-bottom: none; }
  .file-table tr:hover td { background: #1c2128; }
  .file-icon { margin-right: 6px; }
  .file-link { color: var(--text); text-decoration: none; } .file-link:hover { color: var(--accent); }
  .actions { display: flex; gap: 6px; }
  .actions a, .actions button { font-size: .78rem; padding: 3px 9px; }
  .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.7); z-index: 1000; align-items: center; justify-content: center; }
  .modal-overlay.active { display: flex; }
  .modal { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 24px; width: min(500px, 95vw); }
  .modal h3 { margin-bottom: 16px; color: var(--accent); }
  .form-group { margin-bottom: 14px; }
  label { display: block; font-size: .85rem; margin-bottom: 5px; color: var(--muted); }
  input[type=text], textarea, select { width: 100%; padding: 8px 10px; background: var(--bg); border: 1px solid var(--border); color: var(--text); border-radius: 6px; font-size: .9rem; }
  textarea { min-height: 120px; resize: vertical; }
  .editor-area { background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 0; overflow: hidden; }
  .editor-header { background: var(--surface); padding: 10px 16px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--border); font-size: .85rem; }
  #code-editor { width: 100%; min-height: 60vh; padding: 16px; background: var(--bg); color: var(--text); border: none; outline: none; font-family: 'JetBrains Mono', 'Fira Code', monospace; font-size: .88rem; resize: none; tab-size: 4; }
  .flash { padding: 10px 14px; border-radius: 6px; margin-bottom: 12px; font-size: .9rem; }
  .flash.error   { background: #2d1216; border: 1px solid var(--danger); color: var(--danger); }
  .flash.success { background: #0d2818; border: 1px solid var(--success); color: var(--success); }
  .empty-dir { text-align: center; padding: 40px; color: var(--muted); }
  @media (max-width: 600px) { .file-table th:nth-child(3), .file-table td:nth-child(3) { display: none; } }
</style>
</head>
<body>
<header>
  <div>
    <h1>📂 God Madara File Manager</h1>
    <span>Project: {{ project_name }} &nbsp;|&nbsp; Session expires: {{ expiry }}</span>
  </div>
</header>
<div class="container">
{% with messages = get_flashed_messages(with_categories=true) %}
  {% for category, message in messages %}
    <div class="flash {{ category }}">{{ message }}</div>
  {% endfor %}
{% endwith %}
{% block content %}{% endblock %}
</div>
{% block modals %}{% endblock %}
{% block scripts %}{% endblock %}
</body>
</html>"""

BROWSER_TEMPLATE = BASE_TEMPLATE.replace("{% block content %}{% endblock %}", """
{% block content %}
<div class="breadcrumb">
  <a href="{{ url_for('browse', token=token, rel='') }}">🏠 Root</a>
  {% for crumb in breadcrumbs %}
    <span>/</span>
    <a href="{{ url_for('browse', token=token, rel=crumb.rel) }}">{{ crumb.name }}</a>
  {% endfor %}
</div>
<div class="toolbar">
  <button class="btn btn-primary" onclick="showModal('modal-newfile')">➕ New File</button>
  <button class="btn btn-primary" onclick="showModal('modal-newfolder')">📁 New Folder</button>
  <label class="btn btn-muted" style="cursor:pointer">
    📤 Upload <input type="file" multiple hidden onchange="uploadFiles(this)">
  </label>
</div>
{% if files %}
<table class="file-table">
  <thead><tr><th>Name</th><th>Size</th><th>Modified</th><th>Actions</th></tr></thead>
  <tbody>
  {% for f in files %}
  <tr>
    <td>
      <span class="file-icon">{{ '📁' if f.is_dir else '📄' }}</span>
      {% if f.is_dir %}
        <a class="file-link" href="{{ url_for('browse', token=token, rel=f.rel) }}">{{ f.name }}</a>
      {% elif f.editable %}
        <a class="file-link" href="{{ url_for('edit_file', token=token, rel=f.rel) }}">{{ f.name }}</a>
      {% else %}
        <a class="file-link" href="{{ url_for('download_file', token=token, rel=f.rel) }}" download>{{ f.name }}</a>
      {% endif %}
    </td>
    <td>{{ f.size }}</td>
    <td>{{ f.mtime }}</td>
    <td>
      <div class="actions">
        {% if not f.is_dir and f.editable %}
          <a class="btn btn-muted" href="{{ url_for('edit_file', token=token, rel=f.rel) }}">✏️</a>
        {% endif %}
        {% if not f.is_dir %}
          <a class="btn btn-muted" href="{{ url_for('download_file', token=token, rel=f.rel) }}" download>⬇️</a>
        {% endif %}
        <button class="btn btn-muted" onclick="renameItem('{{ f.rel }}', '{{ f.name }}')">🔤</button>
        <button class="btn btn-danger" onclick="deleteItem('{{ f.rel }}', {{ 'true' if f.is_dir else 'false' }})">🗑️</button>
      </div>
    </td>
  </tr>
  {% endfor %}
  </tbody>
</table>
{% else %}
<div class="empty-dir">📭 This directory is empty.</div>
{% endif %}
{% endblock %}""").replace("{% block modals %}{% endblock %}", """
{% block modals %}
<!-- New File Modal -->
<div class="modal-overlay" id="modal-newfile">
  <div class="modal">
    <h3>➕ Create New File</h3>
    <form method="POST" action="{{ url_for('create_file', token=token) }}">
      <input type="hidden" name="dir" value="{{ current_rel }}">
      <div class="form-group">
        <label>Filename</label>
        <input type="text" name="filename" placeholder="main.py" required autofocus>
      </div>
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button type="button" class="btn btn-muted" onclick="hideModal('modal-newfile')">Cancel</button>
        <button type="submit" class="btn btn-primary">Create</button>
      </div>
    </form>
  </div>
</div>
<!-- New Folder Modal -->
<div class="modal-overlay" id="modal-newfolder">
  <div class="modal">
    <h3>📁 Create New Folder</h3>
    <form method="POST" action="{{ url_for('create_folder', token=token) }}">
      <input type="hidden" name="dir" value="{{ current_rel }}">
      <div class="form-group">
        <label>Folder Name</label>
        <input type="text" name="foldername" placeholder="my_folder" required autofocus>
      </div>
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button type="button" class="btn btn-muted" onclick="hideModal('modal-newfolder')">Cancel</button>
        <button type="submit" class="btn btn-primary">Create</button>
      </div>
    </form>
  </div>
</div>
<!-- Rename Modal -->
<div class="modal-overlay" id="modal-rename">
  <div class="modal">
    <h3>🔤 Rename</h3>
    <form method="POST" action="{{ url_for('rename_item', token=token) }}">
      <input type="hidden" name="rel" id="rename-rel">
      <div class="form-group">
        <label>New Name</label>
        <input type="text" name="newname" id="rename-newname" required autofocus>
      </div>
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button type="button" class="btn btn-muted" onclick="hideModal('modal-rename')">Cancel</button>
        <button type="submit" class="btn btn-primary">Rename</button>
      </div>
    </form>
  </div>
</div>
<!-- Delete confirm Modal -->
<div class="modal-overlay" id="modal-delete">
  <div class="modal">
    <h3>🗑️ Confirm Delete</h3>
    <p style="margin-bottom:16px;color:var(--muted)">Are you sure you want to delete <strong id="delete-name"></strong>?</p>
    <form method="POST" action="{{ url_for('delete_item', token=token) }}">
      <input type="hidden" name="rel" id="delete-rel">
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button type="button" class="btn btn-muted" onclick="hideModal('modal-delete')">Cancel</button>
        <button type="submit" class="btn btn-danger">Delete</button>
      </div>
    </form>
  </div>
</div>
{% endblock %}""").replace("{% block scripts %}{% endblock %}", """
{% block scripts %}
<script>
function showModal(id) { document.getElementById(id).classList.add('active'); }
function hideModal(id) { document.getElementById(id).classList.remove('active'); }
function renameItem(rel, name) {
  document.getElementById('rename-rel').value = rel;
  document.getElementById('rename-newname').value = name;
  showModal('modal-rename');
}
function deleteItem(rel, isDir) {
  document.getElementById('delete-rel').value = rel;
  document.getElementById('delete-name').textContent = rel.split('/').pop();
  showModal('modal-delete');
}
function uploadFiles(input) {
  const files = Array.from(input.files);
  if (!files.length) return;
  const fd = new FormData();
  files.forEach(f => fd.append('files', f));
  fd.append('dir', '{{ current_rel }}');
  fetch('{{ url_for("upload_files", token=token) }}', { method: 'POST', body: fd })
    .then(r => r.json())
    .then(d => { if (d.ok) location.reload(); else alert('Upload failed: ' + d.error); });
}
document.querySelectorAll('.modal-overlay').forEach(el => {
  el.addEventListener('click', e => { if (e.target === el) el.classList.remove('active'); });
});
</script>
{% endblock %}""")

EDITOR_TEMPLATE = BASE_TEMPLATE.replace("{% block content %}{% endblock %}", """
{% block content %}
<div class="breadcrumb">
  <a href="{{ url_for('browse', token=token, rel='') }}">🏠 Root</a>
  <span>/</span>
  <span>{{ rel }}</span>
</div>
<div class="editor-area">
  <div class="editor-header">
    <span>✏️ Editing: <strong>{{ filename }}</strong></span>
    <div style="display:flex;gap:8px">
      <a class="btn btn-muted" href="{{ url_for('browse', token=token, rel=parent_rel) }}">⬅️ Back</a>
      <button class="btn btn-success" onclick="saveFile()">💾 Save</button>
    </div>
  </div>
  <textarea id="code-editor" spellcheck="false">{{ content }}</textarea>
</div>
{% endblock %}
{% block scripts %}
<script>
function saveFile() {
  const content = document.getElementById('code-editor').value;
  fetch('{{ url_for("save_file", token=token) }}', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ rel: '{{ rel }}', content: content })
  }).then(r => r.json()).then(d => {
    if (d.ok) {
      const btn = document.querySelector('.btn-success');
      btn.textContent = '✅ Saved!';
      setTimeout(() => { btn.textContent = '💾 Save'; }, 2000);
    } else { alert('Save failed: ' + d.error); }
  });
}
document.getElementById('code-editor').addEventListener('keydown', function(e) {
  if (e.key === 'Tab') {
    e.preventDefault();
    const s = this.selectionStart, end = this.selectionEnd;
    this.value = this.value.substring(0, s) + '    ' + this.value.substring(end);
    this.selectionStart = this.selectionEnd = s + 4;
  }
});
</script>
{% endblock %}""")


# ─── Flask Routes ─────────────────────────────────────────────────────────────

@flask_app.route("/")
def index():
    return jsonify({"status": "ok", "service": "God Madara Hosting Bot", "version": "1.0.0"})


@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "God Madara Hosting Bot"})


@flask_app.route("/fm/<token>")
@flask_app.route("/fm/<token>/browse")
def browse(token):
    rel = request.args.get("rel", "")
    _, base = get_token_and_base(token)
    files = list_dir(base, rel)
    return render_template_string(
        BROWSER_TEMPLATE,
        token=token,
        files=files,
        breadcrumbs=build_breadcrumbs(rel),
        current_rel=rel,
        project_name=get_project_name(base),
        expiry=token_expiry_str(token),
    )


@flask_app.route("/fm/<token>/edit")
def edit_file(token):
    rel = request.args.get("rel", "")
    _, base = get_token_and_base(token)
    target = safe_path(base, rel)
    if not target or not target.is_file():
        flash("File not found.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    try:
        content = target.read_text(errors="replace")
    except Exception:
        flash("Cannot read file.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    parent_rel = str(Path(rel).parent) if rel else ""
    if parent_rel == ".":
        parent_rel = ""
    return render_template_string(
        EDITOR_TEMPLATE,
        token=token,
        rel=rel,
        filename=target.name,
        content=content,
        parent_rel=parent_rel,
        project_name=get_project_name(base),
        expiry=token_expiry_str(token),
    )


@flask_app.route("/fm/<token>/save", methods=["POST"])
def save_file(token):
    _, base = get_token_and_base(token)
    data = request.get_json()
    if not data:
        return jsonify({"ok": False, "error": "No data"})
    rel = data.get("rel", "")
    content = data.get("content", "")
    target = safe_path(base, rel)
    if not target:
        return jsonify({"ok": False, "error": "Invalid path"})
    try:
        target.write_text(content, encoding="utf-8")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@flask_app.route("/fm/<token>/download")
def download_file(token):
    rel = request.args.get("rel", "")
    _, base = get_token_and_base(token)
    target = safe_path(base, rel)
    if not target or not target.is_file():
        abort(404)
    return send_from_directory(str(target.parent), target.name, as_attachment=True)


@flask_app.route("/fm/<token>/create_file", methods=["POST"])
def create_file(token):
    _, base = get_token_and_base(token)
    dir_rel = request.form.get("dir", "")
    filename = secure_filename(request.form.get("filename", "").strip())
    if not filename:
        flash("Invalid filename.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    target_dir = safe_path(base, dir_rel) if dir_rel else base
    if not target_dir:
        flash("Invalid path.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    new_file = target_dir / filename
    if new_file.exists():
        flash(f"File '{filename}' already exists.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    new_file.touch()
    flash(f"✅ Created '{filename}'", "success")
    new_rel = str(new_file.relative_to(base))
    return redirect(url_for("edit_file", token=token, rel=new_rel))


@flask_app.route("/fm/<token>/create_folder", methods=["POST"])
def create_folder(token):
    _, base = get_token_and_base(token)
    dir_rel = request.form.get("dir", "")
    foldername = secure_filename(request.form.get("foldername", "").strip())
    if not foldername:
        flash("Invalid folder name.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    target_dir = safe_path(base, dir_rel) if dir_rel else base
    if not target_dir:
        flash("Invalid path.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    new_dir = target_dir / foldername
    if new_dir.exists():
        flash(f"Folder '{foldername}' already exists.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    new_dir.mkdir(parents=True)
    flash(f"✅ Folder '{foldername}' created", "success")
    return redirect(url_for("browse", token=token, rel=dir_rel))


@flask_app.route("/fm/<token>/rename", methods=["POST"])
def rename_item(token):
    _, base = get_token_and_base(token)
    rel = request.form.get("rel", "")
    newname = secure_filename(request.form.get("newname", "").strip())
    if not newname or not rel:
        flash("Invalid rename request.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    target = safe_path(base, rel)
    if not target or not target.exists():
        flash("Item not found.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    new_target = target.parent / newname
    if new_target.exists():
        flash(f"'{newname}' already exists.", "error")
        parent_rel = str(target.parent.relative_to(base)) if target.parent != base else ""
        return redirect(url_for("browse", token=token, rel=parent_rel))
    target.rename(new_target)
    flash(f"✅ Renamed to '{newname}'", "success")
    parent_rel = str(new_target.parent.relative_to(base)) if new_target.parent != base else ""
    return redirect(url_for("browse", token=token, rel=parent_rel))


@flask_app.route("/fm/<token>/delete", methods=["POST"])
def delete_item(token):
    _, base = get_token_and_base(token)
    rel = request.form.get("rel", "")
    if not rel:
        flash("Cannot delete root.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    target = safe_path(base, rel)
    if not target or not target.exists():
        flash("Item not found.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    parent_rel = str(target.parent.relative_to(base)) if target.parent != base else ""
    try:
        if target.is_dir():
            shutil.rmtree(str(target))
        else:
            target.unlink()
        flash(f"✅ Deleted '{target.name}'", "success")
    except Exception as e:
        flash(f"Delete failed: {e}", "error")
    return redirect(url_for("browse", token=token, rel=parent_rel))


@flask_app.route("/fm/<token>/upload", methods=["POST"])
def upload_files(token):
    _, base = get_token_and_base(token)
    dir_rel = request.form.get("dir", "")
    files = request.files.getlist("files")
    if not files:
        return jsonify({"ok": False, "error": "No files"})
    target_dir = safe_path(base, dir_rel) if dir_rel else base
    if not target_dir:
        return jsonify({"ok": False, "error": "Invalid directory"})
    for f in files:
        fname = secure_filename(f.filename)
        if fname:
            f.save(str(target_dir / fname))
    return jsonify({"ok": True, "count": len(files)})


# ═══ APPLICATION SETUP & post_init ═══════════════════════════════════════════


async def post_init(application: Application):
    """Called after the bot application is initialized."""
    await ensure_indexes()
    await restore_all_projects_on_startup()
    # Schedule periodic backup watcher
    asyncio.create_task(periodic_backup_watcher(application))
    # Set bot commands
    await application.bot.set_my_commands([
        BotCommand("start", "🚀 Open main menu"),
    ])
    logger.info("✅ Bot post_init complete")


# ═══ ENTRY POINT (threading: Flask + Bot together) ═══════════════════════════


def run_flask():
    """Run Flask file manager in a background daemon thread."""
    logger.info("🌐 Starting Flask file manager on port %s", FM_PORT)
    flask_app.run(
        host="0.0.0.0",
        port=FM_PORT,
        debug=False,
        use_reloader=False,
        threaded=True,
    )


def main():
    # ── Start Flask in a background daemon thread ──────────────────────────
    fm_thread = threading.Thread(target=run_flask, daemon=True, name="flask-fm")
    fm_thread.start()
    logger.info("🌐 File Manager thread started on port %s", FM_PORT)

    # ── Build and run Telegram bot in the main thread ──────────────────────
    app_bot = build_app()
    app_bot.post_init = post_init

    logger.info("🚀 God Madara Hosting Bot starting…")
    app_bot.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
