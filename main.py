import asyncio
import datetime
import logging
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
import threading
import zipfile
from pathlib import Path
from typing import Optional

import psutil
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import (
    Bot, InlineKeyboardButton, InlineKeyboardMarkup,
    Update, InputFile
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, ConversationHandler, MessageHandler, filters
)
from telegram.error import BadRequest

# --- Load .env ---
load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "owner")
MONGODB_URI = os.environ["MONGODB_URI"]
DATABASE_NAME = os.environ.get("DATABASE_NAME", "god_madara_hosting")
PORT = int(os.environ.get("PORT", 8080))
PROJECTS_DIR = os.path.join(os.getcwd(), "projects")
os.makedirs(PROJECTS_DIR, exist_ok=True)

FREE_PROJECT_LIMIT = 1
PREMIUM_PROJECT_LIMIT = 10
FILE_MANAGER_EXPIRY_MINUTES = 10

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# --- MongoDB ---
mongo_client = AsyncIOMotorClient(MONGODB_URI)
db = mongo_client[DATABASE_NAME]
users_col = db["users"]
projects_col = db["projects"]
tokens_col = db["file_tokens"]

# --- Bot start time ---
BOT_START_TIME = time.time()

# --- Running processes: {f"{user_id}:{project_name}": subprocess.Popen} ---
running_processes: dict[str, subprocess.Popen] = {}

# --- Conversation states ---
(
    NEW_PROJECT_NAME,
    NEW_PROJECT_FILES,
    EDIT_RUN_CMD,
    ADMIN_GIVE_PREMIUM,
    ADMIN_REMOVE_PREMIUM,
    ADMIN_TEMP_PREMIUM_ID,
    ADMIN_TEMP_PREMIUM_DURATION,
    ADMIN_BAN_USER,
    ADMIN_UNBAN_USER,
    ADMIN_BROADCAST_MSG,
    ADMIN_SEND_USER_ID,
    ADMIN_SEND_USER_MSG,
    DELETE_CONFIRM,
) = range(13)

# ─────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────

def project_key(user_id: int, project_name: str) -> str:
    return f"{user_id}:{project_name}"


def project_dir(user_id: int, project_name: str) -> str:
    return os.path.join(PROJECTS_DIR, str(user_id), project_name)


def venv_python(user_id: int, project_name: str) -> str:
    return os.path.join(project_dir(user_id, project_name), "venv", "bin", "python3")


def venv_pip(user_id: int, project_name: str) -> str:
    return os.path.join(project_dir(user_id, project_name), "venv", "bin", "pip")


def log_file(user_id: int, project_name: str) -> str:
    return os.path.join(project_dir(user_id, project_name), "output.log")


def format_uptime(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    elif seconds < 86400:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}m"
    else:
        d = seconds // 86400
        h = (seconds % 86400) // 3600
        return f"{d}d {h}h"


def format_bytes(b: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


async def ensure_user(update: Update) -> dict:
    """Upsert user in DB and return user document."""
    user = update.effective_user
    doc = await users_col.find_one({"user_id": user.id})
    if not doc:
        doc = {
            "user_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name or "",
            "is_premium": False,
            "premium_expiry": None,
            "is_banned": False,
            "joined_date": datetime.datetime.utcnow(),
        }
        await users_col.insert_one(doc)
    else:
        await users_col.update_one(
            {"user_id": user.id},
            {"$set": {"username": user.username or "", "first_name": user.first_name or ""}},
        )
    return doc


async def get_user(user_id: int) -> Optional[dict]:
    return await users_col.find_one({"user_id": user_id})


async def check_premium_expiry(user_id: int) -> bool:
    """Returns True if user is currently premium (also handles expiry cleanup)."""
    doc = await users_col.find_one({"user_id": user_id})
    if not doc:
        return False
    if not doc.get("is_premium"):
        return False
    expiry = doc.get("premium_expiry")
    if expiry and expiry < datetime.datetime.utcnow():
        await users_col.update_one(
            {"user_id": user_id},
            {"$set": {"is_premium": False, "premium_expiry": None}},
        )
        return False
    return True


async def get_project_count(user_id: int) -> int:
    return await projects_col.count_documents({"user_id": user_id})


async def is_banned(user_id: int) -> bool:
    doc = await users_col.find_one({"user_id": user_id})
    return bool(doc and doc.get("is_banned"))


async def banned_reply(update: Update):
    text = "🚫 You are banned from using this bot.\nContact the owner for support."
    if update.message:
        await update.message.reply_text(text)
    elif update.callback_query:
        await update.callback_query.answer(text, show_alert=True)


async def get_project(user_id: int, project_name: str) -> Optional[dict]:
    return await projects_col.find_one({"user_id": user_id, "name": project_name})


def get_process_uptime(proc_key: str) -> Optional[float]:
    """Get uptime in seconds for a running process."""
    proc = running_processes.get(proc_key)
    if proc is None:
        return None
    try:
        p = psutil.Process(proc.pid)
        return time.time() - p.create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def is_process_running(proc_key: str) -> bool:
    proc = running_processes.get(proc_key)
    if proc is None:
        return False
    return proc.poll() is None


def safe_send_message_sync(bot: Bot, chat_id: int, text: str):
    """Send a Telegram message synchronously (for use in threads)."""
    asyncio.run(bot.send_message(chat_id=chat_id, text=text))


# ─────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────

def main_menu_keyboard(is_owner: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("🆕 New Project", callback_data="new_project"),
            InlineKeyboardButton("📂 My Projects", callback_data="my_projects"),
        ],
        [
            InlineKeyboardButton("💎 Premium", callback_data="premium"),
            InlineKeyboardButton("📊 Bot Status", callback_data="bot_status"),
        ],
    ]
    if is_owner:
        rows.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(rows)


def back_keyboard(data: str = "start") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=data)]])


def project_dashboard_keyboard(is_running: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("▶️ Run", callback_data="proj_run"),
            InlineKeyboardButton("🔄 Restart", callback_data="proj_restart"),
            InlineKeyboardButton("📋 Logs", callback_data="proj_logs"),
        ],
        [
            InlineKeyboardButton("🔃 Refresh", callback_data="proj_refresh"),
            InlineKeyboardButton("✏️ Edit Run CMD", callback_data="proj_edit_cmd"),
            InlineKeyboardButton("📁 File Manager", callback_data="proj_file_manager"),
        ],
        [
            InlineKeyboardButton("🗑 Delete Project", callback_data="proj_delete"),
            InlineKeyboardButton("🔙 Back", callback_data="my_projects"),
        ],
    ])


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👥 User List", callback_data="admin_users"),
            InlineKeyboardButton("🟢 Running Scripts", callback_data="admin_running"),
        ],
        [
            InlineKeyboardButton("💎 Give Premium", callback_data="admin_give_premium"),
            InlineKeyboardButton("❌ Remove Premium", callback_data="admin_remove_premium"),
        ],
        [
            InlineKeyboardButton("⏰ Temp Premium", callback_data="admin_temp_premium"),
            InlineKeyboardButton("🚫 Ban User", callback_data="admin_ban"),
        ],
        [
            InlineKeyboardButton("✅ Unban User", callback_data="admin_unban"),
            InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="start")],
    ])


# ─────────────────────────────────────────
# START COMMAND
# ─────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    doc = await ensure_user(update)
    if doc.get("is_banned"):
        await banned_reply(update)
        return

    await check_premium_expiry(user.id)
    doc = await get_user(user.id)
    is_premium = doc.get("is_premium", False)
    project_count = await get_project_count(user.id)
    limit = PREMIUM_PROJECT_LIMIT if is_premium else FREE_PROJECT_LIMIT
    plan_label = "Premium ✨" if is_premium else "Free"
    is_owner = user.id == OWNER_ID

    text = (
        f"🌟 *Welcome to God Madara Hosting Bot!*\n\n"
        f"👋 Hello {user.first_name}!\n\n"
        f"🚀 *What I can do:*\n"
        f"• Host Python projects 24/7\n"
        f"• Web File Manager — Edit files in browser\n"
        f"• Auto-install requirements.txt\n"
        f"• Real-time logs & monitoring\n"
        f"• Free: 1 project | Premium: 10 projects\n\n"
        f"📊 *Your Status:*\n"
        f"👤 ID: `{user.id}`\n"
        f"💎 Plan: {plan_label}\n"
        f"📁 Projects: {project_count}/{limit}\n\n"
        f"Choose an option below:"
    )

    keyboard = main_menu_keyboard(is_owner)
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
    elif update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
        except BadRequest:
            await update.callback_query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


# ─────────────────────────────────────────
# MY PROJECTS
# ─────────────────────────────────────────

async def show_my_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    query = update.callback_query
    if query:
        await query.answer()

    if await is_banned(user.id):
        await banned_reply(update)
        return

    await check_premium_expiry(user.id)
    projects = await projects_col.find({"user_id": user.id}).to_list(50)

    if not projects:
        text = "📂 *My Projects*\n\nYou have no projects yet.\nClick 🆕 New Project to get started!"
        kb = back_keyboard("start")
    else:
        text = "📂 *My Projects*\n\nSelect a project to manage:"
        buttons = []
        for p in projects:
            proc_key = project_key(user.id, p["name"])
            status = "🟢" if is_process_running(proc_key) else "🔴"
            buttons.append([InlineKeyboardButton(f"{status} {p['name']}", callback_data=f"project:{p['name']}")])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="start")])
        kb = InlineKeyboardMarkup(buttons)

    if query:
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        except BadRequest:
            await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


async def show_project_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the dashboard for a specific project."""
    query = update.callback_query
    user = update.effective_user
    if query:
        await query.answer()

    if await is_banned(user.id):
        await banned_reply(update)
        return

    project_name = context.user_data.get("current_project")
    if not project_name:
        if query and ":" in query.data:
            project_name = query.data.split(":", 1)[1]
            context.user_data["current_project"] = project_name
        else:
            await query.edit_message_text("❌ Project not found.", reply_markup=back_keyboard("my_projects"))
            return

    proj = await get_project(user.id, project_name)
    if not proj:
        text = "❌ Project not found."
        if query:
            await query.edit_message_text(text, reply_markup=back_keyboard("my_projects"))
        return

    proc_key = project_key(user.id, project_name)
    running = is_process_running(proc_key)
    uptime = get_process_uptime(proc_key)

    status_str = "🟢 Running" if running else "🔴 Stopped"
    pid_str = str(running_processes[proc_key].pid) if running and proc_key in running_processes else "N/A"
    uptime_str = format_uptime(uptime) if uptime is not None else "N/A"
    last_run = proj.get("last_run")
    last_run_str = last_run.strftime("%Y-%m-%d %H:%M UTC") if last_run else "Never"
    exit_code = proj.get("exit_code")
    exit_code_str = str(exit_code) if exit_code is not None else "None"
    run_cmd = proj.get("run_command", "python3 main.py")
    created = proj.get("created_date", datetime.datetime.utcnow())
    created_str = created.strftime("%Y-%m-%d")

    text = (
        f"📊 *Project: {project_name}*\n\n"
        f"🔹 Status: {status_str}\n"
        f"🔹 PID: `{pid_str}`\n"
        f"🔹 Uptime: {uptime_str}\n"
        f"🔹 Last Run: {last_run_str}\n"
        f"🔹 Exit Code: {exit_code_str}\n"
        f"🔹 Run Command: `{run_cmd}`\n"
        f"📅 Created: {created_str}"
    )

    if query:
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                          reply_markup=project_dashboard_keyboard(running))
        except BadRequest:
            await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN,
                                           reply_markup=project_dashboard_keyboard(running))
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN,
                                        reply_markup=project_dashboard_keyboard(running))


# ─────────────────────────────────────────
# NEW PROJECT CONVERSATION
# ─────────────────────────────────────────

async def new_project_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    if query:
        await query.answer()

    if await is_banned(user.id):
        await banned_reply(update)
        return ConversationHandler.END

    await check_premium_expiry(user.id)
    is_premium = await check_premium_expiry(user.id)
    project_count = await get_project_count(user.id)
    limit = PREMIUM_PROJECT_LIMIT if is_premium else FREE_PROJECT_LIMIT

    if project_count >= limit:
        text = (
            f"❌ *Project Limit Reached!*\n\n"
            f"You have {project_count}/{limit} projects.\n\n"
            f"{'Upgrade to Premium to host up to 10 projects!' if not is_premium else 'Premium limit reached.'}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💎 Get Premium", callback_data="premium")],
            [InlineKeyboardButton("🔙 Back", callback_data="start")],
        ])
        if query:
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        else:
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return ConversationHandler.END

    context.user_data["new_project_files"] = []
    text = (
        "🆕 *New Project*\n\n"
        "📝 Enter a project name:\n"
        "• Alphanumeric characters only\n"
        "• No spaces (use underscores)\n"
        "• Maximum 20 characters\n\n"
        "Example: `my_bot` or `weather_scraper`"
    )
    kb = back_keyboard("start")
    if query:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    return NEW_PROJECT_NAME


async def new_project_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    name = update.message.text.strip()

    if not re.match(r"^[a-zA-Z0-9_]{1,20}$", name):
        await update.message.reply_text(
            "❌ Invalid name. Use only letters, numbers, underscores (max 20 chars). Try again:"
        )
        return NEW_PROJECT_NAME

    existing = await get_project(user.id, name)
    if existing:
        await update.message.reply_text(
            f"❌ You already have a project named `{name}`. Choose a different name:",
            parse_mode=ParseMode.MARKDOWN,
        )
        return NEW_PROJECT_NAME

    context.user_data["new_project_name"] = name
    context.user_data["new_project_files"] = []

    text = (
        f"✅ Project name: `{name}`\n\n"
        f"📁 *Upload your files:*\n"
        f"• Send files one by one, OR\n"
        f"• Send a single `.zip` file (auto-extracted)\n"
        f"• Include `requirements.txt` for auto-install\n"
        f"• Include `main.py` as the entry point\n\n"
        f"Send /done or click Done when finished."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Done Uploading", callback_data="new_project_done")],
        [InlineKeyboardButton("❌ Cancel", callback_data="start")],
    ])
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    return NEW_PROJECT_FILES


async def new_project_receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    project_name = context.user_data.get("new_project_name")
    files_received = context.user_data.get("new_project_files", [])

    if not project_name:
        await update.message.reply_text("❌ Session expired. Use /start to begin again.")
        return ConversationHandler.END

    document = update.message.document
    if not document:
        await update.message.reply_text("Please send a file or use /done when finished.")
        return NEW_PROJECT_FILES

    file_name = document.file_name
    file_size = document.file_size

    if file_size > 50 * 1024 * 1024:  # 50MB limit
        await update.message.reply_text("❌ File too large (max 50MB). Try again.")
        return NEW_PROJECT_FILES

    # Create temp download dir
    temp_dir = os.path.join(PROJECTS_DIR, f"_temp_{user.id}")
    os.makedirs(temp_dir, exist_ok=True)

    tg_file = await document.get_file()
    temp_path = os.path.join(temp_dir, file_name)
    await tg_file.download_to_drive(temp_path)

    files_received.append({"name": file_name, "path": temp_path})
    context.user_data["new_project_files"] = files_received

    count = len(files_received)
    await update.message.reply_text(
        f"✅ Received: `{file_name}` ({format_bytes(file_size)})\n"
        f"📦 Total files received: {count}\n\n"
        f"Send more files or /done to finalize.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return NEW_PROJECT_FILES


async def new_project_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /done command or Done button during file upload."""
    query = update.callback_query
    user = update.effective_user
    if query:
        await query.answer()

    if await is_banned(user.id):
        await banned_reply(update)
        return ConversationHandler.END

    project_name = context.user_data.get("new_project_name")
    files = context.user_data.get("new_project_files", [])

    if not project_name:
        text = "❌ Session expired. Use /start to begin again."
        if query:
            await query.edit_message_text(text)
        else:
            await update.message.reply_text(text)
        return ConversationHandler.END

    if not files:
        text = "❌ No files uploaded yet. Please send at least one file."
        if query:
            await query.edit_message_text(text)
        else:
            await update.message.reply_text(text)
        return NEW_PROJECT_FILES

    processing_text = f"⚙️ *Setting up project `{project_name}`...*\n\n"
    if query:
        msg = await query.edit_message_text(processing_text, parse_mode=ParseMode.MARKDOWN)
    else:
        msg = await update.message.reply_text(processing_text, parse_mode=ParseMode.MARKDOWN)

    proj_dir = project_dir(user.id, project_name)
    os.makedirs(proj_dir, exist_ok=True)
    temp_dir = os.path.join(PROJECTS_DIR, f"_temp_{user.id}")

    # Move/extract files
    status_lines = ["📂 *Files:*"]
    for f in files:
        src = f["path"]
        fname = f["name"]
        if fname.endswith(".zip"):
            try:
                with zipfile.ZipFile(src, "r") as zf:
                    zf.extractall(proj_dir)
                status_lines.append(f"  ✅ `{fname}` — extracted")
            except Exception as e:
                status_lines.append(f"  ❌ `{fname}` — zip error: {e}")
        else:
            dest = os.path.join(proj_dir, fname)
            shutil.move(src, dest)
            status_lines.append(f"  ✅ `{fname}` — uploaded")

    # Cleanup temp dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    # Create virtualenv
    status_lines.append("\n🐍 *Creating virtualenv...*")
    venv_dir = os.path.join(proj_dir, "venv")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "venv", venv_dir],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            status_lines.append("  ✅ Virtualenv created")
        else:
            status_lines.append(f"  ❌ Virtualenv error: {result.stderr[:200]}")
    except Exception as e:
        status_lines.append(f"  ❌ Virtualenv exception: {e}")

    # Install requirements.txt if present
    req_file = os.path.join(proj_dir, "requirements.txt")
    if os.path.exists(req_file):
        status_lines.append("\n📦 *Installing requirements.txt...*")
        pip_path = venv_pip(user.id, project_name)
        try:
            with open(req_file, "r") as rf:
                packages = [l.strip() for l in rf if l.strip() and not l.startswith("#")]
            for pkg in packages:
                try:
                    res = subprocess.run(
                        [pip_path, "install", pkg, "--quiet"],
                        capture_output=True, text=True, timeout=120
                    )
                    if res.returncode == 0:
                        status_lines.append(f"  ✅ `{pkg}`")
                    else:
                        err = res.stderr.strip()[:100]
                        status_lines.append(f"  ❌ `{pkg}` — {err}")
                except subprocess.TimeoutExpired:
                    status_lines.append(f"  ⏱ `{pkg}` — install timed out")
                except Exception as e:
                    status_lines.append(f"  ❌ `{pkg}` — {e}")
        except Exception as e:
            status_lines.append(f"  ❌ requirements.txt error: {e}")
    else:
        status_lines.append("\n📦 No requirements.txt found, skipping install.")

    # Save project to DB
    now = datetime.datetime.utcnow()
    await projects_col.insert_one({
        "user_id": user.id,
        "name": project_name,
        "run_command": "python3 main.py",
        "created_date": now,
        "last_run": None,
        "exit_code": None,
        "status": "stopped",
        "pid": None,
    })

    context.user_data["current_project"] = project_name

    status_text = "\n".join(status_lines)
    final_text = (
        f"✅ *Project `{project_name}` created successfully!*\n\n"
        f"{status_text}\n\n"
        f"🔹 Run Command: `python3 main.py`\n"
        f"_(You can change this in the project dashboard)_"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Open Dashboard", callback_data=f"project:{project_name}")],
        [InlineKeyboardButton("🔙 My Projects", callback_data="my_projects")],
    ])

    try:
        await msg.edit_text(final_text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except Exception:
        await context.bot.send_message(
            chat_id=user.id, text=final_text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb
        )

    context.user_data.pop("new_project_name", None)
    context.user_data.pop("new_project_files", None)
    return ConversationHandler.END


# ─────────────────────────────────────────
# PROJECT ACTIONS
# ─────────────────────────────────────────

async def run_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    await query.answer()

    if await is_banned(user.id):
        await banned_reply(update)
        return

    project_name = context.user_data.get("current_project")
    if not project_name:
        await query.answer("❌ No project selected.", show_alert=True)
        return

    proj = await get_project(user.id, project_name)
    if not proj:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_keyboard("my_projects"))
        return

    proc_key = project_key(user.id, project_name)
    if is_process_running(proc_key):
        await query.answer("⚠️ Project is already running!", show_alert=True)
        return

    # Check if admin stopped it
    if proj.get("admin_stopped"):
        await query.answer(
            "⚠️ Your project was stopped by admin. Contact owner.", show_alert=True
        )
        return

    proj_dir_path = project_dir(user.id, project_name)
    python_exec = venv_python(user.id, project_name)
    if not os.path.exists(python_exec):
        python_exec = sys.executable  # fallback to system python

    run_cmd = proj.get("run_command", "python3 main.py")
    log_path = log_file(user.id, project_name)

    # Build command, replacing python3 with venv python
    cmd_parts = run_cmd.split()
    if cmd_parts[0] in ("python3", "python"):
        cmd_parts[0] = python_exec

    try:
        log_f = open(log_path, "a", encoding="utf-8")
        log_f.write(f"\n--- Run started at {datetime.datetime.utcnow().isoformat()} ---\n")
        proc = subprocess.Popen(
            cmd_parts,
            cwd=proj_dir_path,
            stdout=log_f,
            stderr=log_f,
            start_new_session=True,
        )
        running_processes[proc_key] = proc
    except Exception as e:
        await query.answer(f"❌ Failed to start: {e}", show_alert=True)
        return

    now = datetime.datetime.utcnow()
    await projects_col.update_one(
        {"user_id": user.id, "name": project_name},
        {"$set": {"status": "running", "pid": proc.pid, "last_run": now, "admin_stopped": False}},
    )

    await query.answer(f"✅ Started with PID {proc.pid}")
    await show_project_dashboard(update, context)


async def stop_project_by_key(proc_key: str) -> bool:
    """Stop a running project. Returns True if stopped."""
    proc = running_processes.get(proc_key)
    if proc is None:
        return False
    try:
        parent = psutil.Process(proc.pid)
        for child in parent.children(recursive=True):
            child.kill()
        parent.kill()
        proc.wait(timeout=5)
    except (psutil.NoSuchProcess, subprocess.TimeoutExpired, ProcessLookupError):
        pass
    running_processes.pop(proc_key, None)
    return True


async def restart_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    await query.answer()

    if await is_banned(user.id):
        await banned_reply(update)
        return

    project_name = context.user_data.get("current_project")
    if not project_name:
        await query.answer("❌ No project selected.", show_alert=True)
        return

    proc_key = project_key(user.id, project_name)
    await stop_project_by_key(proc_key)
    await projects_col.update_one(
        {"user_id": user.id, "name": project_name},
        {"$set": {"status": "stopped", "pid": None}},
    )
    await query.answer("🔄 Restarting...")
    await run_project(update, context)


async def show_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    await query.answer()

    project_name = context.user_data.get("current_project")
    if not project_name:
        await query.answer("❌ No project selected.", show_alert=True)
        return

    log_path = log_file(user.id, project_name)
    if not os.path.exists(log_path):
        await query.answer("📋 No logs yet.", show_alert=True)
        return

    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        last_50 = "".join(lines[-50:]).strip()
        if not last_50:
            last_50 = "(empty)"
    except Exception as e:
        last_50 = f"Error reading logs: {e}"

    # Truncate if too long for Telegram
    if len(last_50) > 4000:
        last_50 = "...(truncated)\n" + last_50[-3900:]

    text = f"📋 *Logs: {project_name}*\n\n```\n{last_50}\n```"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back", callback_data=f"project:{project_name}")],
    ])
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except BadRequest:
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


async def edit_run_cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    project_name = context.user_data.get("current_project")
    if not project_name:
        await query.answer("❌ No project selected.", show_alert=True)
        return ConversationHandler.END

    proj = await get_project(update.effective_user.id, project_name)
    current_cmd = proj.get("run_command", "python3 main.py") if proj else "python3 main.py"

    await query.edit_message_text(
        f"✏️ *Edit Run Command*\n\n"
        f"Current: `{current_cmd}`\n\n"
        f"Send the new run command:\n"
        f"Examples:\n"
        f"• `python3 main.py`\n"
        f"• `python3 bot.py --token abc`\n"
        f"• `python3 -m mymodule`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard(f"project:{project_name}"),
    )
    return EDIT_RUN_CMD


async def edit_run_cmd_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    new_cmd = update.message.text.strip()

    if not new_cmd:
        await update.message.reply_text("❌ Command cannot be empty. Try again:")
        return EDIT_RUN_CMD

    project_name = context.user_data.get("current_project")
    await projects_col.update_one(
        {"user_id": user.id, "name": project_name},
        {"$set": {"run_command": new_cmd}},
    )
    await update.message.reply_text(
        f"✅ Run command updated to:\n`{new_cmd}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard(f"project:{project_name}"),
    )
    return ConversationHandler.END


async def open_file_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    await query.answer()

    if await is_banned(user.id):
        await banned_reply(update)
        return

    project_name = context.user_data.get("current_project")
    if not project_name:
        await query.answer("❌ No project selected.", show_alert=True)
        return

    proj_path = project_dir(user.id, project_name)
    if not os.path.exists(proj_path):
        await query.answer("❌ Project directory not found.", show_alert=True)
        return

    token = secrets.token_urlsafe(32)
    now = datetime.datetime.utcnow()
    expires_at = now + datetime.timedelta(minutes=FILE_MANAGER_EXPIRY_MINUTES)

    # Store in memory (Flask) and MongoDB
    from file_manager import register_token
    register_token(token, user.id, project_name, proj_path, expires_at)

    await tokens_col.insert_one({
        "token": token,
        "user_id": user.id,
        "project_name": project_name,
        "created_at": now,
        "expires_at": expires_at,
    })

    # Build URL — Render provides the external URL via RENDER_EXTERNAL_URL env var
    base_url = os.environ.get("RENDER_EXTERNAL_URL", f"http://localhost:{PORT}")
    fm_url = f"{base_url}/fm/{token}/"

    text = (
        f"📁 *File Manager — {project_name}*\n\n"
        f"⏱ Link expires in {FILE_MANAGER_EXPIRY_MINUTES} minutes.\n\n"
        f"Click the button below to open:"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 Open File Manager", url=fm_url)],
        [InlineKeyboardButton("🔙 Back", callback_data=f"project:{project_name}")],
    ])
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except BadRequest:
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


async def delete_project_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    project_name = context.user_data.get("current_project")
    if not project_name:
        await query.answer("❌ No project selected.", show_alert=True)
        return

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Delete", callback_data="proj_delete_confirm"),
            InlineKeyboardButton("❌ No, Cancel", callback_data=f"project:{project_name}"),
        ]
    ])
    await query.edit_message_text(
        f"🗑 *Delete Project: {project_name}*\n\n"
        f"⚠️ This will permanently delete all files and data.\n\n"
        f"Are you sure?",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb,
    )


async def delete_project_execute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    await query.answer()

    project_name = context.user_data.get("current_project")
    if not project_name:
        await query.answer("❌ No project selected.", show_alert=True)
        return

    # Stop if running
    proc_key = project_key(user.id, project_name)
    await stop_project_by_key(proc_key)

    # Delete files
    proj_path = project_dir(user.id, project_name)
    if os.path.exists(proj_path):
        shutil.rmtree(proj_path)

    # Delete from DB
    await projects_col.delete_one({"user_id": user.id, "name": project_name})
    await tokens_col.delete_many({"user_id": user.id, "project_name": project_name})

    context.user_data.pop("current_project", None)

    await query.edit_message_text(
        f"✅ Project `{project_name}` has been deleted.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard("my_projects"),
    )


# ─────────────────────────────────────────
# PREMIUM PAGE
# ─────────────────────────────────────────

async def show_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    await query.answer()

    is_premium = await check_premium_expiry(user.id)

    plan_comparison = (
        f"*Free Plan:*\n"
        f"• 1 Project only\n"
        f"• File Manager (10 min)\n\n"
        f"*Premium Plan:*\n"
        f"• ✅ 10 projects\n"
        f"• ✅ Priority support\n"
        f"• ✅ Extended file manager\n"
        f"• ✅ Advanced monitoring\n"
    )

    if is_premium:
        text = (
            f"💎 *Premium Membership*\n\n"
            f"✨ *You are Premium!* ✨\n\n"
            f"{plan_comparison}\n"
            f"🌟 Premium is active!"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="start")]])
    else:
        text = (
            f"💎 *Premium Membership*\n\n"
            f"{plan_comparison}\n"
            f"To get Premium, contact the owner!"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Contact Owner", url=f"https://t.me/{OWNER_USERNAME}")],
            [InlineKeyboardButton("🔙 Back", callback_data="start")],
        ])

    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except BadRequest:
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


# ─────────────────────────────────────────
# BOT STATUS
# ─────────────────────────────────────────

async def show_bot_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    total_users = await users_col.count_documents({})
    premium_users = await users_col.count_documents({"is_premium": True})
    total_projects = await projects_col.count_documents({})
    running_count = len([k for k in running_processes if is_process_running(k)])

    cpu = psutil.cpu_percent(interval=0.5)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    bot_uptime = format_uptime(time.time() - BOT_START_TIME)

    # Ping
    ping_start = time.time()
    ping_ms = round((time.time() - ping_start) * 1000, 2)

    text = (
        f"📊 *Bot Dashboard*\n\n"
        f"👥 Total Users: {total_users}\n"
        f"💎 Premium Users: {premium_users}\n"
        f"📁 Total Projects: {total_projects}\n"
        f"🟢 Running Projects: {running_count}\n"
        f"💾 Database: MongoDB ✅\n"
        f"🐍 Python: {py_ver}\n\n"
        f"💻 *System:*\n"
        f"├ CPU: {cpu}%\n"
        f"├ RAM: {format_bytes(mem.used)}/{format_bytes(mem.total)} ({mem.percent}%)\n"
        f"└ Disk: {format_bytes(disk.used)}/{format_bytes(disk.total)} ({disk.percent}%)\n\n"
        f"🏓 Bot Ping: {ping_ms}ms\n"
        f"⏰ Uptime: {bot_uptime}"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔃 Refresh", callback_data="bot_status"),
            InlineKeyboardButton("🔙 Back", callback_data="start"),
        ]
    ])
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except BadRequest:
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


# ─────────────────────────────────────────
# ADMIN PANEL
# ─────────────────────────────────────────

def owner_only(func):
    """Decorator to restrict handlers to owner only."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if user.id != OWNER_ID:
            if update.callback_query:
                await update.callback_query.answer("🚫 Owner only.", show_alert=True)
            else:
                await update.message.reply_text("🚫 Owner only.")
            return
        return await func(update, context)
    return wrapper


@owner_only
async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    total_users = await users_col.count_documents({})
    premium_count = await users_col.count_documents({"is_premium": True})
    banned_count = await users_col.count_documents({"is_banned": True})
    total_projects = await projects_col.count_documents({})
    running_count = len([k for k in running_processes if is_process_running(k)])

    text = (
        f"⚙️ *Admin Panel*\n\n"
        f"👥 Total Users: {total_users}\n"
        f"💎 Premium: {premium_count}\n"
        f"🚫 Banned: {banned_count}\n"
        f"📁 Projects: {total_projects}\n"
        f"🟢 Running: {running_count}"
    )
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=admin_keyboard())
    except BadRequest:
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=admin_keyboard())


@owner_only
async def admin_user_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    page = context.user_data.get("admin_user_page", 0)
    per_page = 10
    users = await users_col.find().skip(page * per_page).limit(per_page).to_list(per_page)
    total = await users_col.count_documents({})

    lines = [f"👥 *User List* (Page {page+1}/{max(1,(total+per_page-1)//per_page)})\n"]
    for u in users:
        uname = f"@{u['username']}" if u.get("username") else "No username"
        premium = "💎" if u.get("is_premium") else ""
        banned = "🚫" if u.get("is_banned") else ""
        lines.append(f"• `{u['user_id']}` {uname} {premium}{banned}")

    text = "\n".join(lines) if len(lines) > 1 else "👥 *User List*\n\nNo users found."

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Prev", callback_data="admin_users_prev"))
    if (page + 1) * per_page < total:
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data="admin_users_next"))

    kb = InlineKeyboardMarkup([nav_buttons] if nav_buttons else [] + [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]])
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except BadRequest:
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


@owner_only
async def admin_running_scripts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    lines = ["🟢 *Running Scripts*\n"]
    if not running_processes:
        lines.append("No running projects.")
    else:
        for proc_key, proc in list(running_processes.items()):
            if not is_process_running(proc_key):
                continue
            parts = proc_key.split(":", 1)
            uid, pname = int(parts[0]), parts[1]
            user_doc = await users_col.find_one({"user_id": uid})
            uname = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
            uptime = get_process_uptime(proc_key)
            uptime_str = format_uptime(uptime) if uptime else "N/A"

            # List script files
            pdir = project_dir(uid, pname)
            try:
                fnames = [f for f in os.listdir(pdir) if os.path.isfile(os.path.join(pdir, f)) and f != "output.log"]
            except Exception:
                fnames = []
            files_str = ", ".join(fnames[:5]) or "N/A"

            lines.append(
                f"━━━━━━━━━━━\n"
                f"USER NAME: {uname}\n"
                f"User ID: `{uid}`\n"
                f"Project PID: `{proc.pid}`\n"
                f"Running Time: {uptime_str}\n"
                f"Script: {files_str}"
            )

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3900] + "\n...(truncated)"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]])
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except BadRequest:
        await query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


# --- Give Premium Conversation ---
@owner_only
async def admin_give_premium_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "💎 *Give Premium*\n\nEnter the User ID to give premium to:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard("admin_panel"),
    )
    return ADMIN_GIVE_PREMIUM


async def admin_give_premium_execute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Enter a number:")
        return ADMIN_GIVE_PREMIUM

    result = await users_col.update_one(
        {"user_id": target_id},
        {"$set": {"is_premium": True, "premium_expiry": None}},
    )
    if result.matched_count:
        await update.message.reply_text(
            f"✅ User `{target_id}` is now Premium!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="🎉 *Congratulations!* You've been granted Premium membership! 💎",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
    else:
        await update.message.reply_text(
            f"❌ User `{target_id}` not found in database.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
    return ConversationHandler.END


# --- Remove Premium Conversation ---
@owner_only
async def admin_remove_premium_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "❌ *Remove Premium*\n\nEnter the User ID to remove premium from:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard("admin_panel"),
    )
    return ADMIN_REMOVE_PREMIUM


async def admin_remove_premium_execute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Enter a number:")
        return ADMIN_REMOVE_PREMIUM

    result = await users_col.update_one(
        {"user_id": target_id},
        {"$set": {"is_premium": False, "premium_expiry": None}},
    )
    if result.matched_count:
        await update.message.reply_text(
            f"✅ Removed premium from user `{target_id}`.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
    else:
        await update.message.reply_text(
            f"❌ User `{target_id}` not found.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
    return ConversationHandler.END


# --- Temp Premium Conversation ---
@owner_only
async def admin_temp_premium_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "⏰ *Temp Premium*\n\nEnter the User ID:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard("admin_panel"),
    )
    return ADMIN_TEMP_PREMIUM_ID


async def admin_temp_premium_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(update.message.text.strip())
        context.user_data["temp_premium_target"] = target_id
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Enter a number:")
        return ADMIN_TEMP_PREMIUM_ID

    await update.message.reply_text(
        f"⏰ Enter duration for user `{target_id}`:\n"
        f"Format: `Xh` (hours) or `Xd` (days)\n"
        f"Example: `24h` or `7d`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ADMIN_TEMP_PREMIUM_DURATION


async def admin_temp_premium_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip().lower()
    target_id = context.user_data.get("temp_premium_target")

    match = re.match(r"^(\d+)([hd])$", raw)
    if not match:
        await update.message.reply_text("❌ Invalid format. Use `24h` or `7d`:")
        return ADMIN_TEMP_PREMIUM_DURATION

    amount, unit = int(match.group(1)), match.group(2)
    if unit == "h":
        delta = datetime.timedelta(hours=amount)
    else:
        delta = datetime.timedelta(days=amount)

    expiry = datetime.datetime.utcnow() + delta

    result = await users_col.update_one(
        {"user_id": target_id},
        {"$set": {"is_premium": True, "premium_expiry": expiry}},
    )
    if result.matched_count:
        await update.message.reply_text(
            f"✅ User `{target_id}` has temp premium until `{expiry.strftime('%Y-%m-%d %H:%M UTC')}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text=f"🎉 You've been granted *Temporary Premium* until {expiry.strftime('%Y-%m-%d %H:%M UTC')}! 💎",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
    else:
        await update.message.reply_text(
            f"❌ User `{target_id}` not found.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
    return ConversationHandler.END


# --- Ban User Conversation ---
@owner_only
async def admin_ban_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "🚫 *Ban User*\n\nEnter the User ID to ban:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard("admin_panel"),
    )
    return ADMIN_BAN_USER


async def admin_ban_execute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Enter a number:")
        return ADMIN_BAN_USER

    if target_id == OWNER_ID:
        await update.message.reply_text("❌ Cannot ban yourself!")
        return ConversationHandler.END

    # Stop all their projects
    for key in list(running_processes.keys()):
        if key.startswith(f"{target_id}:"):
            await stop_project_by_key(key)

    await projects_col.update_many(
        {"user_id": target_id},
        {"$set": {"status": "stopped", "pid": None}},
    )
    result = await users_col.update_one(
        {"user_id": target_id},
        {"$set": {"is_banned": True}},
    )
    if result.matched_count:
        await update.message.reply_text(
            f"✅ User `{target_id}` has been banned. All their projects stopped.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
    else:
        await update.message.reply_text(
            f"❌ User `{target_id}` not found.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
    return ConversationHandler.END


# --- Unban Conversation ---
@owner_only
async def admin_unban_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "✅ *Unban User*\n\nEnter the User ID to unban:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard("admin_panel"),
    )
    return ADMIN_UNBAN_USER


async def admin_unban_execute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Enter a number:")
        return ADMIN_UNBAN_USER

    result = await users_col.update_one(
        {"user_id": target_id},
        {"$set": {"is_banned": False}},
    )
    if result.matched_count:
        await update.message.reply_text(
            f"✅ User `{target_id}` has been unbanned.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="✅ You have been unbanned. You can use the bot again.",
            )
        except Exception:
            pass
    else:
        await update.message.reply_text(
            f"❌ User `{target_id}` not found.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_keyboard("admin_panel"),
        )
    return ConversationHandler.END


# --- Broadcast Conversation ---
@owner_only
async def admin_broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Broadcast All", callback_data="admin_broadcast_all")],
        [InlineKeyboardButton("📩 Send to User", callback_data="admin_send_user")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
    ])
    await query.edit_message_text(
        "📢 *Broadcast*\n\nChoose broadcast type:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb,
    )


@owner_only
async def admin_broadcast_all_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["broadcast_target"] = "all"
    await query.edit_message_text(
        "📢 *Broadcast to All Users*\n\nSend your message:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard("admin_broadcast"),
    )
    return ADMIN_BROADCAST_MSG


@owner_only
async def admin_send_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "📩 *Send to Specific User*\n\nEnter the User ID:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard("admin_broadcast"),
    )
    return ADMIN_SEND_USER_ID


async def admin_send_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_id = int(update.message.text.strip())
        context.user_data["broadcast_target"] = target_id
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Enter a number:")
        return ADMIN_SEND_USER_ID
    await update.message.reply_text(
        f"📩 Send your message for user `{target_id}`:",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ADMIN_SEND_USER_MSG


async def admin_do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = context.user_data.get("broadcast_target")
    message_text = update.message.text

    if target == "all":
        all_users = await users_col.find({}, {"user_id": 1}).to_list(10000)
        sent, failed = 0, 0
        for u in all_users:
            try:
                await context.bot.send_message(
                    chat_id=u["user_id"],
                    text=f"📢 *Broadcast from Admin:*\n\n{message_text}",
                    parse_mode=ParseMode.MARKDOWN,
                )
                sent += 1
            except Exception:
                failed += 1
        await update.message.reply_text(
            f"✅ Broadcast complete!\nSent: {sent} | Failed: {failed}",
            reply_markup=back_keyboard("admin_panel"),
        )
    else:
        try:
            await context.bot.send_message(
                chat_id=int(target),
                text=f"📩 *Message from Admin:*\n\n{message_text}",
                parse_mode=ParseMode.MARKDOWN,
            )
            await update.message.reply_text(
                f"✅ Message sent to user `{target}`.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=back_keyboard("admin_panel"),
            )
        except Exception as e:
            await update.message.reply_text(
                f"❌ Failed to send: {e}",
                reply_markup=back_keyboard("admin_panel"),
            )
    return ConversationHandler.END


# ─────────────────────────────────────────
# CALLBACK QUERY ROUTER
# ─────────────────────────────────────────

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    data = query.data

    # Banned check (except for admin panel actions by owner)
    if user.id != OWNER_ID and await is_banned(user.id):
        await query.answer("🚫 You are banned. Contact owner.", show_alert=True)
        return

    if data == "start":
        await start(update, context)
    elif data == "my_projects":
        await show_my_projects(update, context)
    elif data.startswith("project:"):
        project_name = data.split(":", 1)[1]
        context.user_data["current_project"] = project_name
        await show_project_dashboard(update, context)
    elif data == "premium":
        await show_premium(update, context)
    elif data == "bot_status":
        await show_bot_status(update, context)
    elif data == "admin_panel":
        await show_admin_panel(update, context)
    elif data == "proj_run":
        await run_project(update, context)
    elif data == "proj_restart":
        await restart_project(update, context)
    elif data == "proj_logs":
        await show_logs(update, context)
    elif data == "proj_refresh":
        await show_project_dashboard(update, context)
    elif data == "proj_file_manager":
        await open_file_manager(update, context)
    elif data == "proj_delete":
        await delete_project_confirm(update, context)
    elif data == "proj_delete_confirm":
        await delete_project_execute(update, context)
    elif data == "admin_users":
        context.user_data["admin_user_page"] = 0
        await admin_user_list(update, context)
    elif data == "admin_users_next":
        context.user_data["admin_user_page"] = context.user_data.get("admin_user_page", 0) + 1
        await admin_user_list(update, context)
    elif data == "admin_users_prev":
        context.user_data["admin_user_page"] = max(0, context.user_data.get("admin_user_page", 0) - 1)
        await admin_user_list(update, context)
    elif data == "admin_running":
        await admin_running_scripts(update, context)
    elif data == "admin_broadcast":
        await admin_broadcast_menu(update, context)
    elif data == "admin_broadcast_all":
        await admin_broadcast_all_start(update, context)
    elif data == "admin_send_user":
        await admin_send_user_start(update, context)
    elif data == "new_project_done":
        await new_project_done(update, context)


# ─────────────────────────────────────────
# DONE COMMAND (alias for file upload done)
# ─────────────────────────────────────────

async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /done command during file upload."""
    if context.user_data.get("new_project_name"):
        await new_project_done(update, context)
    else:
        await update.message.reply_text("No active upload session. Use /start to begin.")


# ─────────────────────────────────────────
# PROCESS MONITOR (background task)
# ─────────────────────────────────────────

async def monitor_processes(app: Application):
    """Background task to monitor running processes and update DB."""
    while True:
        await asyncio.sleep(30)
        for proc_key, proc in list(running_processes.items()):
            if proc.poll() is not None:  # Process has exited
                exit_code = proc.returncode
                parts = proc_key.split(":", 1)
                uid, pname = int(parts[0]), parts[1]

                await projects_col.update_one(
                    {"user_id": uid, "name": pname},
                    {"$set": {"status": "stopped", "pid": None, "exit_code": exit_code}},
                )
                running_processes.pop(proc_key, None)
                logger.info(f"Process {proc_key} exited with code {exit_code}")


# ─────────────────────────────────────────
# APPLICATION SETUP
# ─────────────────────────────────────────

def build_application() -> Application:
    application = Application.builder().token(BOT_TOKEN).build()

    # Start command
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("done", done_command))

    # New Project conversation
    new_project_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(new_project_start, pattern="^new_project$")],
        states={
            NEW_PROJECT_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_project_name),
            ],
            NEW_PROJECT_FILES: [
                MessageHandler(filters.Document.ALL, new_project_receive_file),
                CommandHandler("done", new_project_done),
                CallbackQueryHandler(new_project_done, pattern="^new_project_done$"),
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            CallbackQueryHandler(callback_router, pattern="^start$"),
        ],
        allow_reentry=True,
    )
    application.add_handler(new_project_conv)

    # Edit Run Command conversation
    edit_cmd_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_run_cmd_start, pattern="^proj_edit_cmd$")],
        states={
            EDIT_RUN_CMD: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_run_cmd_save)],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    application.add_handler(edit_cmd_conv)

    # Admin conversations
    give_premium_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_give_premium_start, pattern="^admin_give_premium$")],
        states={ADMIN_GIVE_PREMIUM: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_give_premium_execute)]},
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    application.add_handler(give_premium_conv)

    remove_premium_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_remove_premium_start, pattern="^admin_remove_premium$")],
        states={ADMIN_REMOVE_PREMIUM: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_remove_premium_execute)]},
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    application.add_handler(remove_premium_conv)

    temp_premium_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_temp_premium_start, pattern="^admin_temp_premium$")],
        states={
            ADMIN_TEMP_PREMIUM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_premium_id)],
            ADMIN_TEMP_PREMIUM_DURATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_premium_duration)],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    application.add_handler(temp_premium_conv)

    ban_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_ban_start, pattern="^admin_ban$")],
        states={ADMIN_BAN_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ban_execute)]},
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    application.add_handler(ban_conv)

    unban_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_unban_start, pattern="^admin_unban$")],
        states={ADMIN_UNBAN_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_unban_execute)]},
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    application.add_handler(unban_conv)

    broadcast_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(admin_broadcast_all_start, pattern="^admin_broadcast_all$"),
            CallbackQueryHandler(admin_send_user_start, pattern="^admin_send_user$"),
        ],
        states={
            ADMIN_BROADCAST_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_do_broadcast)],
            ADMIN_SEND_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_send_user_id)],
            ADMIN_SEND_USER_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_do_broadcast)],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    application.add_handler(broadcast_conv)

    # Generic callback router (must be last)
    application.add_handler(CallbackQueryHandler(callback_router))

    return application


async def post_init(application: Application):
    """Start background process monitor."""
    asyncio.create_task(monitor_processes(application))


def main():
    # Start Flask file manager in background thread
    from file_manager import run_flask
    flask_thread = threading.Thread(target=run_flask, args=(PORT,), daemon=True)
    flask_thread.start()
    logger.info(f"Flask file manager started on port {PORT}")

    # Build and run bot
    application = build_application()
    application.post_init = post_init

    logger.info("Starting God Madara Hosting Bot...")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
