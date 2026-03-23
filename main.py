#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════════
                    GOD MADARA HOSTING BOT v3.0
══════════════════════════════════════════════════════════════════
🔥 Ultimate Python Project Hosting Bot for Telegram
👑 Owner: Your Name
⚡ Features: 24/7 Hosting | File Manager | Auto-Backup | Admin Panel

[FINAL COMPLETE SCRIPT - COPY PASTE AND RUN]
══════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import uuid
import shutil
import psutil
import zipfile
import asyncio
import tempfile
import subprocess
import threading
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path

# Flask for File Manager
from flask import Flask, request, send_file, render_template_string, redirect, flash

# Telegram
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, 
    InputFile
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters
)

# MongoDB
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# ==================== 🔧 CONFIGURATION ====================
print("🔧 Loading Configuration...")

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
RENDER_URL = os.getenv("RENDER_URL", "https://your-service.onrender.com")

if not all([BOT_TOKEN, MONGO_URI, OWNER_ID]):
    print("❌ ERROR: Missing environment variables!")
    print("Required: BOT_TOKEN, MONGO_URI, OWNER_ID")
    sys.exit(1)

# Constants
FREE_PROJECTS_LIMIT = 1
PREMIUM_PROJECTS_LIMIT = 999999
BACKUP_INTERVAL = 300  # 5 minutes
FILE_MANAGER_EXPIRY = 600  # 10 minutes

# ==================== 🗄️ MONGODB SETUP ====================
print("🗄️ Connecting to MongoDB...")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()  # Test connection
    db = client["god_madara_bot"]
    users_collection = db["users"]
    projects_collection = db["projects"]
    backups_collection = db["backups"]
    
    # Create indexes
    users_collection.create_index("user_id", unique=True)
    projects_collection.create_index("project_id", unique=True)
    projects_collection.create_index("user_id")
    
    print("✅ MongoDB Connected!")
except Exception as e:
    print(f"❌ MongoDB Error: {e}")
    sys.exit(1)

# ==================== 📁 PROJECT MANAGER ====================
class ProjectManager:
    def __init__(self):
        self.processes = {}  # project_id: {process, start_time, log_file, run_command}
        self.base_dir = Path("projects")
        self.base_dir.mkdir(exist_ok=True)
        self.lock = threading.Lock()
    
    def get_project_dir(self, user_id, project_name):
        safe_name = "".join(c if c.isalnum() or c in ['-', '_'] else '_' for c in project_name)
        project_dir = self.base_dir / str(user_id) / safe_name
        project_dir.mkdir(parents=True, exist_ok=True)
        return project_dir
    
    def generate_project_id(self, user_id, project_name):
        return f"{user_id}_{uuid.uuid4().hex[:8]}_{int(time.time())}"
    
    def install_requirements(self, project_dir):
        req_file = project_dir / "requirements.txt"
        if req_file.exists():
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "pip", "install", "-r", str(req_file)],
                    capture_output=True,
                    text=True,
                    cwd=str(project_dir),
                    timeout=180
                )
                return result.returncode == 0, result.stdout + result.stderr
            except Exception as e:
                return False, str(e)
        return True, "No requirements.txt found"
    
    def run_project(self, project_id, project_dir, run_command):
        with self.lock:
            if project_id in self.processes:
                return False, "⚠️ Project already running!"
            
            log_file = project_dir / "output.log"
            
            try:
                # Clear old logs
                with open(log_file, "w") as f:
                    f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Starting project...\n")
                    f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Command: {run_command}\n\n")
                
                # Start process
                process = subprocess.Popen(
                    run_command,
                    shell=True,
                    cwd=str(project_dir),
                    stdout=open(log_file, "a"),
                    stderr=subprocess.STDOUT,
                    preexec_fn=os.setsid if os.name != 'nt' else None,
                    universal_newlines=True
                )
                
                self.processes[project_id] = {
                    'process': process,
                    'start_time': datetime.now(),
                    'log_file': log_file,
                    'run_command': run_command
                }
                
                # Monitor thread
                def monitor():
                    process.wait()
                    exit_code = process.returncode
                    with self.lock:
                        if project_id in self.processes:
                            del self.processes[project_id]
                    # Update DB
                    projects_collection.update_one(
                        {"project_id": project_id},
                        {"$set": {
                            "last_exit_code": exit_code,
                            "crashed": exit_code != 0 and exit_code is not None,
                            "last_run": datetime.now().isoformat()
                        }}
                    )
                    with open(log_file, "a") as f:
                        f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Process ended with code {exit_code}\n")
                
                threading.Thread(target=monitor, daemon=True).start()
                
                return True, "✅ Project started successfully!"
            except Exception as e:
                return False, f"❌ Error: {str(e)}"
    
    def stop_project(self, project_id):
        with self.lock:
            if project_id not in self.processes:
                return False, "⚠️ Project not running"
            
            try:
                proc_info = self.processes[project_id]
                process = proc_info['process']
                
                # Try graceful termination
                if os.name != 'nt':
                    import signal
                    try:
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    except:
                        pass
                else:
                    process.terminate()
                
                # Wait for termination
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill
                    if os.name != 'nt':
                        try:
                            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                        except:
                            pass
                    else:
                        process.kill()
                
                del self.processes[project_id]
                return True, "⏹️ Project stopped successfully"
            except Exception as e:
                return False, f"❌ Error: {str(e)}"
    
    def restart_project(self, project_id, project_dir, run_command):
        self.stop_project(project_id)
        time.sleep(2)
        return self.run_project(project_id, project_dir, run_command)
    
    def get_status(self, project_id):
        project = projects_collection.find_one({"project_id": project_id})
        if not project:
            return None
        
        with self.lock:
            is_running = project_id in self.processes
        
        pid = "N/A"
        uptime = "N/A"
        
        if is_running:
            proc_info = self.processes[project_id]
            pid = proc_info['process'].pid
            delta = datetime.now() - proc_info['start_time']
            uptime = str(delta).split('.')[0]
        
        # Determine status emoji
        if is_running:
            status_emoji = "🟢 Running"
        elif project.get('crashed', False):
            status_emoji = "🟠 Crashed"
        else:
            status_emoji = "🔴 Stopped"
        
        return {
            'status': status_emoji,
            'pid': pid,
            'uptime': uptime,
            'last_run': project.get('last_run', 'Never'),
            'exit_code': str(project.get('last_exit_code', 'None')),
            'run_command': project.get('run_command', 'python3 main.py'),
            'is_running': is_running
        }
    
    def get_logs(self, project_id, lines=200):
        project = projects_collection.find_one({"project_id": project_id})
        if not project:
            return None
        
        log_file = Path(project['project_dir']) / "output.log"
        if not log_file.exists():
            return "📭 No logs available yet"
        
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                all_lines = f.readlines()
                if not all_lines:
                    return "📭 Log file is empty"
                return ''.join(all_lines[-lines:])
        except Exception as e:
            return f"❌ Error reading logs: {str(e)}"

# Initialize
project_manager = ProjectManager()
file_manager_sessions = {}

# ==================== 🔗 FILE MANAGER ====================
def generate_file_manager_link(user_id, project_id):
    session_id = uuid.uuid4().hex[:16]
    expiry = datetime.now() + timedelta(seconds=FILE_MANAGER_EXPIRY)
    
    project = projects_collection.find_one({"project_id": project_id})
    if not project:
        return None
    
    file_manager_sessions[session_id] = {
        'user_id': user_id,
        'project_id': project_id,
        'expiry': expiry,
        'project_dir': project['project_dir']
    }
    
    return f"{RENDER_URL}/filemanager/{session_id}"

# ==================== 💾 BACKUP SYSTEM ====================
async def backup_system():
    """Background task to backup system state every 5 minutes"""
    while True:
        await asyncio.sleep(BACKUP_INTERVAL)
        try:
            with project_manager.lock:
                running_projects = []
                for pid, info in project_manager.processes.items():
                    running_projects.append({
                        'project_id': pid,
                        'start_time': info['start_time'].isoformat(),
                        'run_command': info['run_command']
                    })
            
            backups_collection.update_one(
                {"type": "system_backup"},
                {"$set": {
                    "timestamp": datetime.now(),
                    "running_projects": running_projects,
                    "total_running": len(running_projects)
                }},
                upsert=True
            )
            print(f"[💾 BACKUP] {datetime.now().strftime('%H:%M:%S')} - {len(running_projects)} projects saved")
        except Exception as e:
            print(f"[❌ BACKUP ERROR] {e}")

def restore_from_backup():
    """Restore running projects from backup on startup"""
    try:
        backup = backups_collection.find_one({"type": "system_backup"})
        if backup and backup.get('running_projects'):
            print(f"[🔄 RESTORE] Found {len(backup['running_projects'])} projects to restore...")
            for proj in backup['running_projects']:
                try:
                    project = projects_collection.find_one({"project_id": proj['project_id']})
                    if project and os.path.exists(project['project_dir']):
                        print(f"  → Restoring {project['project_name']}...")
                        project_manager.run_project(
                            proj['project_id'],
                            Path(project['project_dir']),
                            proj['run_command']
                        )
                        time.sleep(1)  # Small delay between restarts
                except Exception as e:
                    print(f"  ❌ Failed to restore {proj['project_id']}: {e}")
            print("[✅ RESTORE] Complete!")
    except Exception as e:
        print(f"[❌ RESTORE ERROR] {e}")

# ==================== 👤 USER MANAGEMENT ====================
def get_user(user_id):
    return users_collection.find_one({"user_id": user_id})

def create_user(user_id, username=None, first_name=None):
    try:
        users_collection.insert_one({
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "is_premium": False,
            "premium_expiry": None,
            "created_at": datetime.now(),
            "is_banned": False,
            "projects_count": 0,
            "last_activity": datetime.now()
        })
        return True
    except DuplicateKeyError:
        return False

def update_activity(user_id):
    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {"last_activity": datetime.now()}}
    )

def is_premium(user_id):
    user = get_user(user_id)
    if not user:
        return False
    if user.get('is_premium'):
        expiry = user.get('premium_expiry')
        if expiry:
            if isinstance(expiry, str):
                expiry = datetime.fromisoformat(expiry)
            if datetime.now() > expiry:
                users_collection.update_one(
                    {"user_id": user_id},
                    {"$set": {"is_premium": False, "premium_expiry": None}}
                )
                return False
        return True
    return False

def can_create_project(user_id):
    user = get_user(user_id)
    if not user:
        return False, "User not found"
    if user.get('is_banned'):
        return False, "🚫 You are banned from using this bot!"
    
    limit = PREMIUM_PROJECTS_LIMIT if is_premium(user_id) else FREE_PROJECTS_LIMIT
    current = projects_collection.count_documents({"user_id": user_id})
    
    if current >= limit:
        if limit == FREE_PROJECTS_LIMIT:
            return False, f"⚠️ Free users can only create {FREE_PROJECTS_LIMIT} project!\n\n💎 Upgrade to Premium for unlimited projects!"
        return False, "⚠️ Project limit reached!"
    
    return True, None

# ==================== 🔐 DECORATORS ====================
def owner_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            if update.callback_query:
                await update.callback_query.answer("⛔ Owner Only!", show_alert=True)
            else:
                await update.message.reply_text("⛔ This command is for owner only!")
            return
        return await func(update, context)
    return wrapper

def not_banned(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = get_user(update.effective_user.id)
        if user and user.get('is_banned'):
            text = "🚫 **You are banned from using this bot!**\n\nContact support if you think this is a mistake."
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode='Markdown')
            else:
                await update.message.reply_text(text, parse_mode='Markdown')
            return
        return await func(update, context)
    return wrapper

# ==================== 🎹 CONVERSATION STATES ====================
(
    WAITING_PROJECT_NAME,
    WAITING_FILES,
    WAITING_RUN_COMMAND,
    ADMIN_USER_ID,
    ADMIN_PREMIUM_DAYS,
    ADMIN_BAN_REASON
) = range(6)

# ==================== ⌨️ KEYBOARDS ====================
def main_menu_keyboard(user_id):
    is_prem = is_premium(user_id)
    is_owner = user_id == OWNER_ID
    
    status_emoji = "✨" if is_prem else "💎"
    status_text = "Premium Active" if is_prem else "Premium"
    
    buttons = [
        [
            InlineKeyboardButton("🆕 New Project", callback_data="new_project"),
            InlineKeyboardButton("📁 My Projects", callback_data="my_projects")
        ],
        [
            InlineKeyboardButton(f"{status_emoji} {status_text}", callback_data="premium_info"),
            InlineKeyboardButton("📊 Bot Status", callback_data="bot_status")
        ]
    ]
    
    if is_owner:
        buttons.append([InlineKeyboardButton("🔐 Admin Panel", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(buttons)

def project_action_keyboard(project_id, is_running=False):
    run_btn = "⏹️ Stop" if is_running else "▶️ Run"
    run_cb = f"stop_{project_id}" if is_running else f"run_{project_id}"
    
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(run_btn, callback_data=run_cb),
            InlineKeyboardButton("🔄 Restart", callback_data=f"restart_{project_id}")
        ],
        [
            InlineKeyboardButton("📄 Logs", callback_data=f"logs_{project_id}"),
            InlineKeyboardButton("ℹ️ Refresh Status", callback_data=f"status_{project_id}")
        ],
        [
            InlineKeyboardButton("🔧 Edit Command", callback_data=f"edit_cmd_{project_id}"),
            InlineKeyboardButton("📁 File Manager", callback_data=f"filemgr_{project_id}")
        ],
        [
            InlineKeyboardButton("🗑️ Delete Project", callback_data=f"delete_{project_id}")
        ],
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="main_menu")]
    ])

def admin_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Statistics", callback_data="admin_stats"),
            InlineKeyboardButton("👥 User List", callback_data="admin_users")
        ],
        [
            InlineKeyboardButton("💎 Premium Users", callback_data="admin_premium"),
            InlineKeyboardButton("▶️ Running", callback_data="admin_running")
        ],
        [
            InlineKeyboardButton("➕ Give Premium", callback_data="admin_give_premium"),
            InlineKeyboardButton("➖ Remove Premium", callback_data="admin_remove_premium")
        ],
        [
            InlineKeyboardButton("⏱️ Temp Premium", callback_data="admin_temp_premium"),
            InlineKeyboardButton("📥 Download Project", callback_data="admin_download")
        ],
        [
            InlineKeyboardButton("🚫 Ban User", callback_data="admin_ban"),
            InlineKeyboardButton("✅ Unban User", callback_data="admin_unban")
        ],
        [
            InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
            InlineKeyboardButton("🔙 Back", callback_data="main_menu")
        ]
    ])

# ==================== 🤖 BOT HANDLERS ====================
@not_banned
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name
    
    user = get_user(user_id)
    if not user:
        create_user(user_id, username, first_name)
        print(f"[👤 NEW USER] {user_id} - {first_name}")
    else:
        update_activity(user_id)
    
    is_prem = is_premium(user_id)
    
    welcome_text = f"""
🌟 **Welcome to God Madara Hosting Bot!** 🌟

👋 Hello **{first_name}**!

🚀 **What I can do:**
• Host Python projects **24/7**
• **Web File Manager** - Edit files in browser
• **Auto-install** requirements.txt
• Real-time **logs & monitoring**
• **Free**: {FREE_PROJECTS_LIMIT} project | **Premium**: Unlimited

📊 **Your Status:**
👤 ID: `{user_id}`
💎 Plan: {'**Premium** ✨' if is_prem else '**Free**'}
📁 Projects: {projects_collection.count_documents({"user_id": user_id})}/{'∞' if is_prem else FREE_PROJECTS_LIMIT}

**Choose an option below:**
    """
   
    await update.message.reply_text(
        welcome_text,
        parse_mode='Markdown',
        reply_markup=main_menu_keyboard(user_id)
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = query.data
   
    # Update activity
    update_activity(user_id)
   
    # Main Menu
    if data == "main_menu":
        await query.edit_message_text(
            "🏠 **Main Menu**\n\nChoose what you want to do:",
            parse_mode='Markdown',
            reply_markup=main_menu_keyboard(user_id)
        )
        return
   
    # New Project
    elif data == "new_project":
        can_create, error = can_create_project(user_id)
        if not can_create:
            await query.edit_message_text(
                f"❌ **Error**\n\n{error}",
                parse_mode='Markdown',
                reply_markup=main_menu_keyboard(user_id)
            )
            return
       
        await query.edit_message_text(
            "📝 **Create New Project**\n\n"
            "Please send me a **name** for your project:\n"
            "_Example: my_bot, telegram_app, etc._\n\n"
            "❌ Use /cancel to abort",
            parse_mode='Markdown',
                reply_markup=main_menu_keyboard(user_id)
            )
            return
       
        await query.edit_message_text(
            "📝 **Create New Project**\n\n"
            "Please send me a **name** for your project:\n"
            "_Example: my_bot, telegram_app, etc._\n\n"
            "❌ Use /cancel to abort",
            parse_mode='Markdown'
        )
        return WAITING_PROJECT_NAME
   
    # My Projects
    elif data == "my_projects":
        projects = list(projects_collection.find({"user_id": user_id}))
       
        if not projects:
            await query.edit_message_text(
                "📭 **No Projects Found**\n\n"
                "You don't have any projects yet!\n"
                "Click '**New Project**' to create one.",
                parse_mode='Markdown',
                reply_markup=main_menu_keyboard(user_id)
            )
            return
       
        text = f"📁 **Your Projects** ({len(projects)})\n\n"
        buttons = []
       
        for proj in projects:
            status = project_manager.get_status(proj['project_id'])
            emoji = "🟢" if status and status['is_running'] else "🔴"
            text += f"{emoji} **{proj['project_name']}**\n"
            text += f"   `ID: {proj['project_id']}`\n"
            if status:
                text += f"   Status: {status['status']} | Uptime: {status['uptime']}\n"
            text += "\n"
           
            buttons.append([InlineKeyboardButton(
                f"🔧 Manage: {proj['project_name']}",
                callback_data=f"manage_{proj['project_id']}"
            )])
       
        buttons.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="main_menu")])
       
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
   
    # Manage Project
    elif data.startswith("manage_"):
        project_id = data.replace("manage_", "")
        project = projects_collection.find_one({"project_id": project_id, "user_id": user_id})
       
        if not project:
            await query.answer("❌ Project not found!", show_alert=True)
            return
       
        status = project_manager.get_status(project_id)
       
        text = f"""
📊 **Project Dashboard: {project['project_name']}**

🔹 **Status:** {status['status'] if status else 'Unknown'}
🔹 **PID:** `{status['pid'] if status else 'N/A'}`
🔹 **Uptime:** {status['uptime'] if status else 'N/A'}
🔹 **Last Run:** {status['last_run'] if status else 'Never'}
🔹 **Exit Code:** `{status['exit_code'] if status else 'None'}`
🔹 **Run Command:** `{status['run_command'] if status else 'python3 main.py'}`

📅 Created: {project.get('created_at', 'Unknown').strftime('%Y-%m-%d %H:%M') if isinstance(project.get('created_at'), datetime) else 'Unknown'}
        """
       
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=project_action_keyboard(project_id, status['is_running'] if status else False)
        )
        return
   
    # Run Project
    elif data.startswith("run_"):
        project_id = data.replace("run_", "")
        project = projects_collection.find_one({"project_id": project_id, "user_id": user_id})
       
        if not project:
            await query.answer("❌ Project not found!", show_alert=True)
            return
       
        # Check if already running
        status = project_manager.get_status(project_id)
        if status and status['is_running']:
            await query.answer("⚠️ Already running!", show_alert=True)
            return
       
        await query.answer("🚀 Starting project...")
       
        success, msg = project_manager.run_project(
            project_id,
            Path(project['project_dir']),
            project.get('run_command', 'python3 main.py')
        )
       
        if success:
            projects_collection.update_one(
                {"project_id": project_id},
                {"$set": {"last_run": datetime.now().isoformat(), "crashed": False}}
            )
            await query.answer("✅ Started!", show_alert=True)
        else:
            await query.answer(f"❌ {msg[:200]}", show_alert=True)
       
        # Refresh view
        await asyncio.sleep(1)
        context.user_data['refresh_manage'] = project_id
        await button_handler(update, context)
        return
   
    # Stop Project
    elif data.startswith("stop_"):
        project_id = data.replace("stop_", "")
       
        await query.answer("⏹️ Stopping project...")
        success, msg = project_manager.stop_project(project_id)
       
        if success:
            await query.answer("⏹️ Stopped!", show_alert=True)
        else:
            await query.answer(f"⚠️ {msg}", show_alert=True)
       
        await asyncio.sleep(1)
        context.user_data['refresh_manage'] = project_id
        await button_handler(update, context)
        return
   
    # Restart Project
    elif data.startswith("restart_"):
        project_id = data.replace("restart_", "")
        project = projects_collection.find_one({"project_id": project_id, "user_id": user_id})
       
        if not project:
            await query.answer("❌ Project not found!", show_alert=True)
            return
       
        await query.answer("🔄 Restarting...")
       
        success, msg = project_manager.restart_project(
            project_id,
            Path(project['project_dir']),
            project.get('run_command', 'python3 main.py')
        )
       
        if success:
            projects_collection.update_one(
                {"project_id": project_id},
                {"$set": {"last_run": datetime.now().isoformat(), "crashed": False}}
            )
            await query.answer("🔄 Restarted!", show_alert=True)
        else:
            await query.answer(f"❌ {msg[:200]}", show_alert=True)
       
        await asyncio.sleep(1)
        context.user_data['refresh_manage'] = project_id
        await button_handler(update, context)
        return
   
    # Get Logs
    elif data.startswith("logs_"):
        project_id = data.replace("logs_", "")
       
        await query.answer("📄 Fetching logs...")
       
        logs = project_manager.get_logs(project_id, lines=300)
       
        if logs and not logs.startswith("❌") and not logs.startswith("📭"):
            # Save to temp file
            temp_file = f"/tmp/logs_{project_id}.txt"
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    f.write(f"Logs for Project: {project_id}\n")
                    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("=" * 50 + "\n\n")
                    f.write(logs)
               
                await query.message.reply_document(
                    document=open(temp_file, 'rb'),
                    filename=f"logs_{project_id}.txt",
                    caption=f"📄 **Logs for** `{project_id}`",
                    parse_mode='Markdown'
                )
                os.remove(temp_file)
            except Exception as e:
                await query.answer(f"❌ Error: {str(e)[:100]}", show_alert=True)
        else:
            await query.message.reply_text(
                f"📄 **Logs**\n\n```\n{logs}\n```",
                parse_mode='Markdown'
            )
        return
   
    # Refresh Status
    elif data.startswith("status_"):
        await query.answer("🔄 Refreshing...")
        context.user_data['refresh_manage'] = data.replace("status_", "")
        await button_handler(update, context)
        return
   
    # Edit Run Command
    elif data.startswith("edit_cmd_"):
        project_id = data.replace("edit_cmd_", "")
        context.user_data['editing_project'] = project_id
       
        project = projects_collection.find_one({"project_id": project_id, "user_id": user_id})
        current_cmd = project.get('run_command', 'python3 main.py') if project else 'python3 main.py'
       
        await query.edit_message_text(
            f"✏️ **Edit Run Command**\n\n"
            f"Current: `{current_cmd}`\n\n"
            f"Send me the new command:\n"
            f"_Examples:_\n"
            f"• `python3 main.py`\n"
            f"• `python3 bot.py --port 8080`\n"
            f"• `gunicorn app:app`\n\n"
            f"❌ Send /cancel to abort",
            parse_mode='Markdown'
        )
        return WAITING_RUN_COMMAND
   
    # File Manager
    elif data.startswith("filemgr_"):
        project_id = data.replace("filemgr_", "")
       
        link = generate_file_manager_link(user_id, project_id)
       
        if not link:
            await query.answer("❌ Error generating link!", show_alert=True)
            return
       
        await query.edit_message_text(
            f"""
📁 **File Manager Access**

🔗 **Link:** `{link}`

⏱️ **Valid for:** 10 minutes
🔒 **Security:** Single-use session

**You can:**
• Edit files online ✏️
• Upload new files 📤
• Delete/Rename files 🗑️
• Create new files 📄
• Download files ⬇️

⚠️ **Note:** Link expires in 10 minutes for security!
            """,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh Status", callback_data=f"manage_{project_id}")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data="main_menu")]
            ])
        )
        return
   
    # Delete Project
    elif data.startswith("delete_"):
        project_id = data.replace("delete_", "")
       
        # Confirm first
        if not context.user_data.get(f'confirm_delete_{project_id}'):
            context.user_data[f'confirm_delete_{project_id}'] = True
            await query.answer("⚠️ Tap again to confirm deletion!", show_alert=True)
           
            await query.edit_message_text(
                "🗑️ **Delete Project?**\n\n"
                "⚠️ This will permanently delete:\n"
                "• All project files\n"
                "• All logs and data\n"
                "• Running process (if any)\n\n"
                "🔴 **This action cannot be undone!**\n\n"
                "Tap 🗑️ Delete again to confirm, or go back.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑️ Confirm Delete", callback_data=f"delete_{project_id}")],
                    [InlineKeyboardButton("🔙 Cancel", callback_data=f"manage_{project_id}")]
                ])
            )
            return
       
        # Actually delete
        project = projects_collection.find_one({"project_id": project_id, "user_id": user_id})
       
        if project:
            # Stop if running
            project_manager.stop_project(project_id)
           
            # Delete directory
            try:
                shutil.rmtree(project['project_dir'], ignore_errors=True)
            except:
                pass
           
            # Delete from DB
            projects_collection.delete_one({"project_id": project_id})
           
            await query.edit_message_text(
                f"🗑️ **Project Deleted!**\n\n"
                f"Project `{project_id}` has been permanently removed.",
                parse_mode='Markdown',
                reply_markup=main_menu_keyboard(user_id)
            )
       
        # Clear confirmation flag
        context.user_data.pop(f'confirm_delete_{project_id}', None)
        return
   
    # Premium Info
    elif data == "premium_info":
        is_prem = is_premium(user_id)
       
        text = f"""
💎 **Premium Membership**

{'✨ **You are Premium!** ✨' if is_prem else '👤 **You are on Free Plan**'}

**Free Plan:**
• {FREE_PROJECTS_LIMIT} Project only
• Basic file manager (10 min)
• Community support

**Premium Plan:**
• ✅ **Unlimited** projects
• ✅ Priority support
• ✅ Extended file manager time
• ✅ Advanced monitoring
• ✅ Auto-restart on crash

{'Your premium is active!' if is_prem else 'Contact @YourUsername to upgrade!'}
        """
       
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=main_menu_keyboard(user_id)
        )
        return
   
    # Bot Status
    elif data == "bot_status":
        total_users = users_collection.count_documents({})
        total_projects = projects_collection.count_documents({})
        running_projects = len(project_manager.processes)
        premium_count = users_collection.count_documents({"is_premium": True})
       
        # System stats
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
           
            sys_stats = f"""
🖥️ **System:**
CPU: {cpu_percent}% | RAM: {memory.percent}%
Disk: {disk.percent}% used
            """
        except:
            sys_stats = ""
       
        text = f"""
📊 **Bot Status Dashboard**

👥 **Users:** {total_users}
💎 **Premium:** {premium_count}
📁 **Projects:** {total_projects}
▶️ **Running:** {running_projects}

{sys_stats}

⏱️ **Uptime:** Running
💾 **Database:** MongoDB ✅
🔧 **Version:** 3.0 Final
        """
       
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=main_menu_keyboard(user_id)
        )
        return
   
    # ==================== ADMIN PANEL ====================
    elif data == "admin_panel":
        if user_id != OWNER_ID:
            await query.answer("⛔ Owner Only!", show_alert=True)
            return
       
        await query.edit_message_text(
            "🔐 **Admin Panel**\n\n"
            "Select an action:",
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
        return
   
    # Admin Stats
    elif data == "admin_stats":
        if user_id != OWNER_ID:
            return
       
        total_users = users_collection.count_documents({})
        total_projects = projects_collection.count_documents({})
        running = len(project_manager.processes)
        premium = users_collection.count_documents({"is_premium": True})
        banned = users_collection.count_documents({"is_banned": True})
       
        # Recent users (last 24h)
        yesterday = datetime.now() - timedelta(days=1)
        recent = users_collection.count_documents({"last_activity": {"$gte": yesterday}})
       
        text = f"""
📊 **Admin Statistics**

👥 Total Users: {total_users}
👤 Active (24h): {recent}
💎 Premium: {premium}
🚫 Banned: {banned}
📁 Total Projects: {total_projects}
▶️ Running: {running}
        """
       
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
        return
   
    # Admin Users List
    elif data == "admin_users":
        if user_id != OWNER_ID:
            return
       
        users = list(users_collection.find({}).limit(50).sort("created_at", -1))
       
        text = f"👥 **Recent Users** (showing {len(users)})\n\n"
       
        for user in users:
            status = "💎" if user.get('is_premium') else "👤"
            if user.get('is_banned'):
                status = "🚫"
            name = user.get('first_name', 'Unknown')[:15]
            text += f"{status} `{user['user_id']}` - {name}\n"
       
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
        return
   
    # Admin Premium Users
    elif data == "admin_premium":
        if user_id != OWNER_ID:
            return
       
        premium_users = list(users_collection.find({"is_premium": True}))
       
        text = f"💎 **Premium Users** ({len(premium_users)})\n\n"
       
        for user in premium_users:
            expiry = user.get('premium_expiry', 'Permanent')
            if isinstance(expiry, datetime):
                expiry = expiry.strftime('%Y-%m-%d')
            elif isinstance(expiry, str):
                try:
                    expiry = datetime.fromisoformat(expiry).strftime('%Y-%m-%d')
                except:
                    pass
           
            name = user.get('first_name', 'Unknown')[:15]
            text += f"• `{user['user_id']}` - {name}\n  Expires: {expiry}\n\n"
       
        if not premium_users:
            text += "No premium users yet."
       
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
        return
   
    # Admin Running Projects
    elif data == "admin_running":
        if user_id != OWNER_ID:
            return
       
        running = list(project_manager.processes.keys())
       
        text = f"▶️ **Running Projects** ({len(running)})\n\n"
       
        for pid in running:
            proj = projects_collection.find_one({"project_id": pid})
            if proj:
                user = users_collection.find_one({"user_id": proj['user_id']})
                user_name = user.get('first_name', 'Unknown') if user else 'Unknown'
                uptime = project_manager.processes[pid]['start_time']
                delta = datetime.now() - uptime
                text += f"• {proj['project_name']}\n"
                text += f"  User: {user_name} (`{proj['user_id']}`)\n"
                text += f"  Uptime: {str(delta).split('.')[0]}\n\n"
       
        if not running:
            text += "No projects currently running."
       
        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
        return
   
    # Admin Actions with Input
    elif data in ["admin_give_premium", "admin_remove_premium", "admin_ban", "admin_unban", "admin_temp_premium", "admin_download"]:
        if user_id != OWNER_ID:
            return
       
        context.user_data['admin_action'] = data
       
        action_names = {
            "admin_give_premium": "➕ Give Premium (Permanent)",
            "admin_remove_premium": "➖ Remove Premium",
            "admin_ban": "🚫 Ban User",
            "admin_unban": "✅ Unban User",
            "admin_temp_premium": "⏱️ Give Temporary Premium",
            "admin_download": "📥 Download User's Projects"
        }
       
        await query.edit_message_text(
            f"{action_names[data]}\n\n"
            f"Send me the **User ID** (numeric):\n\n"
            f"❌ Send /cancel to abort",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Cancel", callback_data="admin_panel")]
            ])
        )
        return ADMIN_USER_ID
   
    # Handle refresh from context
    if context.user_data.get('refresh_manage'):
        project_id = context.user_data.pop('refresh_manage')
        data = f"manage_{project_id}"
        await button_handler(update, context)
        return

# ==================== 📨 MESSAGE HANDLERS ====================
async def receive_project_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
   
    if not name:
        await update.message.reply_text("❌ Name cannot be empty! Try again:")
        return WAITING_PROJECT_NAME
   
    if len(name) > 50:
        await update.message.reply_text("❌ Name too long (max 50 chars)! Try again:")
        return WAITING_PROJECT_NAME
   
    # Check for invalid characters
    invalid_chars = '<>:"/\\|?*'
    if any(c in name for c in invalid_chars):
        await update.message.reply_text("❌ Name contains invalid characters! Try again:")
        return WAITING_PROJECT_NAME
   
    context.user_data['project_name'] = name
   
    await update.message.reply_text(
        f"✅ **Project Name:** `{name}`\n\n"
        f"📤 **Now send me your files:**\n\n"
        f"You can:\n"
        f"• Upload a **ZIP** archive\n"
        f"• Upload multiple **.py** files one by one\n"
        f"• Include **requirements.txt** for auto-install\n\n"
        f"✅ Send /done when finished uploading\n"
        f"❌ Send /cancel to abort",
        parse_mode='Markdown'
    )
   
    context.user_data['files'] = []
    return WAITING_FILES

async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
   
    if not document:
        await update.message.reply_text("❌ Please send a file (not a photo/text)!")
        return WAITING_FILES
   
    file_name = document.file_name
    file_id = document.file_id
   
    # Validate file type
    allowed_exts = ['.py', '.zip', '.txt', '.json', '.env', '.md', '.yml', '.yaml', '.cfg', '.ini']
    if not any(file_name.lower().endswith(ext) for ext in allowed_exts):
        await update.message.reply_text(
            f"⚠️ **Warning:** `{file_name}` may not be a valid Python project file.\n"
            f"Allowed: .py, .zip, .txt, .json, .env, etc.\n\n"
            f"Send /done when finished or upload more files."
        )
   
    try:
        # Download file
        file = await context.bot.get_file(file_id)
       
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, file_name)
        await file.download_to_drive(file_path)
       
        context.user_data['files'].append({
            'path': file_path,
            'name': file_name,
            'is_zip': file_name.lower().endswith('.zip'),
            'temp_dir': temp_dir
        })
       
        await update.message.reply_text(
            f"📥 **Received:** `{file_name}`\n"
            f"📊 **Total files:** {len(context.user_data['files'])}\n\n"
            f"Send more files or type **/done** to finish",
            parse_mode='Markdown'
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error downloading file: {str(e)[:100]}")
   
    return WAITING_FILES

async def done_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    project_name = context.user_data.get('project_name')
    files = context.user_data.get('files', [])
   
    if not files:
        await update.message.reply_text(
            "❌ **No files uploaded!**\n\n"
            "Please upload at least one file.\n"
            "Send /cancel to abort."
        )
        return WAITING_FILES
   
    # Send processing message
    processing_msg = await update.message.reply_text("⚙️ **Processing files...**")
   
    try:
        # Create project
        project_id = project_manager.generate_project_id(user_id, project_name)
        project_dir = project_manager.get_project_dir(user_id, project_name)
       
        # Extract files
        for file_info in files:
            try:
                if file_info['is_zip']:
                    with zipfile.ZipFile(file_info['path'], 'r') as zip_ref:
                        zip_ref.extractall(project_dir)
                else:
                    shutil.copy(file_info['path'], project_dir / file_info['name'])
            finally:
                # Cleanup temp
                try:
                    shutil.rmtree(file_info['temp_dir'])
                except:
                    pass
       
        # Auto-install requirements
        await processing_msg.edit_text("📦 **Installing requirements...**")
        installed, install_output = project_manager.install_requirements(project_dir)
       
        # Determine main file
        main_candidates = ['main.py', 'bot.py', 'app.py', 'run.py', 'start.py']
        main_file = None
        for candidate in main_candidates:
            if (project_dir / candidate).exists():
                main_file = candidate
                break
       
        # If no main file found, use first .py file
        if not main_file:
            py_files = list(project_dir.glob("*.py"))
            if py_files:
                main_file = py_files[0].name
       
        run_command = f"python3 {main_file}" if main_file else "python3 main.py"
       
        # Save to DB
        projects_collection.insert_one({
            "project_id": project_id,
            "user_id": user_id,
            "project_name": project_name,
            "project_dir": str(project_dir),
            "created_at": datetime.now(),
            "run_command": run_command,
            "last_run": None,
            "crashed": False,
            "last_exit_code": None
        })
       
        # Update user count
        users_collection.update_one(
            {"user_id": user_id},
            {"$inc": {"projects_count": 1}}
        )
       
        # Build success message
        success_text = f"""
✅ **Project Created Successfully!**

📝 **Name:** {project_name}
🆔 **ID:** `{project_id}`
📁 **Location:** `{project_dir}`

**Files detected:**
        """
       
        # List files
        file_list = list(project_dir.iterdir())
        for f in file_list[:10]:  # Show first 10
            emoji = "📁" if f.is_dir() else "📄"
            if f.name == 'requirements.txt':
                emoji = "📦"
            elif f.suffix == '.py':
                emoji = "🐍"
            success_text += f"\n{emoji} `{f.name}`"
       
        if len(file_list) >  10:
            success_text += f"\n... and {len(file_list) - 10} more"
       
        success_text += f"\n\n{'✅ Auto-installed requirements' if installed else '⚠️ No requirements.txt found'}"
       
        if install_output and len(install_output) < 500:
            success_text += f"\n📋 **Install log:**\n```\n{install_output[-400:]}\n```"
       
        success_text += f"\n\n🚀 **Run Command:** `{run_command}`"
       
        await processing_msg.delete()
        await update.message.reply_text(
            success_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Start Project", callback_data=f"run_{project_id}")],
                [InlineKeyboardButton("📁 Manage Project", callback_data=f"manage_{project_id}")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
            ])
        )
       
    except Exception as e:
        await processing_msg.edit_text(
            f"❌ **Error creating project:**\n\n`{str(e)[:400]}`\n\nPlease try again with /new_project"
        )
        # Cleanup on error
        try:
            if 'project_dir' in locals():
                shutil.rmtree(project_dir, ignore_errors=True)
        except:
            pass
   
    return ConversationHandler.END

async def receive_run_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_cmd = update.message.text.strip()
    project_id = context.user_data.get('editing_project')
   
    if not project_id:
        await update.message.reply_text("❌ Session expired! Start again.")
        return ConversationHandler.END
   
    # Validate command
    if not new_cmd or len(new_cmd) > 200:
        await update.message.reply_text("❌ Invalid command! Try again:")
        return WAITING_RUN_COMMAND
   
    # Update in DB
    projects_collection.update_one(
        {"project_id": project_id},
        {"$set": {"run_command": new_cmd}}
    )
   
    await update.message.reply_text(
        f"✅ **Run command updated!**\n\nNew command:\n`{new_cmd}`",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Project", callback_data=f"manage_{project_id}")]
        ])
    )
   
    return ConversationHandler.END

# ==================== 🔐 ADMIN MESSAGE HANDLERS ====================
async def admin_receive_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    action = context.user_data.get('admin_action')
   
    try:
        target_user_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID! Must be a number.")
        return ConversationHandler.END
   
    context.user_data['target_user_id'] = target_user_id
   
    if action == "admin_give_premium":
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"is_premium": True, "premium_expiry": None}},
            upsert=True
        )
        await update.message.reply_text(
            f"✅ **Premium given to** `{target_user_id}`\n(Permanent)",
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
   
    elif action == "admin_remove_premium":
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"is_premium": False, "premium_expiry": None}}
        )
        await update.message.reply_text(
            f"✅ **Premium removed from** `{target_user_id}`",
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
   
    elif action == "admin_ban":
        # Stop all their projects first
        user_projects = list(projects_collection.find({"user_id": target_user_id}))
        for proj in user_projects:
            project_manager.stop_project(proj['project_id'])
       
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"is_banned": True}},
            upsert=True
        )
        await update.message.reply_text(
            f"🚫 **User banned:** `{target_user_id}`\n"
            f"Stopped {len(user_projects)} running projects.",
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
   
    elif action == "admin_unban":
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"is_banned": False}}
        )
        await update.message.reply_text(
            f"✅ **User unbanned:** `{target_user_id}`",
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
   
    elif action == "admin_temp_premium":
        await update.message.reply_text(
            "⏱️ **Send number of days** for temporary premium:\n"
            "Example: `30` for 30 days",
            parse_mode='Markdown'
        )
        return ADMIN_PREMIUM_DAYS
   
    elif action == "admin_download":
        # Find user's projects
        user_projects = list(projects_collection.find({"user_id": target_user_id}))
        if not user_projects:
            await update.message.reply_text(
                f"❌ No projects found for user `{target_user_id}`",
                parse_mode='Markdown',
                reply_markup=admin_keyboard()
            )
            return ConversationHandler.END
       
        # Send processing message
        msg = await update.message.reply_text("📦 **Creating ZIP archive...**")
       
        try:
            zip_path = f"/tmp/admin_user_{target_user_id}_projects.zip"
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for proj in user_projects:
                    proj_dir = Path(proj['project_dir'])
                    if proj_dir.exists():
                        for file_path in proj_dir.rglob('*'):
                            if file_path.is_file():
                                arcname = f"{proj['project_name']}/{file_path.relative_to(proj_dir)}"
                                zipf.write(file_path, arcname)
           
            await msg.delete()
            await update.message.reply_document(
                document=open(zip_path, 'rb'),
                filename=f"user_{target_user_id}_projects.zip",
                caption=f"📥 **Projects of user** `{target_user_id}`\n"
                        f"Total projects: {len(user_projects)}",
                parse_mode='Markdown'
            )
            os.remove(zip_path)
        except Exception as e:
            await msg.edit_text(f"❌ Error: {str(e)[:200]}")
   
    return ConversationHandler.END

async def admin_receive_premium_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        days = int(update.message.text.strip())
        if days <= 0 or days > 3650:  # Max 10 years
            raise ValueError("Invalid days")
       
        target_user_id = context.user_data.get('target_user_id')
       
        expiry = datetime.now() + timedelta(days=days)
        users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"is_premium": True, "premium_expiry": expiry}},
            upsert=True
        )
       
        await update.message.reply_text(
            f"⏱️ **Temporary Premium Given!**\n\n"
            f"User: `{target_user_id}`\n"
            f"Duration: **{days} days**\n"
            f"Expires: **{expiry.strftime('%Y-%m-%d')}**",
            parse_mode='Markdown',
            reply_markup=admin_keyboard()
        )
    except ValueError:
        await update.message.reply_text("❌ Invalid number! Send a number between 1 and 3650.")
        return ADMIN_PREMIUM_DAYS
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:200]}")
   
    return ConversationHandler.END

async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❌ **Operation cancelled.**",
        reply_markup=main_menu_keyboard(update.effective_user.id)
    )
    return ConversationHandler.END

# ==================== 🌐 FLASK FILE MANAGER ====================
flask_app = Flask(__name__)
flask_app.secret_key = os.urandom(24)

FILE_MANAGER_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>God Madara - File Manager</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', system-ui, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 15px;
        }
        .header h1 { font-size: 24px; display: flex; align-items: center; gap: 10px; }
        .expire {
            background: rgba(255,255,255,0.2);
            padding: 10px 20px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: 500;
        }
        .toolbar {
            background: #f8f9fa;
            padding: 20px 30px;
            border-bottom: 1px solid #e9ecef;
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }
        .btn {
            padding: 10px 20px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            transition: all 0.3s;
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 5px;
        }
        .btn-primary { background: #667eea; color: white; }
        .btn-success { background: #28a745; color: white; }
        .btn-danger { background: #dc3545; color: white; }
        .btn-warning { background: #ffc107; color: #000; }
        .btn:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(0,0,0,0.2); }
        .content { padding: 30px; }
        .file-list { list-style: none; }
        .file-item {
            display: flex;
            align-items: center;
            padding: 15px;
            border-bottom: 1px solid #e9ecef;
            transition: all 0.2s;
            gap: 15px;
        }
        .file-item:hover { background: #f8f9fa; }
        .file-icon {
            width: 45px;
            height: 45px;
            background: #e3f2fd;
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 22px;
            flex-shrink: 0;
        }
        .file-info { flex: 1; min-width: 0; }
        .file-name {
            font-weight: 600;
            color: #333;
            margin-bottom: 4px;
            word-break: break-all;
        }
        .file-meta { font-size: 12px; color: #666; }
        .file-actions { display: flex; gap: 5px; flex-shrink: 0; }
        .breadcrumb {
            padding: 15px 30px;
            background: #e9ecef;
            font-size: 14px;
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }
        .breadcrumb a {
            color: #667eea;
            text-decoration: none;
            padding: 4px 8px;
            border-radius: 4px;
            transition: background 0.2s;
        }
        .breadcrumb a:hover { background: rgba(102,126,234,0.1); }
        .editor-container {
            padding: 30px;
        }
        .editor-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 15px;
        }
        .editor {
            width: 100%;
            height: 70vh;
            font-family: 'Consolas', 'Monaco', monospace;
            font-size: 14px;
            padding: 20px;
            border: 2px solid #e9ecef;
            border-radius: 12px;
            resize: vertical;
            line-height: 1.6;
            tab-size: 4;
        }
        .editor:focus {
            outline: none;
            border-color: #667eea;
        }
        .flash {
            padding: 15px 30px;
            background: #d4edda;
            color: #155724;
            border-bottom: 1px solid #c3e6cb;
        }
        .flash-error {
            background: #f8d7da;
            color: #721c24;
            border-bottom-color: #f5c6cb;
        }
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #666;
        }
        .empty-state-icon {
            font-size: 64px;
            margin-bottom: 20px;
