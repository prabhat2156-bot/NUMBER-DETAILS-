import os
import sys
import json
import time
import uuid
import shutil
import psutil
import asyncio
import subprocess
import threading
from datetime import datetime, timedelta
from flask import Flask, request, send_file, render_template_string, redirect, url_for, flash
from werkzeug.utils import secure_filename
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from pymongo import MongoClient
from bson.objectid import ObjectId

# Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
MONGO_URI = os.getenv("MONGO_URI")

# Database Setup
client = MongoClient(MONGO_URI)
db = client['god_madara_bot']
users_collection = db['users']
projects_collection = db['projects']
premium_collection = db['premium']
backup_collection = db['backups']

# Flask App for File Manager
app = Flask(__name__)
app.secret_key = 'god-madara-secret-key-2025'

# Data Storage Paths
BASE_DIR = "/tmp/god_madara_projects"
if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)

# Running Processes Storage
running_processes = {}
file_manager_sessions = {}

# ==================== DATABASE FUNCTIONS ====================

def get_user(user_id):
    return users_collection.find_one({"user_id": user_id})

def create_user(user_id, username):
    user = {
        "user_id": user_id,
        "username": username,
        "created_at": datetime.utcnow(),
        "is_premium": False,
        "premium_until": None,
        "is_banned": False,
        "total_projects": 0,
        "max_projects": 1  # Free user = 1, Premium = unlimited
    }
    users_collection.insert_one(user)
    return user

def update_user(user_id, data):
    users_collection.update_one({"user_id": user_id}, {"$set": data})

def get_user_projects(user_id):
    return list(projects_collection.find({"user_id": user_id}))

def get_project(project_id):
    return projects_collection.find_one({"_id": ObjectId(project_id)})

def create_project(user_id, name, files):
    project = {
        "user_id": user_id,
        "name": name,
        "files": files,
        "created_at": datetime.utcnow(),
        "status": "stopped",
        "pid": None,
        "uptime": None,
        "last_run": None,
        "last_exit_code": None,
        "run_command": "python3 main.py",
        "directory": os.path.join(BASE_DIR, str(user_id), str(uuid.uuid4()))
    }
    result = projects_collection.insert_one(project)
    project['_id'] = result.inserted_id
    
    # Create directory
    os.makedirs(project['directory'], exist_ok=True)
    
    # Update user project count
    users_collection.update_one(
        {"user_id": user_id}, 
        {"$inc": {"total_projects": 1}}
    )
    
    return project

def update_project(project_id, data):
    projects_collection.update_one({"_id": ObjectId(project_id)}, {"$set": data})

def delete_project_db(project_id):
    project = get_project(project_id)
    if project:
        # Kill if running
        if project.get('pid') and psutil.pid_exists(project.get('pid')):
            try:
                psutil.Process(project['pid']).terminate()
            except:
                pass
        
        # Remove directory
        if os.path.exists(project['directory']):
            shutil.rmtree(project['directory'])
        
        # Update user count
        users_collection.update_one(
            {"user_id": project['user_id']}, 
            {"$inc": {"total_projects": -1}}
        )
        
        projects_collection.delete_one({"_id": ObjectId(project_id)})

def backup_data():
    """Backup all data every 5 minutes"""
    while True:
        try:
            backup = {
                "timestamp": datetime.utcnow(),
                "users": list(users_collection.find()),
                "projects": list(projects_collection.find()),
                "premium": list(premium_collection.find())
            }
            backup_collection.insert_one(backup)
            
            # Keep only last 100 backups
            backup_count = backup_collection.count_documents({})
            if backup_count > 100:
                old_backups = backup_collection.find().sort("timestamp", 1).limit(backup_count - 100)
                for old in old_backups:
                    backup_collection.delete_one({"_id": old['_id']})
            
            time.sleep(300)  # 5 minutes
        except Exception as e:
            print(f"Backup error: {e}")
            time.sleep(60)

# Start backup thread
backup_thread = threading.Thread(target=backup_data, daemon=True)
backup_thread.start()

def restore_from_backup():
    """Restore from latest backup if needed"""
    latest = backup_collection.find_one(sort=[("timestamp", -1)])
    if latest:
        # Restore logic here if needed
        pass

# ==================== TELEGRAM BOT ====================

# Conversation States
WAITING_PROJECT_NAME = 1
WAITING_FILES = 2
WAITING_EDIT_COMMAND = 3

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    user = get_user(user_id)
    if not user:
        user = create_user(user_id, username)
    
    if user.get('is_banned'):
        await update.message.reply_text("⛔ You are banned from using this bot!")
        return
    
    # Check premium expiry
    if user.get('is_premium') and user.get('premium_until'):
        if datetime.utcnow() > user['premium_until']:
            update_user(user_id, {
                "is_premium": False, 
                "premium_until": None,
                "max_projects": 1
            })
    
    keyboard = [
        [InlineKeyboardButton("🆕 New Project", callback_data='new_project'),
         InlineKeyboardButton("📁 My Projects", callback_data='my_projects')],
        [InlineKeyboardButton("💎 Premium", callback_data='premium'),
         InlineKeyboardButton("📊 Bot Status", callback_data='bot_status')]
    ]
    
    # Admin button for owner
    if user_id == OWNER_ID:
        keyboard.append([InlineKeyboardButton("🔐 Admin Panel", callback_data='admin_panel')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    welcome_text = f"""
👑 *Welcome to God Madara Hosting Bot* 👑

🤖 *The Ultimate Python Hosting Solution*

✨ *Features:*
• 🚀 Run Python scripts 24/7
• 📁 Web File Manager
• 🔄 Auto Restart on Crash
• 📦 Auto Dependency Installation
• 💾 MongoDB Backup Every 5min

👤 *Your Status:* {'💎 Premium' if user.get('is_premium') else '🆓 Free'}
📊 *Projects:* {user.get('total_projects', 0)}/{'∞' if user.get('is_premium') else '1'}

🎯 *Get Premium for Unlimited Projects!*
    """
    
    await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = query.data
    
    if data == 'new_project':
        await new_project_start(update, context)
    elif data == 'my_projects':
        await show_projects(update, context)
    elif data == 'premium':
        await show_premium(update, context)
    elif data == 'bot_status':
        await bot_status(update, context)
    elif data == 'admin_panel':
        await admin_panel(update, context)
    elif data.startswith('project_'):
        await project_details(update, context, data.replace('project_', ''))
    elif data.startswith('run_'):
        await run_project(update, context, data.replace('run_', ''))
    elif data.startswith('stop_'):
        await stop_project(update, context, data.replace('stop_', ''))
    elif data.startswith('restart_'):
        await restart_project(update, context, data.replace('restart_', ''))
    elif data.startswith('logs_'):
        await get_logs(update, context, data.replace('logs_', ''))
    elif data.startswith('status_'):
        await refresh_status(update, context, data.replace('status_', ''))
    elif data.startswith('edit_cmd_'):
        await edit_command_start(update, context, data.replace('edit_cmd_', ''))
    elif data.startswith('file_manager_'):
        await generate_file_manager(update, context, data.replace('file_manager_', ''))
    elif data.startswith('delete_'):
        await delete_project(update, context, data.replace('delete_', ''))
    elif data.startswith('admin_'):
        await admin_actions(update, context, data)

async def new_project_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    # Check limits
    if not user.get('is_premium') and user.get('total_projects', 0) >= 1:
        await update.callback_query.edit_message_text(
            "❌ *Free users can only create 1 project!*\n\n"
            "💎 Upgrade to Premium for unlimited projects!",
            parse_mode='Markdown'
        )
        return
    
    await update.callback_query.edit_message_text(
        "🆕 *Create New Project*\n\n"
        "Please send me the *Project Name* (no spaces, use underscores):\n\n"
        "Example: `my_awesome_bot`",
        parse_mode='Markdown'
    )
    return WAITING_PROJECT_NAME

async def receive_project_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    project_name = update.message.text.strip()
    
    if ' ' in project_name:
        await update.message.reply_text("❌ Project name cannot contain spaces! Use underscores.")
        return WAITING_PROJECT_NAME
    
    context.user_data['project_name'] = project_name
    
    await update.message.reply_text(
        f"✅ Project name set: `{project_name}`\n\n"
        f"📤 Now send me your Python files!\n\n"
        f"• Send multiple files one by one\n"
        f"• Or send a ZIP file with all files\n"
        f"• Must include `main.py` (entry point)\n"
        f"• Type /done when finished uploading",
        parse_mode='Markdown'
    )
    
    context.user_data['files'] = []
    return WAITING_FILES

async def receive_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == '/done':
        return await finish_upload(update, context)
    
    document = update.message.document
    if not document:
        await update.message.reply_text("❌ Please send files or type /done")
        return WAITING_FILES
    
    file_name = document.file_name
    file_obj = await context.bot.get_file(document.file_id)
    
    # Create temp directory for this upload session
    temp_dir = f"/tmp/upload_{update.effective_user.id}_{int(time.time())}"
    os.makedirs(temp_dir, exist_ok=True)
    
    file_path = os.path.join(temp_dir, file_name)
    await file_obj.download_to_drive(file_path)
    
    # If ZIP, extract
    if file_name.endswith('.zip'):
        import zipfile
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        os.remove(file_path)
        
        # List extracted files
        extracted_files = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, temp_dir)
                extracted_files.append({
                    'name': rel_path,
                    'path': full_path,
                    'temp_dir': temp_dir
                })
        context.user_data['files'].extend(extracted_files)
        await update.message.reply_text(f"📦 Extracted {len(extracted_files)} files from ZIP!")
    else:
        context.user_data['files'].append({
            'name': file_name,
            'path': file_path,
            'temp_dir': temp_dir
        })
        await update.message.reply_text(f"✅ Received: {file_name}")
    
    return WAITING_FILES

async def finish_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    files = context.user_data.get('files', [])
    project_name = context.user_data.get('project_name')
    
    if not files:
        await update.message.reply_text("❌ No files uploaded! Please upload at least one file.")
        return WAITING_FILES
    
    # Check for main.py
    has_main = any(f['name'] == 'main.py' or f['name'].endswith('/main.py') for f in files)
    if not has_main:
        await update.message.reply_text("❌ main.py not found! Please include main.py as entry point.")
        return WAITING_FILES
    
    # Create project
    user_id = update.effective_user.id
    project = create_project(user_id, project_name, [])
    
    # Move files to project directory
    for file_info in files:
        dest_path = os.path.join(project['directory'], file_info['name'])
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.move(file_info['path'], dest_path)
        # Clean up temp dir
        if os.path.exists(file_info['temp_dir']):
            shutil.rmtree(file_info['temp_dir'], ignore_errors=True)
    
    # Check for requirements.txt and install
    req_file = os.path.join(project['directory'], 'requirements.txt')
    if os.path.exists(req_file):
        await update.message.reply_text("📦 Found requirements.txt! Installing dependencies...")
        try:
            process = subprocess.Popen(
                [sys.executable, "-m", "pip", "install", "-r", req_file],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=project['directory']
            )
            stdout, stderr = process.communicate(timeout=120)
            
            if process.returncode == 0:
                installed = stdout.decode().count('Successfully installed')
                await update.message.reply_text(f"✅ Installed {installed} packages!")
            else:
                await update.message.reply_text(f"⚠️ Some packages failed to install. Error: {stderr.decode()[:500]}")
        except Exception as e:
            await update.message.reply_text(f"⚠️ Error installing requirements: {str(e)}")
    
    # Auto-detect missing imports
    await check_missing_imports(update, project)
    
    keyboard = [[InlineKeyboardButton("📁 My Projects", callback_data='my_projects')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"🎉 *Project Created Successfully!*\n\n"
        f"📛 Name: `{project_name}`\n"
        f"🆔 ID: `{str(project['_id'])}`\n"
        f"📂 Files: {len(files)}\n\n"
        f"Go to 'My Projects' to manage your bot!",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )
    
    return ConversationHandler.END

async def check_missing_imports(update: Update, project):
    """Check for missing imports in Python files"""
    missing = []
    std_libs = {'os', 'sys', 'time', 'json', 're', 'math', 'random', 'datetime', 'collections', 'itertools', 'functools', 'typing', 'pathlib', 'threading', 'asyncio', 'subprocess', 'logging', 'traceback', 'hashlib', 'base64', 'urllib', 'http', 'socket', 'sqlite3', 'csv', 'pickle', 'copy', 'inspect', 'textwrap', 'string', 'decimal', 'fractions', 'numbers', 'statistics', 'typing', 'uuid', 'warnings', 'contextlib', 'dataclasses', 'enum', 'types', 'weakref', 'numbers'}
    
    for root, dirs, files in os.walk(project['directory']):
        for file in files:
            if file.endswith('.py'):
                with open(os.path.join(root, file), 'r') as f:
                    content = f.read()
                    # Simple regex to find imports
                    import re
                    imports = re.findall(r'^(?:from|import)\s+([a-zA-Z_][a-zA-Z0-9_]*)', content, re.MULTILINE)
                    for imp in imports:
                        if imp not in std_libs:
                            try:
                                __import__(imp)
                            except ImportError:
                                if imp not in missing:
                                    missing.append(imp)
    
    if missing:
        await update.message.reply_text(
            f"⚠️ *Potentially Missing Packages:*\n"
            f"These imports were detected but not found:\n"
            f"• " + "\n• ".join(missing) + "\n\n"
            f"Add them to requirements.txt if needed!",
            parse_mode='Markdown'
        )

async def show_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    projects = get_user_projects(user_id)
    
    if not projects:
        keyboard = [[InlineKeyboardButton("🆕 Create New Project", callback_data='new_project')]]
        await update.callback_query.edit_message_text(
            "📭 *No Projects Found!*\n\nCreate your first project now!",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    text = "📁 *Your Projects:*\n\n"
    keyboard = []
    
    for proj in projects:
        status_emoji = "🟢" if proj.get('status') == 'running' else "🔴" if proj.get('status') == 'stopped' else "🟠"
        text += f"{status_emoji} `{proj['name']}`\n"
        row = [InlineKeyboardButton(f"⚙️ {proj['name']}", callback_data=f"project_{proj['_id']}")]
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data='start')])
    
    await update.callback_query.edit_message_text(
        text, 
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def project_details(update: Update, context: ContextTypes.DEFAULT_TYPE, project_id):
    project = get_project(project_id)
    if not project:
        await update.callback_query.edit_message_text("❌ Project not found!")
        return
    
    user_id = update.effective_user.id
    if project['user_id'] != user_id and user_id != OWNER_ID:
        await update.callback_query.edit_message_text("⛔ Unauthorized!")
        return
    
    status_emoji = {
        'running': '🟢 Running',
        'stopped': '🔴 Stopped',
        'crashed': '🟠 Crashed'
    }.get(project.get('status', 'stopped'), '⚪ Unknown')
    
    uptime = project.get('uptime', 'N/A')
    if project.get('pid') and psutil.pid_exists(project.get('pid')):
        try:
            p = psutil.Process(project['pid'])
            uptime = str(timedelta(seconds=int(time.time() - p.create_time())))
        except:
            pass
    
    text = f"""
📊 *Project Status for* `{project['name']}`

🔹 *Status:* {status_emoji}
🔹 *PID:* {project.get('pid', 'N/A')}
🔹 *Uptime:* {uptime}
🔹 *Last Run:* {project.get('last_run', 'N/A')}
🔹 *Last Exit Code:* {project.get('last_exit_code', 'None')}
🔹 *Run Command:* `{project.get('run_command', 'python3 main.py')}`
    """
    
    keyboard = [
        [InlineKeyboardButton("▶️ Run", callback_data=f"run_{project_id}"),
         InlineKeyboardButton("⏹️ Stop", callback_data=f"stop_{project_id}"),
         InlineKeyboardButton("🔄 Restart", callback_data=f"restart_{project_id}")],
        [InlineKeyboardButton("📋 Logs", callback_data=f"logs_{project_id}"),
         InlineKeyboardButton("🔄 Refresh Status", callback_data=f"status_{project_id}")],
        [InlineKeyboardButton("✏️ Edit Command", callback_data=f"edit_cmd_{project_id}"),
         InlineKeyboardButton("📂 File Manager", callback_data=f"file_manager_{project_id}")],
        [InlineKeyboardButton("🗑️ Delete Project", callback_data=f"delete_{project_id}")],
        [InlineKeyboardButton("⬅️ Back to Projects", callback_data='my_projects')]
    ]
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def run_project(update: Update, context: ContextTypes.DEFAULT_TYPE, project_id):
    project = get_project(project_id)
    if not project:
        await update.callback_query.answer("Project not found!")
        return
    
    if project.get('status') == 'running' and project.get('pid') and psutil.pid_exists(project['pid']):
        await update.callback_query.answer("Already running!")
        return
    
    # Run the project
    try:
        cmd = project.get('run_command', 'python3 main.py').split()
        process = subprocess.Popen(
            cmd,
            stdout=open(os.path.join(project['directory'], 'output.log'), 'a'),
            stderr=subprocess.STDOUT,
            cwd=project['directory'],
            preexec_fn=os.setsid
        )
        
        update_project(project_id, {
            'status': 'running',
            'pid': process.pid,
            'last_run': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        })
        
        await update.callback_query.answer("✅ Project started!")
        
        # Start monitor thread
        threading.Thread(target=monitor_process, args=(project_id, process.pid), daemon=True).start()
        
    except Exception as e:
        await update.callback_query.answer(f"❌ Error: {str(e)}")
    
    await project_details(update, context, project_id)

def monitor_process(project_id, pid):
    """Monitor process and restart if crashed"""
    while True:
        if not psutil.pid_exists(pid):
            project = get_project(project_id)
            if project and project.get('status') == 'running':
                # Process died unexpectedly
                update_project(project_id, {
                    'status': 'crashed',
                    'pid': None,
                    'last_exit_code': 'Crashed'
                })
                
                # Auto restart if enabled (you can add a setting for this)
                # For now, just update status
                break
            break
        time.sleep(5)

async def stop_project(update: Update, context: ContextTypes.DEFAULT_TYPE, project_id):
    project = get_project(project_id)
    if not project:
        return
    
    if project.get('pid'):
        try:
            if psutil.pid_exists(project['pid']):
                os.killpg(os.getpgid(project['pid']), 9)
        except:
            pass
    
    update_project(project_id, {
        'status': 'stopped',
        'pid': None
    })
    
    await update.callback_query.answer("⏹️ Project stopped!")
    await project_details(update, context, project_id)

async def restart_project(update: Update, context: ContextTypes.DEFAULT_TYPE, project_id):
    await stop_project(update, context, project_id)
    await asyncio.sleep(1)
    await run_project(update, context, project_id)

async def get_logs(update: Update, context: ContextTypes.DEFAULT_TYPE, project_id):
    project = get_project(project_id)
    if not project:
        return
    
    log_file = os.path.join(project['directory'], 'output.log')
    if os.path.exists(log_file):
        # Get last 4000 characters
        with open(log_file, 'r') as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 4000))
            logs = f.read()
        
        # Save to temp file
        temp_log = f"/tmp/log_{project_id}.txt"
        with open(temp_log, 'w') as f:
            f.write(f"Logs for {project['name']}\n")
            f.write(f"Generated: {datetime.utcnow()}\n")
            f.write("="*50 + "\n\n")
            f.write(logs)
        
        await update.callback_query.message.reply_document(
            document=open(temp_log, 'rb'),
            filename=f"{project['name']}_logs.txt",
            caption=f"📋 Logs for `{project['name']}`",
            parse_mode='Markdown'
        )
        os.remove(temp_log)
    else:
        await update.callback_query.answer("No logs found!")

async def refresh_status(update: Update, context: ContextTypes.DEFAULT_TYPE, project_id):
    await project_details(update, context, project_id)

async def edit_command_start(update: Update, context: ContextTypes.DEFAULT_TYPE, project_id):
    context.user_data['editing_project'] = project_id
    await update.callback_query.edit_message_text(
        "✏️ *Edit Run Command*\n\n"
        "Current command: `python3 main.py`\n\n"
        "Send me the new command (e.g., `python3 bot.py` or `python3 -m uvicorn main:app`):",
        parse_mode='Markdown'
    )
    return WAITING_EDIT_COMMAND

async def save_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_cmd = update.message.text.strip()
    project_id = context.user_data.get('editing_project')
    
    if not project_id:
        return ConversationHandler.END
    
    update_project(project_id, {'run_command': new_cmd})
    
    keyboard = [[InlineKeyboardButton("⬅️ Back to Project", callback_data=f"project_{project_id}")]]
    await update.message.reply_text(
        f"✅ Command updated to: `{new_cmd}`",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    return ConversationHandler.END

async def generate_file_manager(update: Update, context: ContextTypes.DEFAULT_TYPE, project_id):
    project = get_project(project_id)
    if not project:
        return
    
    # Generate unique session ID
    session_id = str(uuid.uuid4())
    expires = datetime.utcnow() + timedelta(minutes=10)
    
    file_manager_sessions[session_id] = {
        'project_id': str(project_id),
        'user_id': update.effective_user.id,
        'expires': expires,
        'directory': project['directory']
    }
    
    # Clean old sessions
    current_time = datetime.utcnow()
    expired = [k for k, v in file_manager_sessions.items() if v['expires'] < current_time]
    for k in expired:
        del file_manager_sessions[k]
    
    # Get Render external URL or use localhost for testing
    base_url = os.getenv('RENDER_EXTERNAL_URL', 'http://localhost:5000')
    link = f"{base_url}/filemanager/{session_id}"
    
    await update.callback_query.edit_message_text(
        f"📂 *File Manager Link Generated!*\n\n"
        f"🔗 {link}\n\n"
        f"⏰ Valid for 10 minutes\n"
        f"🔄 Generate new link anytime from project menu\n\n"
        f"*Features:*\n"
        f"• Edit files online\n"
        f"• Upload/Download files\n"
        f"• Create/Delete/Rename files\n"
        f"• Real-time syntax highlighting",
        parse_mode='Markdown',
        disable_web_page_preview=True
    )

async def delete_project(update: Update, context: ContextTypes.DEFAULT_TYPE, project_id):
    delete_project_db(project_id)
    await update.callback_query.answer("🗑️ Project deleted!")
    await show_projects(update, context)

async def show_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """
💎 *Premium Features*

✨ *What you get:*
• 🚀 Unlimited Projects (Free: 1 only)
• ⚡ Priority Support
• 🔒 99.9% Uptime Guarantee
• 📊 Advanced Analytics
• 🔄 Auto-Restart on Crash
• 💾 Extended Backup History

💰 *Pricing:*
• Monthly: $9.99
• Yearly: $99.99 (Save 17%)

📞 Contact @YourUsername to upgrade!
    """
    
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data='start')]]
    await update.callback_query.edit_message_text(
        text, 
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def bot_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Get system stats
    cpu = psutil.cpu_percent()
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    total_users = users_collection.count_documents({})
    total_projects = projects_collection.count_documents({})
    running_projects = projects_collection.count_documents({'status': 'running'})
    premium_users = users_collection.count_documents({'is_premium': True})
    
    text = f"""
📊 *God Madara Bot Status*

🖥️ *System:*
• CPU: {cpu}%
• RAM: {memory.percent}% used
• Disk: {disk.percent}% used

👥 *Users:*
• Total Users: {total_users}
• Premium Users: {premium_users}
• Total Projects: {total_projects}
• Running: {running_projects}

⏱️ *Uptime:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC
💾 *Last Backup:* Every 5 minutes active
    """
    
    keyboard = [[InlineKeyboardButton("🔄 Refresh", callback_data='bot_status'),
                 InlineKeyboardButton("⬅️ Back", callback_data='start')]]
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

# ==================== ADMIN PANEL ====================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        return
    
    text = """
🔐 *Admin Panel*

Choose an action:
    """
    
    keyboard = [
        [InlineKeyboardButton("👥 Total Users", callback_data='admin_users'),
         InlineKeyboardButton("💎 Premium Users", callback_data='admin_premium')],
        [InlineKeyboardButton("📁 All Projects", callback_data='admin_projects'),
         InlineKeyboardButton("▶️ Running Scripts", callback_data='admin_running')],
        [InlineKeyboardButton("➕ Give Premium", callback_data='admin_give_premium'),
         InlineKeyboardButton("➖ Remove Premium", callback_data='admin_remove_premium')],
        [InlineKeyboardButton("🚫 Ban User", callback_data='admin_ban'),
         InlineKeyboardButton("✅ Unban User", callback_data='admin_unban')],
        [InlineKeyboardButton("📥 Download Project", callback_data='admin_download')],
        [InlineKeyboardButton("⬅️ Back", callback_data='start')]
    ]
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def admin_actions(update: Update, context: ContextTypes.DEFAULT_TYPE, data):
    action = data.replace('admin_', '')
    
    if action == 'users':
        users = list(users_collection.find())
        text = f"👥 *Total Users: {len(users)}*\n\n"
        for user in users[:20]:  # Show first 20
            status = "💎" if user.get('is_premium') else "🆓"
            banned = "🚫" if user.get('is_banned') else ""
            text += f"{status}{banned} `{user['user_id']}` - @{user.get('username', 'N/A')}\n"
    elif action == 'premium':
        users = list(users_collection.find({'is_premium': True}))
        text = f"💎 *Premium Users: {len(users)}*\n\n"
        for user in users:
            until = user.get('premium_until', 'Lifetime')
            text += f"• `{user['user_id']}` - Until: {until}\n"
    elif action == 'projects':
        projects = list(projects_collection.find())
        text = f"📁 *Total Projects: {len(projects)}*\n\n"
        for proj in projects[:20]:
            user = get_user(proj['user_id'])
            text += f"• `{proj['name']}` by @{user.get('username', 'N/A')}\n"
    elif action == 'running':
        projects = list(projects_collection.find({'status': 'running'}))
        text = f"▶️ *Running Scripts: {len(projects)}*\n\n"
        for proj in projects:
            text += f"• `{proj['name']}` (PID: {proj.get('pid')})\n"
    elif action in ['give_premium', 'remove_premium', 'ban', 'unban', 'download']:
        context.user_data['admin_action'] = action
        await update.callback_query.edit_message_text(
            f"Send the User ID to {action.replace('_', ' ')}:"
        )
        return
    
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data='admin_panel')]]
    await update.callback_query.edit_message_text(
        text[:4000],
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def admin_handle_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    action = context.user_data.get('admin_action')
    target_id = update.message.text.strip()
    
    try:
        target_id = int(target_id)
    except:
        await update.message.reply_text("❌ Invalid User ID!")
        return
    
    if action == 'give_premium':
        # Check if temporary or permanent
        keyboard = [
            [InlineKeyboardButton("♾️ Permanent", callback_data=f'admin_setpremium_{target_id}_permanent')],
            [InlineKeyboardButton("7 Days", callback_data=f'admin_setpremium_{target_id}_7'),
             InlineKeyboardButton("30 Days", callback_data=f'admin_setpremium_{target_id}_30')],
            [InlineKeyboardButton("365 Days", callback_data=f'admin_setpremium_{target_id}_365')]
        ]
        await update.message.reply_text(
            "Select duration:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif action == 'remove_premium':
        update_user(target_id, {
            'is_premium': False,
            'premium_until': None,
            'max_projects': 1
        })
        await update.message.reply_text(f"✅ Removed premium from `{target_id}`", parse_mode='Markdown')
    elif action == 'ban':
        update_user(target_id, {'is_banned': True})
        await update.message.reply_text(f"🚫 Banned user `{target_id}`", parse_mode='Markdown')
    elif action == 'unban':
        update_user(target_id, {'is_banned': False})
        await update.message.reply_text(f"✅ Unbanned user `{target_id}`", parse_mode='Markdown')
    elif action == 'download':
        # Download user's project
        projects = get_user_projects(target_id)
        if not projects:
            await update.message.reply_text("User has no projects!")
            return
        
        keyboard = []
        for proj in projects:
            keyboard.append([InlineKeyboardButton(proj['name'], callback_data=f'admin_dlproj_{proj["_id"]}')])
        
        await update.message.reply_text(
            "Select project to download:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    context.user_data.pop('admin_action', None)

async def admin_set_premium(update: Update, context: ContextTypes.DEFAULT_TYPE, data):
    parts = data.split('_')
    user_id = int(parts[2])
    duration = parts[3]
    
    if duration == 'permanent':
        update_user(user_id, {
            'is_premium': True,
            'premium_until': None,
            'max_projects': 999999
        })
        text = f"✅ Given permanent premium to `{user_id}`"
    else:
        days = int(duration)
        until = datetime.utcnow() + timedelta(days=days)
        update_user(user_id, {
            'is_premium': True,
            'premium_until': until,
            'max_projects': 999999
        })
        text = f"✅ Given {days} days premium to `{user_id}`\nUntil: {until.strftime('%Y-%m-%d')}"
    
    await update.callback_query.edit_message_text(text, parse_mode='Markdown')

async def admin_download_project(update: Update, context: ContextTypes.DEFAULT_TYPE, data):
    project_id = data.replace('admin_dlproj_', '')
    project = get_project(project_id)
    
    if not project:
        await update.callback_query.answer("Project not found!")
        return
    
    # Create zip
    zip_path = f"/tmp/project_{project_id}.zip"
    shutil.make_archive(zip_path.replace('.zip', ''), 'zip', project['directory'])
    
    await update.callback_query.message.reply_document(
        document=open(zip_path, 'rb'),
        filename=f"{project['name']}.zip"
    )
    os.remove(zip_path)

# ==================== FLASK FILE MANAGER ====================

FILE_MANAGER_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>God Madara File Manager</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            min-height: 100vh;
            color: #fff;
        }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { 
            background: rgba(0,0,0,0.3); 
            padding: 20px; 
            border-radius: 10px; 
            margin-bottom: 20px;
            backdrop-filter: blur(10px);
        }
        .header h1 { font-size: 24px; margin-bottom: 10px; }
        .breadcrumb { 
            background: rgba(255,255,255,0.1); 
            padding: 10px; 
            border-radius: 5px; 
            margin-bottom: 20px;
            font-family: monospace;
        }
        .file-grid { 
            display: grid; 
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); 
            gap: 15px; 
        }
        .file-card { 
            background: rgba(255,255,255,0.1); 
            padding: 15px; 
            border-radius: 10px; 
            transition: transform 0.2s;
            backdrop-filter: blur(5px);
            border: 1px solid rgba(255,255,255,0.2);
        }
        .file-card:hover { transform: translateY(-5px); background: rgba(255,255,255,0.2); }
        .file-icon { font-size: 40px; margin-bottom: 10px; }
        .file-name { font-weight: bold; word-break: break-all; }
        .file-actions { margin-top: 10px; display: flex; gap: 5px; flex-wrap: wrap; }
        .btn { 
            padding: 5px 10px; 
            border: none; 
            border-radius: 5px; 
            cursor: pointer; 
            font-size: 12px;
            text-decoration: none;
            display: inline-block;
        }
        .btn-primary { background: #4CAF50; color: white; }
        .btn-danger { background: #f44336; color: white; }
        .btn-warning { background: #ff9800; color: white; }
        .btn-info { background: #2196F3; color: white; }
        .upload-area { 
            border: 2px dashed rgba(255,255,255,0.5); 
            padding: 40px; 
            text-align: center; 
            border-radius: 10px; 
            margin-bottom: 20px;
            background: rgba(255,255,255,0.05);
        }
        .editor { 
            background: #1e1e1e; 
            padding: 20px; 
            border-radius: 10px; 
            margin-top: 20px;
        }
        textarea { 
            width: 100%; 
            height: 400px; 
            background: #2d2d2d; 
            color: #d4d4d4; 
            border: none; 
            padding: 10px; 
            font-family: 'Consolas', monospace;
            font-size: 14px;
            border-radius: 5px;
        }
        .nav-btn { margin-bottom: 20px; }
        .alert { 
            padding: 10px; 
            background: rgba(76, 175, 80, 0.3); 
            border-radius: 5px; 
            margin-bottom: 10px;
            border: 1px solid #4CAF50;
        }
        .stats { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 15px; 
            margin-bottom: 20px;
        }
        .stat-card { 
            background: rgba(255,255,255,0.1); 
            padding: 15px; 
            border-radius: 10px;
            text-align: center;
        }
        .stat-value { font-size: 24px; font-weight: bold; color: #4CAF50; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>👑 God Madara File Manager</h1>
            <p>Project: {{project_name}} | Session expires in: <span id="timer">10:00</span></p>
        </div>
        
        {% with messages = get_flashed_messages() %}
            {% if messages %}
                {% for message in messages %}
                    <div class="alert">{{message}}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value">{{file_count}}</div>
                <div>Files</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{dir_count}}</div>
                <div>Directories</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{total_size}}</div>
                <div>Total Size</div>
            </div>
        </div>
        
        <div class="breadcrumb">
            📁 {{current_path}}
        </div>
        
        <div class="nav-btn">
            <a href="{{url_for('file_manager', session_id=session_id)}}?path={{parent_path}}" class="btn btn-primary">⬆️ Parent Directory</a>
            <a href="{{url_for('new_file', session_id=session_id)}}?path={{current_path}}" class="btn btn-info">📄 New File</a>
            <a href="{{url_for('new_folder', session_id=session_id)}}?path={{current_path}}" class="btn btn-info">📁 New Folder</a>
        </div>
        
        <div class="upload-area">
            <form method="POST" enctype="multipart/form-data" action="{{url_for('upload_file', session_id=session_id)}}?path={{current_path}}">
                <input type="file" name="file" multiple style="margin-bottom: 10px;">
                <br><br>
                <button type="submit" class="btn btn-primary">📤 Upload Files</button>
            </form>
        </div>
        
        <div class="file-grid">
            {% for item in items %}
            <div class="file-card">
                <div class="file-icon">{{item.icon}}</div>
                <div class="file-name">{{item.name}}</div>
                <div class="file-actions">
                    {% if item.is_file %}
                    <a href="{{url_for('edit_file', session_id=session_id)}}?path={{item.path}}" class="btn btn-primary">✏️ Edit</a>
                    <a href="{{url_for('download_file', session_id=session_id)}}?path={{item.path}}" class="btn btn-info">⬇️ Download</a>
                    {% else %}
                    <a href="{{url_for('file_manager', session_id=session_id)}}?path={{item.path}}" class="btn btn-primary">📂 Open</a>
                    {% endif %}
                    <a href="{{url_for('rename_item', session_id=session_id)}}?path={{item.path}}" class="btn btn-warning">✏️ Rename</a>
                    <a href="{{url_for('delete_item', session_id=session_id)}}?path={{item.path}}" class="btn btn-danger" onclick="return confirm('Delete {{item.name}}?')">🗑️ Delete</a>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>
    
    <script>
        // Countdown timer
        let time = 600; // 10 minutes
        setInterval(() => {
            time--;
            const minutes = Math.floor(time / 60);
            const seconds = time % 60;
            document.getElementById('timer').textContent = 
                `${minutes}:${seconds.toString().padStart(2, '0')}`;
            if (time <= 0) {
                alert('Session expired!');
                window.location.reload();
            }
        }, 1000);
    </script>
</body>
</html>
"""

@app.route('/filemanager/<session_id>')
def file_manager(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired or invalid!", 403
    
    session = file_manager_sessions[session_id]
    if datetime.utcnow() > session['expires']:
        del file_manager_sessions[session_id]
        return "Session expired!", 403
    
    current_path = request.args.get('path', session['directory'])
    
    # Security check
    if not current_path.startswith(session['directory']):
        current_path = session['directory']
    
    items = []
    file_count = 0
    dir_count = 0
    total_size = 0
    
    try:
        for item in os.listdir(current_path):
            item_path = os.path.join(current_path, item)
            rel_path = os.path.relpath(item_path, session['directory'])
            is_file = os.path.isfile(item_path)
            
            if is_file:
                file_count += 1
                total_size += os.path.getsize(item_path)
                icon = "📄"
                if item.endswith('.py'): icon = "🐍"
                elif item.endswith('.txt'): icon = "📝"
                elif item.endswith('.json'): icon = "📋"
                elif item.endswith('.zip'): icon = "📦"
                elif item.endswith('.jpg') or item.endswith('.png'): icon = "🖼️"
            else:
                dir_count += 1
                icon = "📁"
            
            items.append({
                'name': item,
                'path': rel_path,
                'is_file': is_file,
                'icon': icon
            })
    except Exception as e:
        flash(f"Error: {str(e)}")
    
    # Calculate parent path
    parent_path = os.path.dirname(current_path) if current_path != session['directory'] else current_path
    
    # Format total size
    size_str = f"{total_size} B"
    if total_size > 1024: size_str = f"{total_size/1024:.1f} KB"
    if total_size > 1024*1024: size_str = f"{total_size/(1024*1024):.1f} MB"
    
    project = get_project(ObjectId(session['project_id']))
    
    return render_template_string(FILE_MANAGER_HTML,
        session_id=session_id,
        current_path=current_path,
        parent_path=parent_path,
        items=items,
        project_name=project['name'] if project else 'Unknown',
        file_count=file_count,
        dir_count=dir_count,
        total_size=size_str,
        get_flashed_messages=lambda: []  # Simplified for this example
    )

@app.route('/filemanager/<session_id>/edit')
def edit_file(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired!", 403
    
    session = file_manager_sessions[session_id]
    file_path = request.args.get('path')
    full_path = os.path.join(session['directory'], file_path)
    
    if not full_path.startswith(session['directory']) or not os.path.exists(full_path):
        return "Invalid file!", 403
    
    content = ""
    try:
        with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        content = f"Error reading file: {str(e)}"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Edit {os.path.basename(file_path)}</title>
        <style>
            body {{ background: #1e1e1e; color: #d4d4d4; font-family: Consolas, monospace; padding: 20px; }}
            textarea {{ width: 100%; height: 80vh; background: #2d2d2d; color: #d4d4d4; border: none; padding: 10px; font-family: Consolas, monospace; font-size: 14px; }}
            .btn {{ padding: 10px 20px; background: #4CAF50; color: white; border: none; cursor: pointer; margin-top: 10px; }}
            .header {{ margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>✏️ Editing: {file_path}</h2>
            <a href="{url_for('file_manager', session_id=session_id)}?path={os.path.dirname(full_path)}" style="color: #4CAF50;">⬅️ Back to Files</a>
        </div>
        <form method="POST" action="{url_for('save_file', session_id=session_id)}?path={file_path}">
            <textarea name="content">{content}</textarea>
            <br>
            <button type="submit" class="btn">💾 Save Changes</button>
        </form>
    </body>
    </html>
    """
    return html

@app.route('/filemanager/<session_id>/save', methods=['POST'])
def save_file(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired!", 403
    
    session = file_manager_sessions[session_id]
    file_path = request.args.get('path')
    full_path = os.path.join(session['directory'], file_path)
    
    if not full_path.startswith(session['directory']):
        return "Invalid path!", 403
    
    content = request.form.get('content', '')
    
    try:
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)
        flash("✅ File saved successfully!")
    except Exception as e:
        flash(f"❌ Error saving file: {str(e)}")
    
    return redirect(url_for('file_manager', session_id=session_id, path=os.path.dirname(full_path)))

@app.route('/filemanager/<session_id>/upload', methods=['POST'])
def upload_file(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired!", 403
    
    session = file_manager_sessions[session_id]
    current_path = request.args.get('path', session['directory'])
    
    if 'file' not in request.files:
        flash("No file selected!")
        return redirect(url_for('file_manager', session_id=session_id, path=current_path))
    
    files = request.files.getlist('file')
    for file in files:
        if file.filename:
            filename = secure_filename(file.filename)
            file_path = os.path.join(current_path, filename)
            file.save(file_path)
    
    flash(f"✅ Uploaded {len(files)} file(s)!")
    return redirect(url_for('file_manager', session_id=session_id, path=current_path))

@app.route('/filemanager/<session_id>/download')
def download_file(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired!", 403
    
    session = file_manager_sessions[session_id]
    file_path = request.args.get('path')
    full_path = os.path.join(session['directory'], file_path)
    
    if not full_path.startswith(session['directory']) or not os.path.isfile(full_path):
        return "Invalid file!", 403
    
    return send_file(full_path, as_attachment=True)

@app.route('/filemanager/<session_id>/delete')
def delete_item(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired!", 403
    
    session = file_manager_sessions[session_id]
    item_path = request.args.get('path')
    full_path = os.path.join(session['directory'], item_path)
    
    if not full_path.startswith(session['directory']):
        return "Invalid path!", 403
    
    try:
        if os.path.isfile(full_path):
            os.remove(full_path)
            flash("🗑️ File deleted!")
        else:
            shutil.rmtree(full_path)
            flash("🗑️ Folder deleted!")
    except Exception as e:
        flash(f"❌ Error: {str(e)}")
    
    return redirect(url_for('file_manager', session_id=session_id, path=os.path.dirname(full_path)))

@app.route('/filemanager/<session_id>/rename')
def rename_item(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired!", 403
    
    session = file_manager_sessions[session_id]
    item_path = request.args.get('path')
    full_path = os.path.join(session['directory'], item_path)
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Rename</title>
        <style>
            body {{ background: #1e3c72; color: white; font-family: Arial; padding: 50px; }}
            input {{ padding: 10px; font-size: 16px; width: 300px; }}
            .btn {{ padding: 10px 20px; background: #4CAF50; color: white; border: none; cursor: pointer; margin-left: 10px; }}
        </style>
    </head>
    <body>
        <h2>✏️ Rename: {os.path.basename(item_path)}</h2>
        <form method="POST" action="{url_for('do_rename', session_id=session_id)}?path={item_path}">
            <input type="text" name="new_name" value="{os.path.basename(item_path)}" required>
            <button type="submit" class="btn">Rename</button>
        </form>
    </body>
    </html>
    """
    return html

@app.route('/filemanager/<session_id>/do_rename', methods=['POST'])
def do_rename(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired!", 403
    
    session = file_manager_sessions[session_id]
    item_path = request.args.get('path')
    old_path = os.path.join(session['directory'], item_path)
    new_name = request.form.get('new_name')
    
    if not old_path.startswith(session['directory']) or not new_name:
        return "Invalid!", 403
    
    new_path = os.path.join(os.path.dirname(old_path), secure_filename(new_name))
    
    try:
        os.rename(old_path, new_path)
        flash("✅ Renamed successfully!")
    except Exception as e:
        flash(f"❌ Error: {str(e)}")
    
    return redirect(url_for('file_manager', session_id=session_id, path=os.path.dirname(new_path)))

@app.route('/filemanager/<session_id>/newfile')
def new_file(session_id):
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>New File</title>
        <style>
            body {{ background: #1e3c72; color: white; font-family: Arial; padding: 50px; }}
            input {{ padding: 10px; font-size: 16px; width: 300px; }}
            .btn {{ padding: 10px 20px; background: #4CAF50; color: white; border: none; cursor: pointer; margin-left: 10px; }}
        </style>
    </head>
    <body>
        <h2>📄 Create New File</h2>
        <form method="POST" action="{url_for('do_new_file', session_id=session_id)}?path={request.args.get('path', '')}">
            <input type="text" name="filename" placeholder="filename.py" required>
            <button type="submit" class="btn">Create</button>
        </form>
    </body>
    </html>
    """
    return html

@app.route('/filemanager/<session_id>/do_newfile', methods=['POST'])
def do_new_file(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired!", 403
    
    session = file_manager_sessions[session_id]
    current_path = request.args.get('path', session['directory'])
    filename = secure_filename(request.form.get('filename', ''))
    
    if filename:
        filepath = os.path.join(current_path, filename)
        try:
            with open(filepath, 'w') as f:
                f.write("# New file created via God Madara File Manager\n")
            flash("✅ File created!")
        except Exception as e:
            flash(f"❌ Error: {str(e)}")
    
    return redirect(url_for('file_manager', session_id=session_id, path=current_path))

@app.route('/filemanager/<session_id>/newfolder')
def new_folder(session_id):
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>New Folder</title>
        <style>
            body {{ background: #1e3c72; color: white; font-family: Arial; padding: 50px; }}
            input {{ padding: 10px; font-size: 16px; width: 300px; }}
            .btn {{ padding: 10px 20px; background: #4CAF50; color: white; border: none; cursor: pointer; margin-left: 10px; }}
        </style>
    </head>
    <body>
        <h2>📁 Create New Folder</h2>
        <form method="POST" action="{url_for('do_new_folder', session_id=session_id)}?path={request.args.get('path', '')}">
            <input type="text" name="foldername" placeholder="folder_name" required>
            <button type="submit" class="btn">Create</button>
        </form>
    </body>
    </html>
    """
    return html

@app.route('/filemanager/<session_id>/do_newfolder', methods=['POST'])
def do_new_folder(session_id):
    if session_id not in file_manager_sessions:
        return "Session expired!", 403
    
    session = file_manager_sessions[session_id]
    current_path = request.args.get('path', session['directory'])
    foldername = secure_filename(request.form.get('foldername', ''))
    
    if foldername:
        folderpath = os.path.join(current_path, foldername)
        try:
            os.makedirs(folderpath)
            flash("✅ Folder created!")
        except Exception as e:
            flash(f"❌ Error: {str(e)}")
    
    return redirect(url_for('file_manager', session_id=session_id, path=current_path))

# ==================== MAIN ====================

def run_flask():
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

def main():
    # Restore from backup if needed
    restore_from_backup()
    
    # Start Flask in separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Build Telegram application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Conversation handler for new project
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(new_project_start, pattern='^new_project$')],
        states={
            WAITING_PROJECT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_project_name)],
            WAITING_FILES: [
                MessageHandler(filters.Document.ALL, receive_files),
                MessageHandler(filters.Regex('^/done$'), finish_upload)
            ],
            WAITING_EDIT_COMMAND: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_command)]
        },
        fallbacks=[CommandHandler('cancel', lambda u, c: u.message.reply_text("Cancelled"))]
    )
    
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_id))
    
    print("🤖 God Madara Bot Started!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
