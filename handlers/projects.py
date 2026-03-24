import os
import zipfile
import shutil
import datetime
from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup,
    InlineKeyboardButton, Document
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from database import (
    create_project, get_user_projects, get_project,
    update_project, delete_project, is_user_premium,
    create_fm_token, get_project_by_name
)
from core.runner import run_project, stop_project, restart_project, get_logs, get_uptime_str
from config import PROJECTS_DIR, BASE_URL, MAX_FREE_PROJECTS
import secrets

router = Router()


class ProjectStates(StatesGroup):
    waiting_name = State()
    waiting_files = State()
    waiting_run_command = State()


def project_keyboard(project_id: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="▶️ Run", callback_data=f"run_{project_id}"),
            InlineKeyboardButton(text="⏹ Stop", callback_data=f"stop_{project_id}"),
            InlineKeyboardButton(text="🔄 Restart", callback_data=f"restart_{project_id}"),
        ],
        [
            InlineKeyboardButton(text="📋 Logs", callback_data=f"logs_{project_id}"),
            InlineKeyboardButton(text="📊 Status", callback_data=f"status_{project_id}"),
        ],
        [
            InlineKeyboardButton(text="⚙️ Edit Run Command", callback_data=f"editcmd_{project_id}"),
            InlineKeyboardButton(text="🌐 File Manager", callback_data=f"filemanager_{project_id}"),
        ],
        [
            InlineKeyboardButton(text="🗑 Delete Project", callback_data=f"delete_{project_id}"),
            InlineKeyboardButton(text="🔙 My Projects", callback_data="my_projects"),
        ]
    ])


def status_emoji(status: str) -> str:
    mapping = {
        "running": "🟢 Running",
        "stopped": "🔴 Stopped",
        "crashed": "🟠 Crashed",
        "installing": "🔵 Installing",
    }
    return mapping.get(status, "⚪ Unknown")


# ─── New Project ──────────────────────────────────────────────────

@router.callback_query(F.data == "new_project")
async def new_project(callback: CallbackQuery, state: FSMContext):
    from database import is_user_banned
    if await is_user_banned(callback.from_user.id):
        await callback.answer("You are banned!", show_alert=True)
        return

    user_projects = await get_user_projects(callback.from_user.id)
    is_premium = await is_user_premium(callback.from_user.id)

    if not is_premium and len(user_projects) >= MAX_FREE_PROJECTS:
        await callback.message.edit_text(
            "❌ *Free plan allows only 1 project.*\n\nUpgrade to Premium for unlimited projects! 👑",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👑 Premium Info", callback_data="premium_info")],
                [InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")]
            ])
        )
        return

    await callback.message.edit_text(
        "📁 *New Project*\n\nPlease enter a name for your project:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="main_menu")]
        ])
    )
    await state.set_state(ProjectStates.waiting_name)


@router.message(ProjectStates.waiting_name)
async def receive_project_name(message: Message, state: FSMContext):
    name = message.text.strip().replace(" ", "_")
    if not name.isidentifier():
        await message.answer("❌ Invalid project name. Use only letters, numbers, underscores.")
        return

    existing = await get_project_by_name(message.from_user.id, name)
    if existing:
        await message.answer("❌ A project with this name already exists. Choose a different name.")
        return

    await state.update_data(project_name=name)
    await message.answer(
        f"✅ Project name: *{name}*\n\n"
        "📤 Now upload your files.\n\n"
        "You can send:\n"
        "• A `.zip` file containing all your files\n"
        "• Multiple `.py` files one by one\n\n"
        "When done, send /done",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Done Uploading", callback_data="upload_done")]
        ])
    )
    await state.update_data(uploaded_files=[])
    await state.set_state(ProjectStates.waiting_files)


@router.message(ProjectStates.waiting_files, F.document)
async def receive_files(message: Message, state: FSMContext):
    data = await state.get_data()
    project_name = data["project_name"]
    user_id = message.from_user.id

    project_path = os.path.join(PROJECTS_DIR, str(user_id), project_name)
    os.makedirs(project_path, exist_ok=True)

    doc: Document = message.document
    file_name = doc.file_name

    # Download file
    file = await message.bot.get_file(doc.file_id)
    dest = os.path.join(project_path, file_name)
    await message.bot.download_file(file.file_path, dest)

    # If zip, extract
    if file_name.endswith(".zip"):
        with zipfile.ZipFile(dest, 'r') as z:
            z.extractall(project_path)
        os.remove(dest)
        await message.answer(f"📦 `{file_name}` extracted successfully!\n\nSend more files or tap *Done*.",
                             parse_mode="Markdown",
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                 [InlineKeyboardButton(text="✅ Done Uploading", callback_data="upload_done")]
                             ]))
    else:
        await message.answer(f"✅ `{file_name}` uploaded!\n\nSend more files or tap *Done*.",
                             parse_mode="Markdown",
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                 [InlineKeyboardButton(text="✅ Done Uploading", callback_data="upload_done")]
                             ]))

    files = data.get("uploaded_files", [])
    files.append(file_name)
    await state.update_data(uploaded_files=files)


@router.message(ProjectStates.waiting_files)
async def done_command(message: Message, state: FSMContext):
    if message.text and message.text.strip() == "/done":
        await finalize_upload(message, state)


@router.callback_query(F.data == "upload_done")
async def upload_done_callback(callback: CallbackQuery, state: FSMContext):
    await finalize_upload(callback.message, state, user_id=callback.from_user.id)


async def finalize_upload(message: Message, state: FSMContext, user_id: int = None):
    data = await state.get_data()
    uid = user_id or message.from_user.id
    project_name = data.get("project_name")

    if not project_name:
        await message.answer("❌ Something went wrong. Please start over.")
        await state.clear()
        return

    project_path = os.path.join(PROJECTS_DIR, str(uid), project_name)
    project = await create_project(uid, project_name, project_path)
    project_id = str(project["_id"])

    await state.clear()

    await message.answer(
        f"🎉 *Project '{project_name}' created successfully!*\n\n"
        f"📂 Files are ready.\n"
        f"⚙️ Default run command: `python3 main.py`\n\n"
        f"Use the buttons below to manage your project.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗂 My Projects", callback_data="my_projects")],
            [InlineKeyboardButton(text="▶️ Run Now", callback_data=f"run_{project_id}")],
        ])
    )


# ─── My Projects ──────────────────────────────────────────────────

@router.callback_query(F.data == "my_projects")
async def my_projects(callback: CallbackQuery):
    projects = await get_user_projects(callback.from_user.id)

    if not projects:
        await callback.message.edit_text(
            "📭 *You have no projects yet.*\n\nCreate one with New Project!",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📁 New Project", callback_data="new_project")],
                [InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")]
            ])
        )
        return

    buttons = []
    for proj in projects:
        s = proj.get("status", "stopped")
        emoji = {"running": "🟢", "stopped": "🔴", "crashed": "🟠"}.get(s, "⚪")
        buttons.append([
            InlineKeyboardButton(
                text=f"{emoji} {proj['project_name']}",
                callback_data=f"status_{str(proj['_id'])}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")])

    await callback.message.edit_text(
        "🗂 *My Projects*\n\nSelect a project to manage:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


# ─── Project Status ───────────────────────────────────────────────

@router.callback_query(F.data.startswith("status_"))
async def project_status(callback: CallbackQuery):
    project_id = callback.data.replace("status_", "")
    project = await get_project(project_id)

    if not project or project["user_id"] != callback.from_user.id:
        await callback.answer("Project not found.", show_alert=True)
        return

    uptime = get_uptime_str(project.get("uptime_start"))
    last_run = project.get("last_run")
    last_run_str = last_run.strftime("%Y-%m-%d %H:%M:%S UTC") if last_run else "Never"
    status = status_emoji(project.get("status", "stopped"))
    pid = project.get("pid") or "N/A"
    exit_code = project.get("last_exit_code")
    run_cmd = project.get("run_command", "python3 main.py")

    text = (
        f"📊 *Project Status for {project['project_name']}*\n\n"
        f"🔹 Status: {status}\n"
        f"🔹 PID: `{pid}`\n"
        f"🔹 Uptime: `{uptime}`\n"
        f"🔹 Last Run: `{last_run_str}`\n"
        f"🔹 Last Exit Code: `{exit_code}`\n"
        f"🔹 Run Command: `{run_cmd}`"
    )

    await callback.message.edit_text(text, parse_mode="Markdown",
                                     reply_markup=project_keyboard(project_id))


# ─── Run / Stop / Restart ─────────────────────────────────────────

@router.callback_query(F.data.startswith("run_"))
async def run_handler(callback: CallbackQuery):
    project_id = callback.data.replace("run_", "")
    project = await get_project(project_id)
    if not project or project["user_id"] != callback.from_user.id:
        await callback.answer("Not found.", show_alert=True)
        return

    await callback.message.edit_text("⏳ Starting project...", parse_mode="Markdown")
    result = await run_project(project_id)

    if result["success"]:
        install = result.get("install_info", {})
        installed = install.get("installed", [])
        failed = install.get("failed", [])
        info = ""
        if installed:
            info += f"\n📦 Installed: `{', '.join(installed)}`"
        if failed:
            info += f"\n⚠️ Failed to install: `{', '.join(failed)}`"

        await callback.message.edit_text(
            f"✅ *Project started!*\n🔢 PID: `{result['pid']}`{info}",
            parse_mode="Markdown",
            reply_markup=project_keyboard(project_id)
        )
    else:
        await callback.message.edit_text(
            f"❌ *Failed to start:*\n`{result['error']}`",
            parse_mode="Markdown",
            reply_markup=project_keyboard(project_id)
        )


@router.callback_query(F.data.startswith("stop_"))
async def stop_handler(callback: CallbackQuery):
    project_id = callback.data.replace("stop_", "")
    project = await get_project(project_id)
    if not project or project["user_id"] != callback.from_user.id:
        await callback.answer("Not found.", show_alert=True)
        return
    await stop_project(project_id)
    await callback.message.edit_text("⏹ *Project stopped.*", parse_mode="Markdown",
                                     reply_markup=project_keyboard(project_id))


@router.callback_query(F.data.startswith("restart_"))
async def restart_handler(callback: CallbackQuery):
    project_id = callback.data.replace("restart_", "")
    project = await get_project(project_id)
    if not project or project["user_id"] != callback.from_user.id:
        await callback.answer("Not found.", show_alert=True)
        return
    await callback.message.edit_text("🔄 Restarting...", parse_mode="Markdown")
    result = await restart_project(project_id)
    if result["success"]:
        await callback.message.edit_text("✅ *Restarted successfully!*",
                                         parse_mode="Markdown",
                                         reply_markup=project_keyboard(project_id))
    else:
        await callback.message.edit_text(f"❌ Restart failed: `{result['error']}`",
                                         parse_mode="Markdown",
                                         reply_markup=project_keyboard(project_id))


# ─── Logs ─────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("logs_"))
async def logs_handler(callback: CallbackQuery):
    project_id = callback.data.replace("logs_", "")
    project = await get_project(project_id)
    if not project or project["user_id"] != callback.from_user.id:
        await callback.answer("Not found.", show_alert=True)
        return

    logs = await get_logs(project_id)

    log_file_name = f"logs_{project['project_name']}.txt"
    with open(f"/tmp/{log_file_name}", "w") as f:
        f.write(logs)

    from aiogram.types import FSInputFile
    await callback.message.answer_document(
        FSInputFile(f"/tmp/{log_file_name}"),
        caption=f"📋 Logs for *{project['project_name']}*",
        parse_mode="Markdown",
        reply_markup=project_keyboard(project_id)
    )
    await callback.answer()


# ─── Edit Run Command ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("editcmd_"))
async def edit_cmd_handler(callback: CallbackQuery, state: FSMContext):
    project_id = callback.data.replace("editcmd_", "")
    project = await get_project(project_id)
    if not project or project["user_id"] != callback.from_user.id:
        await callback.answer("Not found.", show_alert=True)
        return

    await state.update_data(editing_project_id=project_id)
    await state.set_state(ProjectStates.waiting_run_command)
    await callback.message.edit_text(
        f"⚙️ *Edit Run Command*\n\n"
        f"Current: `{project.get('run_command', 'python3 main.py')}`\n\n"
        f"Send the new run command (e.g. `python3 bot.py`):",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data=f"status_{project_id}")]
        ])
    )


@router.message(ProjectStates.waiting_run_command)
async def receive_run_command(message: Message, state: FSMContext):
    data = await state.get_data()
    project_id = data.get("editing_project_id")
    if not project_id:
        await state.clear()
        return
    new_cmd = message.text.strip()
    await update_project(project_id, {"run_command": new_cmd})
    await state.clear()
    await message.answer(
        f"✅ Run command updated to: `{new_cmd}`",
        parse_mode="Markdown",
        reply_markup=project_keyboard(project_id)
    )


# ─── File Manager ─────────────────────────────────────────────────

@router.callback_query(F.data.startswith("filemanager_"))
async def file_manager_handler(callback: CallbackQuery):
    project_id = callback.data.replace("filemanager_", "")
    project = await get_project(project_id)
    if not project or project["user_id"] != callback.from_user.id:
        await callback.answer("Not found.", show_alert=True)
        return

    token = secrets.token_urlsafe(32)
    await create_fm_token(callback.from_user.id, project_id, token)

    url = f"{BASE_URL}/fm/{token}"
    await callback.message.edit_text(
        f"🌐 *File Manager*\n\n"
        f"Click the link below to open the file manager:\n"
        f"[Open File Manager]({url})\n\n"
        f"⚠️ This link is valid for *10 minutes* only.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Back", callback_data=f"status_{project_id}")]
        ])
    )


# ─── Delete Project ───────────────────────────────────────────────

@router.callback_query(F.data.startswith("delete_"))
async def delete_handler(callback: CallbackQuery):
    project_id = callback.data.replace("delete_", "")
    project = await get_project(project_id)
    if not project or project["user_id"] != callback.from_user.id:
        await callback.answer("Not found.", show_alert=True)
        return

    await callback.message.edit_text(
        f"⚠️ *Delete '{project['project_name']}'?*\n\nThis cannot be undone.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🗑 Yes, Delete", callback_data=f"confirm_delete_{project_id}"),
                InlineKeyboardButton(text="❌ Cancel", callback_data=f"status_{project_id}"),
            ]
        ])
    )


@router.callback_query(F.data.startswith("confirm_delete_"))
async def confirm_delete(callback: CallbackQuery):
    project_id = callback.data.replace("confirm_delete_", "")
    project = await get_project(project_id)
    if not project or project["user_id"] != callback.from_user.id:
        await callback.answer("Not found.", show_alert=True)
        return

    await stop_project(project_id)
    shutil.rmtree(project["project_path"], ignore_errors=True)
    await delete_project(project_id)

    await callback.message.edit_text(
        "🗑 *Project deleted successfully.*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗂 My Projects", callback_data="my_projects")]
        ])
  )
  
