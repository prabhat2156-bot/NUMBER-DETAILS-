import os
import zipfile
import shutil
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from database import (
    get_all_users, get_premium_users, get_all_projects,
    get_running_projects, get_project, get_user,
    set_premium, ban_user
)
from config import OWNER_ID

router = Router()


class AdminStates(StatesGroup):
    waiting_premium_id = State()
    waiting_premium_days = State()
    waiting_ban_id = State()
    waiting_unban_id = State()
    waiting_remove_premium_id = State()


def admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👥 All Users", callback_data="admin_users"),
            InlineKeyboardButton(text="👑 Premium Users", callback_data="admin_premium_users"),
        ],
        [
            InlineKeyboardButton(text="▶️ Running Scripts", callback_data="admin_running"),
            InlineKeyboardButton(text="📁 All Projects", callback_data="admin_all_projects"),
        ],
        [
            InlineKeyboardButton(text="➕ Add Premium", callback_data="admin_add_premium"),
            InlineKeyboardButton(text="➖ Remove Premium", callback_data="admin_remove_premium"),
        ],
        [
            InlineKeyboardButton(text="🚫 Ban User", callback_data="admin_ban"),
            InlineKeyboardButton(text="✅ Unban User", callback_data="admin_unban"),
        ],
        [
            InlineKeyboardButton(text="⏱ Temp Premium", callback_data="admin_temp_premium"),
        ]
    ])


@router.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id != OWNER_ID:
        await message.answer("❌ Access denied.")
        return
    await message.answer(
        "🛡 *God Madara Admin Panel*\n\nWelcome, Owner!",
        parse_mode="Markdown",
        reply_markup=admin_keyboard()
    )


# ─── Users List ───────────────────────────────────────────────────

@router.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return
    users = await get_all_users()
    text = f"👥 *Total Users: {len(users)}*\n\n"
    for u in users[:30]:
        prem = "👑" if u.get("is_premium") else "👤"
        ban = "🚫" if u.get("is_banned") else ""
        text += f"{prem}{ban} `{u['user_id']}` — @{u.get('username', 'N/A')}\n"
    if len(users) > 30:
        text += f"\n... and {len(users)-30} more"
    await callback.message.edit_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_back")]
        ]))


@router.callback_query(F.data == "admin_premium_users")
async def admin_premium_users(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return
    users = await get_premium_users()
    text = f"👑 *Premium Users: {len(users)}*\n\n"
    for u in users:
        expiry = u.get("premium_expiry")
        exp_str = expiry.strftime("%Y-%m-%d") if expiry else "Permanent"
        text += f"• `{u['user_id']}` @{u.get('username','N/A')} — Expires: {exp_str}\n"
    if not users:
        text += "No premium users yet."
    await callback.message.edit_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_back")]
        ]))


@router.callback_query(F.data == "admin_running")
async def admin_running(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return
    running = await get_running_projects()
    text = f"▶️ *Running Scripts: {len(running)}*\n\n"
    for p in running:
        text += f"• `{p['project_name']}` — User: `{p['user_id']}` PID: `{p.get('pid','N/A')}`\n"
    if not running:
        text += "No running scripts."
    await callback.message.edit_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_back")]
        ]))


@router.callback_query(F.data == "admin_all_projects")
async def admin_all_projects(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return
    projects = await get_all_projects()
    text = f"📁 *All Projects: {len(projects)}*\n\n"
    for p in projects[:30]:
        s = p.get("status", "stopped")
        emoji = {"running": "🟢", "stopped": "🔴", "crashed": "🟠"}.get(s, "⚪")
        text += f"{emoji} `{p['project_name']}` — User: `{p['user_id']}`\n"
    await callback.message.edit_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_back")]
        ]))


# ─── Add Premium ──────────────────────────────────────────────────

@router.callback_query(F.data == "admin_add_premium")
async def admin_add_premium(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return
    await callback.message.edit_text(
        "➕ *Add Permanent Premium*\n\nSend the User ID:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_back")]
        ])
    )
    await state.set_state(AdminStates.waiting_premium_id)
    await state.update_data(premium_type="permanent")


@router.callback_query(F.data == "admin_temp_premium")
async def admin_temp_premium(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return
    await callback.message.edit_text(
        "⏱ *Add Temporary Premium*\n\nSend the User ID:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_back")]
        ])
    )
    await state.set_state(AdminStates.waiting_premium_id)
    await state.update_data(premium_type="temporary")


@router.message(AdminStates.waiting_premium_id)
async def receive_premium_user_id(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return
    try:
        uid = int(message.text.strip())
    except:
        await message.answer("❌ Invalid ID.")
        return

    data = await state.get_data()
    ptype = data.get("premium_type", "permanent")

    if ptype == "permanent":
        await set_premium(uid, -1)
        await state.clear()
        await message.answer(f"✅ User `{uid}` given permanent premium!", parse_mode="Markdown",
                             reply_markup=admin_keyboard())
        try:
            await message.bot.send_message(uid,
                "🎉 *Congratulations!* You have been granted *Permanent Premium* access!\n\n"
                "You can now create unlimited projects! 🚀",
                parse_mode="Markdown")
        except:
            pass
    else:
        await state.update_data(temp_premium_uid=uid)
        await state.set_state(AdminStates.waiting_premium_days)
        await message.answer(f"⏱ How many days of premium for `{uid}`? Send number of days:",
                             parse_mode="Markdown")


@router.message(AdminStates.waiting_premium_days)
async def receive_premium_days(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return
    try:
        days = int(message.text.strip())
    except:
        await message.answer("❌ Invalid number.")
        return
    data = await state.get_data()
    uid = data["temp_premium_uid"]
    await set_premium(uid, days)
    await state.clear()
    await message.answer(f"✅ User `{uid}` given `{days}` days premium!", parse_mode="Markdown",
                         reply_markup=admin_keyboard())
    try:
        await message.bot.send_message(uid,
            f"🎉 You have been granted *{days} days Premium* access!\n\nEnjoy unlimited projects! 🚀",
            parse_mode="Markdown")
    except:
        pass


# ─── Remove Premium ───────────────────────────────────────────────

@router.callback_query(F.data == "admin_remove_premium")
async def admin_remove_premium(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return
    await callback.message.edit_text("➖ *Remove Premium*\n\nSend User ID:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_back")]
        ]))
    await state.set_state(AdminStates.waiting_remove_premium_id)


@router.message(AdminStates.waiting_remove_premium_id)
async def receive_remove_premium_id(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return
    try:
        uid = int(message.text.strip())
    except:
        await message.answer("❌ Invalid ID.")
        return
    await set_premium(uid, 0)
    await state.clear()
    await message.answer(f"✅ Premium removed from `{uid}`", parse_mode="Markdown",
                         reply_markup=admin_keyboard())


# ─── Ban / Unban ──────────────────────────────────────────────────

@router.callback_query(F.data == "admin_ban")
async def admin_ban(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return
    await callback.message.edit_text("🚫 *Ban User*\n\nSend User ID to ban:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_back")]
        ]))
    await state.set_state(AdminStates.waiting_ban_id)


@router.message(AdminStates.waiting_ban_id)
async def receive_ban_id(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return
    try:
        uid = int(message.text.strip())
    except:
        await message.answer("❌ Invalid ID.")
        return
    if uid == OWNER_ID:
        await message.answer("❌ You cannot ban yourself.")
        return
    await ban_user(uid, True)
    await state.clear()
    await message.answer(f"✅ User `{uid}` has been banned.", parse_mode="Markdown",
                         reply_markup=admin_keyboard())


@router.callback_query(F.data == "admin_unban")
async def admin_unban(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return
    await callback.message.edit_text("✅ *Unban User*\n\nSend User ID to unban:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_back")]
        ]))
    await state.set_state(AdminStates.waiting_unban_id)


@router.message(AdminStates.waiting_unban_id)
async def receive_unban_id(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return
    try:
        uid = int(message.text.strip())
    except:
        await message.answer("❌ Invalid ID.")
        return
    await ban_user(uid, False)
    await state.clear()
    await message.answer(f"✅ User `{uid}` has been unbanned.", parse_mode="Markdown",
                         reply_markup=admin_keyboard())


@router.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return
    await state.clear()
    await callback.message.edit_text(
        "🛡 *God Madara Admin Panel*",
        parse_mode="Markdown",
        reply_markup=admin_keyboard()
    )


# ─── Download User Project ────────────────────────────────────────

@router.message(Command("download_project"))
async def download_project_cmd(message: Message):
    """Usage: /download_project <project_id>"""
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Usage: `/download_project <project_id>`", parse_mode="Markdown")
        return
    project_id = parts[1]
    project = await get_project(project_id)
    if not project:
        await message.answer("❌ Project not found.")
        return

    path = project["project_path"]
    zip_path = f"/tmp/{project['project_name']}_backup.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(path):
            for file in files:
                fp = os.path.join(root, file)
                zf.write(fp, os.path.relpath(fp, path))

    await message.answer_document(
        FSInputFile(zip_path),
        caption=f"📦 Project: `{project['project_name']}`\nUser: `{project['user_id']}`",
        parse_mode="Markdown"
)
  
