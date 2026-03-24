from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart
from database import create_user, is_user_banned
from config import OWNER_ID

router = Router()


def main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📁 New Project", callback_data="new_project"),
            InlineKeyboardButton(text="🗂 My Projects", callback_data="my_projects"),
        ],
        [
            InlineKeyboardButton(text="👑 Premium", callback_data="premium_info"),
            InlineKeyboardButton(text="🤖 Bot Status", callback_data="bot_status"),
        ]
    ])


@router.message(CommandStart())
async def start_handler(message: Message):
    user = message.from_user
    if await is_user_banned(user.id):
        await message.answer("🚫 You are banned from using this bot.")
        return

    await create_user(user.id, user.username or "", user.full_name)

    text = (
        "⚡️ *Welcome to God Madara Hosting Bot* ⚡️\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🌌 *The Ultimate Python Script Hosting Platform*\n\n"
        "🔥 Run your Python bots & scripts 24/7\n"
        "📦 Easy file upload & management\n"
        "🌐 Web-based file manager\n"
        "📊 Real-time logs & status\n"
        "⚙️ Auto dependency installation\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "👤 *User:* " + (f"@{user.username}" if user.username else user.full_name) + "\n"
        "🆔 *ID:* `" + str(user.id) + "`\n\n"
        "Choose an option below to get started 👇"
    )

    await message.answer(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())


@router.callback_query(F.data == "main_menu")
async def back_to_main(callback: CallbackQuery):
    await callback.message.edit_text(
        "⚡️ *God Madara Hosting Bot* — Main Menu\n\nChoose an option 👇",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )


@router.callback_query(F.data == "premium_info")
async def premium_info(callback: CallbackQuery):
    from database import is_user_premium
    is_prem = await is_user_premium(callback.from_user.id)
    status = "✅ *You are a Premium user!*" if is_prem else "❌ *You are on Free plan.*"

    text = (
        f"👑 *Premium Information*\n\n"
        f"{status}\n\n"
        f"*Free Plan:*\n"
        f"• 1 project\n"
        f"• Basic hosting\n\n"
        f"*Premium Plan:*\n"
        f"• ✅ Unlimited projects\n"
        f"• ✅ Priority support\n"
        f"• ✅ Unlimited uptime\n\n"
        f"Contact the owner to get Premium!"
    )
    await callback.message.edit_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")]
        ]))


@router.callback_query(F.data == "bot_status")
async def bot_status(callback: CallbackQuery):
    from database import get_all_users, get_running_projects, get_all_projects
    users = await get_all_users()
    running = await get_running_projects()
    all_proj = await get_all_projects()

    text = (
        f"🤖 *Bot Status*\n\n"
        f"👥 Total Users: `{len(users)}`\n"
        f"📁 Total Projects: `{len(all_proj)}`\n"
        f"▶️ Running Scripts: `{len(running)}`\n\n"
        f"✅ Bot is online and running 24/7"
    )
    await callback.message.edit_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")]
        ]))
  
