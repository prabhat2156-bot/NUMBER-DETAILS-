import os
import shutil
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command

router = Router()

# Base directory for user files
BASE_DIR = "user_files"

# Ensure base dir exists
os.makedirs(BASE_DIR, exist_ok=True)


# 📌 Get user folder
def get_user_dir(user_id: int):
    path = os.path.join(BASE_DIR, str(user_id))
    os.makedirs(path, exist_ok=True)
    return path


# =============================
# 📤 Upload File
# =============================
@router.message(F.document)
async def upload_file(message: Message):
    user_id = message.from_user.id
    user_dir = get_user_dir(user_id)

    file = message.document
    file_path = os.path.join(user_dir, file.file_name)

    await message.bot.download(file, destination=file_path)

    await message.reply(f"✅ File uploaded:\n`{file.file_name}`")


# =============================
# 📂 List Files
# =============================
@router.message(Command("files"))
async def list_files(message: Message):
    user_id = message.from_user.id
    user_dir = get_user_dir(user_id)

    files = os.listdir(user_dir)

    if not files:
        await message.reply("📂 No files found.")
        return

    file_list = "\n".join([f"• {f}" for f in files])
    await message.reply(f"📁 Your Files:\n\n{file_list}")


# =============================
# 📥 Download File
# =============================
@router.message(Command("get"))
async def get_file(message: Message):
    user_id = message.from_user.id
    user_dir = get_user_dir(user_id)

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("⚠️ Usage: /get filename")
        return

    filename = args[1]
    file_path = os.path.join(user_dir, filename)

    if not os.path.exists(file_path):
        await message.reply("❌ File not found.")
        return

    await message.reply_document(open(file_path, "rb"))


# =============================
# 🗑 Delete File
# =============================
@router.message(Command("delete"))
async def delete_file(message: Message):
    user_id = message.from_user.id
    user_dir = get_user_dir(user_id)

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("⚠️ Usage: /delete filename")
        return

    filename = args[1]
    file_path = os.path.join(user_dir, filename)

    if not os.path.exists(file_path):
        await message.reply("❌ File not found.")
        return

    os.remove(file_path)
    await message.reply(f"🗑 Deleted: `{filename}`")


# =============================
# 🧹 Clear All Files
# =============================
@router.message(Command("clearfiles"))
async def clear_files(message: Message):
    user_id = message.from_user.id
    user_dir = get_user_dir(user_id)

    shutil.rmtree(user_dir)
    os.makedirs(user_dir, exist_ok=True)

    await message.reply("🧹 All files deleted.")
