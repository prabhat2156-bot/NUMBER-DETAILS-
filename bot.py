import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.fsm.storage.mongo import MongoStorage
from aiogram.client.default import DefaultBotProperties
from motor.motor_asyncio import AsyncIOMotorClient

from config import BOT_TOKEN, MONGO_URI
from handlers import start, projects, admin
from core.backup import backup_loop
from core.runner import restore_running_projects

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

async def main():
    # MongoDB FSM storage so states survive restarts
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    storage = MongoStorage(client=mongo_client, db_name="god_madara_bot",
                           collection_name="fsm_states")

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    )
    dp = Dispatcher(storage=storage)

    # Register routers
    dp.include_router(start.router)
    dp.include_router(projects.router)
    dp.include_router(admin.router)

    # On startup: restore running projects & start backup loop
    async def on_startup():
        await restore_running_projects()
        asyncio.create_task(backup_loop())
        logging.info("✅ God Madara Bot started!")

    dp.startup.register(on_startup)

    await dp.start_polling(bot, drop_pending_updates=True)


def run_web():
    """Run Flask file manager in a thread"""
    import threading
    from web.app import app
    t = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False),
        daemon=True
    )
    t.start()


if __name__ == "__main__":
    run_web()
    asyncio.run(main())
  
