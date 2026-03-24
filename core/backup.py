import asyncio
import datetime
from database import (
    save_backup, get_latest_backup,
    users_col, projects_col
)


async def perform_backup():
    users = await users_col.find({}).to_list(None)
    projects = await projects_col.find({}).to_list(None)

    # Convert ObjectId to str for serialization
    for u in users:
        u["_id"] = str(u["_id"])
    for p in projects:
        p["_id"] = str(p["_id"])

    data = {
        "users": users,
        "projects": projects,
        "backed_up_at": datetime.datetime.utcnow().isoformat(),
    }
    await save_backup(data)


async def backup_loop():
    while True:
        await asyncio.sleep(300)  # every 5 minutes
        try:
            await perform_backup()
        except Exception as e:
            print(f"[Backup Error] {e}")
          
