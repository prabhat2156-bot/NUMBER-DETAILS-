import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_URI

client = AsyncIOMotorClient(MONGO_URI)
db = client["god_madara_bot"]

users_col = db["users"]
projects_col = db["projects"]
backups_col = db["backups"]
tokens_col = db["file_manager_tokens"]


# ─── User Operations ──────────────────────────────────────────────

async def get_user(user_id: int):
    user = await users_col.find_one({"user_id": user_id})
    return user

async def create_user(user_id: int, username: str, full_name: str):
    existing = await get_user(user_id)
    if existing:
        return existing
    user = {
        "user_id": user_id,
        "username": username,
        "full_name": full_name,
        "is_premium": False,
        "is_banned": False,
        "premium_expiry": None,
        "joined_at": datetime.datetime.utcnow(),
    }
    await users_col.insert_one(user)
    return user

async def set_premium(user_id: int, days: int = -1):
    """days=-1 means permanent"""
    if days == -1:
        expiry = None
        is_premium = True
    elif days == 0:
        expiry = None
        is_premium = False
    else:
        expiry = datetime.datetime.utcnow() + datetime.timedelta(days=days)
        is_premium = True
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": {"is_premium": is_premium, "premium_expiry": expiry}}
    )

async def ban_user(user_id: int, ban: bool = True):
    await users_col.update_one({"user_id": user_id}, {"$set": {"is_banned": ban}})

async def get_all_users():
    return await users_col.find({}).to_list(None)

async def get_premium_users():
    return await users_col.find({"is_premium": True}).to_list(None)

async def is_user_banned(user_id: int) -> bool:
    user = await get_user(user_id)
    if not user:
        return False
    return user.get("is_banned", False)

async def is_user_premium(user_id: int) -> bool:
    user = await get_user(user_id)
    if not user:
        return False
    if not user.get("is_premium", False):
        return False
    expiry = user.get("premium_expiry")
    if expiry and datetime.datetime.utcnow() > expiry:
        await set_premium(user_id, 0)
        return False
    return True


# ─── Project Operations ───────────────────────────────────────────

async def create_project(user_id: int, project_name: str, project_path: str):
    project = {
        "user_id": user_id,
        "project_name": project_name,
        "project_path": project_path,
        "status": "stopped",
        "pid": None,
        "run_command": "python3 main.py",
        "last_run": None,
        "last_exit_code": None,
        "uptime_start": None,
        "created_at": datetime.datetime.utcnow(),
    }
    result = await projects_col.insert_one(project)
    project["_id"] = result.inserted_id
    return project

async def get_user_projects(user_id: int):
    return await projects_col.find({"user_id": user_id}).to_list(None)

async def get_project(project_id: str):
    from bson import ObjectId
    return await projects_col.find_one({"_id": ObjectId(project_id)})

async def get_project_by_name(user_id: int, name: str):
    return await projects_col.find_one({"user_id": user_id, "project_name": name})

async def update_project(project_id: str, data: dict):
    from bson import ObjectId
    await projects_col.update_one({"_id": ObjectId(project_id)}, {"$set": data})

async def delete_project(project_id: str):
    from bson import ObjectId
    await projects_col.delete_one({"_id": ObjectId(project_id)})

async def get_all_projects():
    return await projects_col.find({}).to_list(None)

async def get_running_projects():
    return await projects_col.find({"status": "running"}).to_list(None)


# ─── File Manager Token ───────────────────────────────────────────

async def create_fm_token(user_id: int, project_id: str, token: str):
    expiry = datetime.datetime.utcnow() + datetime.timedelta(seconds=600)
    await tokens_col.delete_many({"user_id": user_id, "project_id": project_id})
    await tokens_col.insert_one({
        "user_id": user_id,
        "project_id": project_id,
        "token": token,
        "expiry": expiry
    })

async def verify_fm_token(token: str):
    doc = await tokens_col.find_one({"token": token})
    if not doc:
        return None
    if datetime.datetime.utcnow() > doc["expiry"]:
        await tokens_col.delete_one({"token": token})
        return None
    return doc


# ─── Backup ───────────────────────────────────────────────────────

async def save_backup(data: dict):
    await backups_col.delete_many({})
    await backups_col.insert_one({"data": data, "saved_at": datetime.datetime.utcnow()})

async def get_latest_backup():
    return await backups_col.find_one({}, sort=[("saved_at", -1)])
                      
