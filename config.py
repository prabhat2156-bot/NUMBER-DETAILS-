import os
from dotenv import load_dotenv
from datetime import timedelta

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
MONGO_URI = os.getenv("MONGO_URI")
OWNER_TOKEN = os.getenv("OWNER_TOKEN", "god_madara_admin_2024")

# Hosting Config
MAX_FREE_PROJECTS = 1
FILE_LINK_EXPIRY = 600  # 10 minutes
BACKUP_INTERVAL = 300   # 5 minutes

# File Manager
FILE_MANAGER_PORT = int(os.getenv("FILE_MANAGER_PORT", 8080))
