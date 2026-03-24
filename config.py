import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
BASE_URL = os.getenv("BASE_URL", "[localhost](http://localhost:5000)")  # Your Render URL
SECRET_KEY = os.getenv("SECRET_KEY", "godmadara_secret_2025")
PROJECTS_DIR = os.getenv("PROJECTS_DIR", "./user_projects")
MAX_FREE_PROJECTS = 1
BACKUP_INTERVAL = 300  # 5 minutes
FILE_MANAGER_TOKEN_EXPIRY = 600  # 10 minutes

os.makedirs(PROJECTS_DIR, exist_ok=True)
