import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")
MONGO_URI = os.getenv("MONGO_URI")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
DOMAIN = os.getenv("DOMAIN", "http://localhost:10000")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 10000))
