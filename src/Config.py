import os
import json

# Base directory is the src/ folder
_basedir = os.path.dirname(os.path.abspath(__file__))

# Load credentials
_credentials_path = os.path.join(_basedir, "credentials.json")

if not os.path.exists(_credentials_path):
    print(f"CRITICAL ERROR: '{_credentials_path}' not found!")
    print("Please copy 'src/credentials_example.json' to 'src/credentials.json' and fill in your bot token and owner ID.")
    exit(1)

with open(_credentials_path) as _f:
    _credentials = json.load(_f)

class Config:
    DATABASE_PATH = os.path.join(_basedir, "Database", "database.db")
    CREDENTIALS_PATH = _credentials_path
    LOG_FILE_PATH = os.path.join(_basedir, "Logger", "activity_log.txt")
    BOT_TOKEN = _credentials["bot_token"]
    OWNER_ID = int(_credentials["owner_id"])
