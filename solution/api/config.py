# config.py
import os
from dotenv import load_dotenv

load_dotenv()
LOCAL_EXECUTION = os.getenv("LOCAL_EXECUTION", "False").lower() in ("true", "1", "t")
SOURCE_TYPE = os.getenv("SOURCE_TYPE", "csv")
SOURCE_PATH = os.getenv("SOURCE_PATH", "data/raw_messages_clean.csv")
DB_URL = os.getenv("DB_URL")
DB_PASS = os.getenv("TF_VAR_db_pass")
DB_HOST = os.getenv("DB_HOST_IP")