# config.py
import os
from dotenv import load_dotenv

load_dotenv()
LOCAL_EXECUTION = os.getenv("LOCAL_EXECUTION", "False").lower() in ("true", "1", "t")
SOURCE_TYPE = os.getenv("SOURCE_TYPE", "csv")
SOURCE_PATH = os.getenv("SOURCE_PATH", "data/raw_messages_clean.csv")