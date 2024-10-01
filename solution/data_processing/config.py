# config.py
import os
from dotenv import load_dotenv

load_dotenv()
DB_PASS = os.getenv('TF_VAR_db_pass')
DB_HOST = os.getenv('DB_HOST_IP')