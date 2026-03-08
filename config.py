# /antiscam/config.py

import sys
import logging
from dotenv import load_dotenv
import asyncio
import os

# --- LOGGING SETUP ---
# This setup is perfect and should stay here.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger('google').setLevel(logging.WARNING)

logging.getLogger('google.generativeai').setLevel(logging.WARNING)
logging.getLogger('google.api_core').setLevel(logging.WARNING)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logging.getLogger('discord').setLevel(logging.INFO)


# --- CONSTANTS & CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
SERVERS_CONFIG_DIR = os.path.join(DATA_DIR, "servers")
LEGACY_DATA_DIR = os.path.join(BASE_DIR, "legacy", "data")
COGS_DIR = os.path.join(BASE_DIR, "cogs")

load_dotenv(os.path.join(BASE_DIR, ".env"))
BOT_TOKEN = os.getenv("ANTISCAM_BOT_TOKEN")

GLOBAL_CONFIG_FILE = os.path.join(DATA_DIR, "global.yaml")
LEGACY_CONFIG_FILE = os.path.join(LEGACY_DATA_DIR, "config.json")
LEGACY_KEYWORDS_FILE = os.path.join(LEGACY_DATA_DIR, "keywords.json")
FED_STATS_FILE = os.path.join(DATA_DIR, "stats.json")
SYNC_STATUS_FILE = os.path.join(DATA_DIR, "sync_status.json")
SCAM_SERVERS_FILE = os.path.join(DATA_DIR, "scam_servers.json")
SYSTEM_PROMPT_FILE = os.path.join(DATA_DIR, "system_prompt.txt")

stats_lock = asyncio.Lock()
keywords_lock = asyncio.Lock()
config_lock = asyncio.Lock()
sync_status_lock = asyncio.Lock()

