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
load_dotenv()
BOT_TOKEN = os.getenv("ANTISCAM_BOT_TOKEN")

GLOBAL_CONFIG_FILE = "data/global.yaml"
SERVERS_CONFIG_DIR = "data/servers"
LEGACY_CONFIG_FILE = "legacy/data/config.json"
LEGACY_KEYWORDS_FILE = "legacy/data/keywords.json"
FED_STATS_FILE = "data/stats.json"
SYNC_STATUS_FILE = "data/sync_status.json"
SCAM_SERVERS_FILE = "data/scam_servers.json"
SYSTEM_PROMPT_FILE = "data/system_prompt.txt"

stats_lock = asyncio.Lock()
keywords_lock = asyncio.Lock()
config_lock = asyncio.Lock()
sync_status_lock = asyncio.Lock()

