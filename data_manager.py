# /antiscam/data_manager.py

import os
import json
import logging
from config import (
    FED_CONFIG_FILE, KEYWORDS_FILE, FED_STATS_FILE, FED_BANS_FILE,
    SYNC_STATUS_FILE, SCAM_SERVERS_FILE, SYSTEM_PROMPT_FILE,
    stats_lock, keywords_lock, fed_bans_lock, sync_status_lock
)

logger = logging.getLogger()

# --- DATA HANDLING ---
def load_federation_config():
    if os.path.exists(FED_CONFIG_FILE):
        with open(FED_CONFIG_FILE, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Could not decode {FED_CONFIG_FILE}.")
                return {}
    else:
        logger.error(f"{FED_CONFIG_FILE} not found.")
        return {}

def load_scam_servers() -> list[int]:
    """Loads the list of known scam server IDs from its JSON file."""
    if not os.path.exists(SCAM_SERVERS_FILE):
        logger.warning(f"{SCAM_SERVERS_FILE} not found. Server identity check will be limited.")
        return []
    with open(SCAM_SERVERS_FILE, 'r') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            logger.error(f"Could not decode {SCAM_SERVERS_FILE}. Returning empty list.")
            return []

async def load_fed_bans():
    """Loads the master federated ban list from its JSON file."""
    async with fed_bans_lock:
        if not os.path.exists(FED_BANS_FILE):
            return {}
        with open(FED_BANS_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Could not decode {FED_BANS_FILE}.")
                return {}

async def save_fed_bans(data: dict):
    """Saves the master federated ban list to its JSON file."""
    async with fed_bans_lock:
        with open(FED_BANS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

async def load_sync_status():
    """Loads the list of synced guild IDs."""
    async with sync_status_lock:
        if not os.path.exists(SYNC_STATUS_FILE):
            return {"synced_guild_ids": []}
        with open(SYNC_STATUS_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Could not decode {SYNC_STATUS_FILE}.")
                return {"synced_guild_ids": []}

async def save_sync_status(data: dict):
    """Saves the list of synced guild IDs."""
    async with sync_status_lock:
        with open(SYNC_STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

async def load_fed_stats():
    async with stats_lock:
        if os.path.exists(FED_STATS_FILE):
            with open(FED_STATS_FILE, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}

async def save_fed_stats(data: dict):
    async with stats_lock:
        with open(FED_STATS_FILE, 'w') as f:
            json.dump(data, f, indent=4)

async def load_keywords():
    async with keywords_lock:
        if os.path.exists(KEYWORDS_FILE):
            with open(KEYWORDS_FILE, 'r', encoding='utf-8') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    logger.error(f"Could not decode {KEYWORDS_FILE}.")
                    return None
        else:
            logger.error(f"{KEYWORDS_FILE} not found.")
            return None

async def save_keywords(keywords_data: dict):
    async with keywords_lock:
        with open(KEYWORDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(keywords_data, f, indent=4)

def load_system_prompt() -> str:
    """Loads the LLM system prompt from its text file."""
    if not os.path.exists(SYSTEM_PROMPT_FILE):
        logger.error(f"CRITICAL: {SYSTEM_PROMPT_FILE} not found. AI features will be severely degraded.")
        return "You are a helpful security assistant. Classify the user's message as MALICIOUS, SUSPICIOUS, or SAFE."
    with open(SYSTEM_PROMPT_FILE, 'r', encoding='utf-8') as f:
        return f.read()