# /antiscam/data_manager.py

import os
import json
import logging
import aiosqlite
from typing import Optional

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from pydantic import BaseModel, ValidationError

from config import (
    GLOBAL_CONFIG_FILE,
    SERVERS_CONFIG_DIR,
    LEGACY_CONFIG_FILE,
    LEGACY_KEYWORDS_FILE,
    FED_STATS_FILE,
    SYNC_STATUS_FILE,
    SCAM_SERVERS_FILE,
    SYSTEM_PROMPT_FILE,
    stats_lock,
    keywords_lock,
    sync_status_lock,
)

logger = logging.getLogger()
DB_FILE = "antiscam.db"

_yaml = YAML()
_yaml.preserve_quotes = True
_yaml.indent(mapping=2, sequence=4, offset=2)
_yaml.width = 120

# --- IN-MEMORY CACHES ---
_config_cache: Optional[dict] = None
_keywords_cache: Optional[dict] = None
_config_cache_mtime: Optional[float] = None
_keywords_cache_mtime: Optional[float] = None
_config_cache_source: Optional[str] = None
_keywords_cache_source: Optional[str] = None

# --- YAML + VALIDATION MODELS ---
class LlmSettingsModel(BaseModel):
    automation_mode: str = "off"
    automation_delay_seconds: int = 180
    assign_role_on_safe: bool = False
    safe_role_id: Optional[int] = None

class FloodDetectionModel(BaseModel):
    enabled: bool = False
    time_window_seconds: int = 5
    message_threshold: int = 5
    channel_threshold: int = 2

class UsernameKeywordsModel(BaseModel):
    substring: list[str] = []
    smart: list[str] = []

class BioMessageKeywordsModel(BaseModel):
    simple_keywords: list[str] = []
    regex_patterns: list[str] = []
    whitelisted_domains_regex: list[str] = []

class KeywordRulesetModel(BaseModel):
    username_keywords: UsernameKeywordsModel = UsernameKeywordsModel()
    bio_and_message_keywords: BioMessageKeywordsModel = BioMessageKeywordsModel()

class GlobalKeywordsModel(BaseModel):
    username_keywords: UsernameKeywordsModel = UsernameKeywordsModel()
    bio_and_message_keywords: BioMessageKeywordsModel = BioMessageKeywordsModel()
    suspicious_identity_tags: list[str] = []

class DefaultsModel(BaseModel):
    timeout_duration_minutes: int = 10
    delete_messages_on_ban_days: int = 0
    llm_settings: LlmSettingsModel = LlmSettingsModel()
    flood_detection: FloodDetectionModel = FloodDetectionModel()

class GlobalConfigModel(BaseModel):
    server_name_to_id: dict[str, str] = {}
    federated_guild_ids: list[int] = []
    bot_owner_id: Optional[int] = None
    log_channel_id: Optional[int] = None
    manual_ban_default_reason: str = "Scam link"
    defaults: DefaultsModel = DefaultsModel()
    global_keywords: GlobalKeywordsModel = GlobalKeywordsModel()

class ChannelsModel(BaseModel):
    action_alert: Optional[int] = None
    federation_notice: Optional[int] = None

class RolesModel(BaseModel):
    moderator: list[int] = []
    whitelisted: list[int] = []

class TimeoutsModel(BaseModel):
    minutes: Optional[int] = None
    delete_messages_days: Optional[int] = None

class ServerConfigModel(BaseModel):
    guild_id: int
    name: Optional[str] = None
    channels: ChannelsModel = ChannelsModel()
    roles: RolesModel = RolesModel()
    timeouts: TimeoutsModel = TimeoutsModel()
    llm_settings: Optional[LlmSettingsModel] = None
    flood_detection: Optional[FloodDetectionModel] = None
    keywords: KeywordRulesetModel = KeywordRulesetModel()

# --- YAML HELPERS ---

def _read_yaml(path: str) -> Optional[CommentedMap]:
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        data = _yaml.load(f)
    if data is None:
        return CommentedMap()
    return data


def _write_yaml(path: str, data: CommentedMap) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        _yaml.dump(data, f)


def _compute_yaml_mtime() -> Optional[float]:
    paths = []
    if os.path.exists(GLOBAL_CONFIG_FILE):
        paths.append(GLOBAL_CONFIG_FILE)
    if os.path.isdir(SERVERS_CONFIG_DIR):
        for filename in os.listdir(SERVERS_CONFIG_DIR):
            if filename.endswith(".yaml"):
                paths.append(os.path.join(SERVERS_CONFIG_DIR, filename))
    if not paths:
        return None
    try:
        return max(os.path.getmtime(path) for path in paths)
    except OSError:
        return None


def _legacy_mtime(path: str) -> Optional[float]:
    try:
        return os.path.getmtime(path) if os.path.exists(path) else None
    except OSError:
        return None


def _model_validate(model_cls, data):
    try:
        if hasattr(model_cls, "model_validate"):
            return model_cls.model_validate(data)
        return model_cls.parse_obj(data)
    except ValidationError as e:
        logger.error(f"Validation error in {model_cls.__name__}: {e}")
        return None


def _load_global_yaml() -> Optional[GlobalConfigModel]:
    raw = _read_yaml(GLOBAL_CONFIG_FILE)
    if raw is None:
        return None
    return _model_validate(GlobalConfigModel, raw)


def _load_server_yaml(path: str) -> Optional[ServerConfigModel]:
    raw = _read_yaml(path)
    if raw is None:
        return None
    return _model_validate(ServerConfigModel, raw)


def _load_server_configs() -> list[ServerConfigModel]:
    if not os.path.isdir(SERVERS_CONFIG_DIR):
        return []
    server_models: list[ServerConfigModel] = []
    for filename in os.listdir(SERVERS_CONFIG_DIR):
        if not filename.endswith('.yaml'):
            continue
        path = os.path.join(SERVERS_CONFIG_DIR, filename)
        model = _load_server_yaml(path)
        if model:
            server_models.append(model)
    return server_models


def _load_legacy_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            logger.error(f"Could not decode legacy JSON at {path}.")
            return None


def _keyword_ruleset_to_dict(ruleset: KeywordRulesetModel) -> dict:
    data = _model_dump(ruleset)
    # Ensure optional keys exist
    data.setdefault("username_keywords", {"substring": [], "smart": []})
    data.setdefault("bio_and_message_keywords", {"simple_keywords": [], "regex_patterns": [], "whitelisted_domains_regex": []})
    data["username_keywords"].setdefault("substring", [])
    data["username_keywords"].setdefault("smart", [])
    data["bio_and_message_keywords"].setdefault("simple_keywords", [])
    data["bio_and_message_keywords"].setdefault("regex_patterns", [])
    data["bio_and_message_keywords"].setdefault("whitelisted_domains_regex", [])
    return data


def _global_keywords_to_dict(global_keywords: GlobalKeywordsModel) -> dict:
    data = _model_dump(global_keywords)
    data.setdefault("username_keywords", {"substring": [], "smart": []})
    data.setdefault("bio_and_message_keywords", {"simple_keywords": [], "regex_patterns": [], "whitelisted_domains_regex": []})
    data["username_keywords"].setdefault("substring", [])
    data["username_keywords"].setdefault("smart", [])
    data["bio_and_message_keywords"].setdefault("simple_keywords", [])
    data["bio_and_message_keywords"].setdefault("regex_patterns", [])
    data["bio_and_message_keywords"].setdefault("whitelisted_domains_regex", [])
    data.setdefault("suspicious_identity_tags", [])
    return data


def _model_dump(model) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()

# --- DATABASE INITIALIZATION ---
async def init_db():
    """Initializes the SQLite database and creates the table if missing."""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bans (
                user_id TEXT PRIMARY KEY,
                username TEXT,
                reason TEXT,
                origin_guild_id INTEGER,
                origin_guild_name TEXT,
                moderator_id INTEGER,
                timestamp TEXT,
                bio_at_import TEXT
            )
        """)
        await db.commit()

# --- NEW SQLITE FUNCTIONS (Replacing Load/Save Bans) ---

async def db_get_ban(user_id: int | str):
    """
    Fetches a specific ban record from the database.
    Returns a dict or None.
    """
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM bans WHERE user_id = ?", (str(user_id),)) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
            return None

async def db_add_ban(user_id, username, reason, origin_id, origin_name, mod_id, timestamp, bio=None):
    """
    Adds or Updates a ban record in the database.
    """
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            INSERT OR REPLACE INTO bans 
            (user_id, username, reason, origin_guild_id, origin_guild_name, moderator_id, timestamp, bio_at_import)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (str(user_id), username, reason, origin_id, origin_name, mod_id, timestamp, bio))
        await db.commit()

async def db_remove_ban(user_id: int | str):
    """Removes a ban record from the database."""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM bans WHERE user_id = ?", (str(user_id),))
        await db.commit()

async def db_get_ban_count():
    """Returns the total number of banned users."""
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM bans") as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

async def db_bulk_import_bans(ban_list: list[tuple]):
    """
    Optimized for GitHub Sync.
    Accepts a list of tuples: (user_id, username, reason, origin_id, origin_name, mod_id, timestamp, bio)
    Uses INSERT OR IGNORE to skip existing bans efficiently.
    Returns the number of rows actually inserted.
    """
    if not ban_list:
        return 0

    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.executemany("""
            INSERT OR IGNORE INTO bans 
            (user_id, username, reason, origin_guild_id, origin_guild_name, moderator_id, timestamp, bio_at_import)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, ban_list)
        await db.commit()
        return cursor.rowcount

# --- CONFIG & KEYWORDS ---

def load_federation_config():
    global _config_cache, _config_cache_mtime
    yaml_mtime = _compute_yaml_mtime()
    if _config_cache is not None and yaml_mtime is not None and _config_cache_mtime == yaml_mtime:
        return _config_cache

    global_model = _load_global_yaml()
    server_models = _load_server_configs()

    if global_model is None:
        legacy = _load_legacy_json(LEGACY_CONFIG_FILE)
        if legacy is not None:
            logger.warning("Loaded legacy config JSON. Migrate to YAML for full functionality.")
            _config_cache = legacy
            _config_cache_mtime = _legacy_mtime(LEGACY_CONFIG_FILE)
            global _config_cache_source
            _config_cache_source = "legacy"
            return legacy
        logger.error("Global config YAML not found or invalid, and no legacy JSON available.")
        return {}

    config = {}
    config["server_name_to_id"] = global_model.server_name_to_id or {}

    federated_ids = list(global_model.federated_guild_ids or [])
    if not federated_ids and server_models:
        federated_ids = [server.guild_id for server in server_models]
    config["federated_guild_ids"] = federated_ids

    defaults = global_model.defaults

    config["bot_owner_id"] = global_model.bot_owner_id
    config["log_channel_id"] = global_model.log_channel_id
    config["manual_ban_default_reason"] = global_model.manual_ban_default_reason

    config["timeout_duration_minutes_default"] = defaults.timeout_duration_minutes
    config["delete_messages_on_ban_days_default"] = defaults.delete_messages_on_ban_days

    config["flood_detection"] = _model_dump(defaults.flood_detection)

    llm_defaults = _model_dump(defaults.llm_settings)
    config["llm_settings"] = {
        "defaults": llm_defaults,
        "per_guild_settings": {}
    }

    config["timeout_duration_minutes_per_guild"] = {}
    config["delete_messages_on_ban_days_per_guild"] = {}
    config["moderator_roles_per_guild"] = {}
    config["whitelisted_roles_per_guild"] = {}
    config["action_alert_channels"] = {}
    config["federation_notice_channels"] = {}

    for server in server_models:
        guild_id_str = str(server.guild_id)
        if server.timeouts.minutes is not None:
            config["timeout_duration_minutes_per_guild"][guild_id_str] = server.timeouts.minutes
        if server.timeouts.delete_messages_days is not None:
            config["delete_messages_on_ban_days_per_guild"][guild_id_str] = server.timeouts.delete_messages_days

        if server.roles.moderator:
            config["moderator_roles_per_guild"][guild_id_str] = server.roles.moderator
        if server.roles.whitelisted:
            config["whitelisted_roles_per_guild"][guild_id_str] = server.roles.whitelisted

        if server.channels.action_alert is not None:
            config["action_alert_channels"][guild_id_str] = server.channels.action_alert
        if server.channels.federation_notice is not None:
            config["federation_notice_channels"][guild_id_str] = server.channels.federation_notice

        if server.llm_settings:
            config["llm_settings"]["per_guild_settings"][guild_id_str] = _model_dump(server.llm_settings)

    _config_cache = config
    _config_cache_mtime = yaml_mtime
    _config_cache_source = "yaml"
    return config


def load_scam_servers() -> list[int]:
    if not os.path.exists(SCAM_SERVERS_FILE):
        logger.warning(f"{SCAM_SERVERS_FILE} not found.")
        return []
    with open(SCAM_SERVERS_FILE, 'r') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []

async def load_sync_status():
    async with sync_status_lock:
        if not os.path.exists(SYNC_STATUS_FILE):
            return {"synced_guild_ids": []}
        with open(SYNC_STATUS_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {"synced_guild_ids": []}

async def save_sync_status(data: dict):
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
        global _keywords_cache, _keywords_cache_mtime
        yaml_mtime = _compute_yaml_mtime()
        if _keywords_cache is not None and yaml_mtime is not None and _keywords_cache_mtime == yaml_mtime:
            return _keywords_cache

        global_model = _load_global_yaml()
        server_models = _load_server_configs()

        if global_model is None:
            legacy = _load_legacy_json(LEGACY_KEYWORDS_FILE)
            if legacy is not None:
                logger.warning("Loaded legacy keywords JSON. Migrate to YAML for full functionality.")
                _keywords_cache = legacy
                _keywords_cache_mtime = _legacy_mtime(LEGACY_KEYWORDS_FILE)
                global _keywords_cache_source
                _keywords_cache_source = "legacy"
                return legacy
            logger.error("Global keywords YAML not found or invalid, and no legacy JSON available.")
            return None

        keywords_data = {
            "server_name_to_id": global_model.server_name_to_id or {},
            "global_keywords": _global_keywords_to_dict(global_model.global_keywords),
            "per_server_keywords": {},
        }

        for server in server_models:
            guild_id_str = str(server.guild_id)
            keywords_data["per_server_keywords"][guild_id_str] = _keyword_ruleset_to_dict(server.keywords)

        _keywords_cache = keywords_data
        _keywords_cache_mtime = yaml_mtime
        _keywords_cache_source = "yaml"
        return keywords_data

async def save_keywords(keywords_data: dict):
    async with keywords_lock:
        global _keywords_cache, _keywords_cache_mtime
        # If YAML config doesn't exist yet, fall back to legacy JSON to avoid data loss.
        if not os.path.exists(GLOBAL_CONFIG_FILE) and os.path.exists(LEGACY_KEYWORDS_FILE):
            with open(LEGACY_KEYWORDS_FILE, 'w', encoding='utf-8') as f:
                json.dump(keywords_data, f, indent=4)
            logger.warning("Saved keywords to legacy JSON because YAML config is missing.")
            _keywords_cache = keywords_data
            _keywords_cache_mtime = _legacy_mtime(LEGACY_KEYWORDS_FILE)
            global _keywords_cache_source
            _keywords_cache_source = "legacy"
            return

        global_yaml = _read_yaml(GLOBAL_CONFIG_FILE) or CommentedMap()
        global_yaml["global_keywords"] = keywords_data.get("global_keywords", {})

        if "server_name_to_id" in keywords_data:
            global_yaml["server_name_to_id"] = keywords_data.get("server_name_to_id", {})

        _write_yaml(GLOBAL_CONFIG_FILE, global_yaml)

        per_server = keywords_data.get("per_server_keywords", {})
        for guild_id_str, ruleset in per_server.items():
            server_path = os.path.join(SERVERS_CONFIG_DIR, f"{guild_id_str}.yaml")
            server_yaml = _read_yaml(server_path) or CommentedMap()
            server_yaml["guild_id"] = int(guild_id_str)
            server_yaml["keywords"] = ruleset
            _write_yaml(server_path, server_yaml)

        _keywords_cache = keywords_data
        _keywords_cache_mtime = _compute_yaml_mtime()
        _keywords_cache_source = "yaml"


def get_cache_state() -> dict:
    return {
        "config": {
            "mtime": _config_cache_mtime,
            "source": _config_cache_source,
            "yaml_mtime": _compute_yaml_mtime(),
            "legacy_mtime": _legacy_mtime(LEGACY_CONFIG_FILE),
        },
        "keywords": {
            "mtime": _keywords_cache_mtime,
            "source": _keywords_cache_source,
            "yaml_mtime": _compute_yaml_mtime(),
            "legacy_mtime": _legacy_mtime(LEGACY_KEYWORDS_FILE),
        }
    }


def load_system_prompt() -> str:
    if not os.path.exists(SYSTEM_PROMPT_FILE):
        logger.error(f"CRITICAL: {SYSTEM_PROMPT_FILE} not found.")
        return "Classify the user's message as MALICIOUS, SUSPICIOUS, or SAFE."
    with open(SYSTEM_PROMPT_FILE, 'r', encoding='utf-8') as f:
        return f.read()

async def db_search_bans(query: str):
    """Searches bans by User ID or Username (partial match). Returns list of (id, data_dict)."""
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row

        if query.isdigit():
            sql = "SELECT * FROM bans WHERE user_id = ?"
            params = (query,)
        else:
            sql = "SELECT * FROM bans WHERE username LIKE ?"
            params = (f"%{query}%",)

        async with db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            return [(row['user_id'], dict(row)) for row in rows]

async def db_get_all_bans():
    """Fetches ALL bans. Use carefully for Onboarding. Returns dict {id: data}."""
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM bans") as cursor:
            rows = await cursor.fetchall()
            return {row['user_id']: dict(row) for row in rows}
