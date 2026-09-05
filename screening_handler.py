# /antiscam/screening_handler.py

import os
import re
import aiohttp
import asyncio
import discord
from collections import deque
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit
from unidecode import unidecode
from typing import TYPE_CHECKING

from utils.helpers import get_timeout_minutes_for_guild, get_delete_days_for_guild, truncate_audit_reason
import data_manager
from config import logger
import llm_handler

if TYPE_CHECKING:
    from antiscam import AntiScamBot


# --- SCREENING ---
PROFILE_FETCH_TIMEOUT_SECONDS = 3
MAX_TRIGGER_FIELD_LENGTH = 1000


def get_member_identity_names(
    member: discord.Member,
    profile: discord.abc.User | None = None,
) -> list[str]:
    """Return every user-controlled name that Discord may show for a member."""
    identity_source = profile or member
    username = getattr(identity_source, "name", None) or member.name
    global_name = getattr(identity_source, "global_name", None)
    nickname = member.nick

    return list(dict.fromkeys(name for name in (username, global_name, nickname) if name))


def format_member_identity_context(
    member: discord.Member,
    profile: discord.abc.User | None = None,
) -> str:
    """Format all Discord name fields for moderator and AI review context."""
    identity_source = profile or member
    username = getattr(identity_source, "name", None) or member.name
    global_name = getattr(identity_source, "global_name", None) or "None"
    nickname = member.nick or "None"
    return f"Username: {username}\nGlobal display name: {global_name}\nServer nickname: {nickname}"


def get_member_name_triggers(
    member: discord.Member,
    keywords_data: dict,
    profile: discord.abc.User | None = None,
) -> list[str]:
    """Check every Discord name field against local and global username rules."""
    local_rules = keywords_data.get("per_server_keywords", {}).get(str(member.guild.id), {})
    global_rules = keywords_data.get("global_keywords", {})

    triggered_keywords = []
    for name in get_member_identity_names(member, profile):
        triggered_keywords.extend(check_text_for_keywords(name, local_rules.get("username_keywords", {})))
        triggered_keywords.extend(check_text_for_keywords(name, global_rules.get("username_keywords", {})))
    return list(dict.fromkeys(triggered_keywords))


def _build_keyword_screening_result(
    member: discord.Member,
    triggered_keywords: list[str],
    profile: discord.abc.User | None = None,
    bio: str = "",
) -> dict:
    identity_source = profile or member
    display_name = member.nick or getattr(identity_source, "global_name", None) or identity_source.name
    embed = discord.Embed(
        title="🚨 Flagged User",
        description=f"{member.mention} (`{member.id}`)",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_author(name=display_name, icon_url=member.display_avatar.url)
    if bio:
        embed.add_field(name="📝 Bio", value=bio[:1024], inline=False)
    embed.add_field(name="🚩 Trigger", value=f"`{_format_trigger_value(triggered_keywords)}`", inline=True)
    embed.add_field(name="Status", value="User timed out. Awaiting review...", inline=True)
    embed.add_field(name="Account Age", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
    return {"flagged": True, "embed": embed, "timeout_reason": "Flagged by keyword screening."}


def screen_member_name_change(
    member: discord.Member,
    keywords_data: dict,
    profile: discord.abc.User | None = None,
) -> dict:
    """Screen only mutable Discord name fields after a profile or nickname update."""
    triggered_keywords = get_member_name_triggers(member, keywords_data, profile)
    if not triggered_keywords:
        return {"flagged": False}
    return _build_keyword_screening_result(member, triggered_keywords, profile)


def _add_screening_latency(embed: discord.Embed, started_at: datetime) -> None:
    elapsed_ms = max(int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000), 0)
    embed.add_field(name="⏱️ Screening Latency", value=f"`{elapsed_ms} ms`", inline=True)


def _format_trigger_value(triggered_keywords: list[str]) -> str:
    unique_triggers = list(dict.fromkeys(triggered_keywords))
    if not unique_triggers:
        return "Unknown"

    joined = ", ".join(unique_triggers)
    if len(joined) <= MAX_TRIGGER_FIELD_LENGTH:
        return joined

    return joined[: MAX_TRIGGER_FIELD_LENGTH - 16] + "...(truncated)"


def check_for_flood(bot: 'AntiScamBot', message: discord.Message) -> bool:
    """
    Checks if a user's message constitutes a flood based on configured thresholds.
    Returns True if a flood is detected, False otherwise.
    """
    flood_config = bot.config.get("flood_detection", {})
    if not flood_config.get("enabled", False):
        return False

    now = datetime.now(timezone.utc)
    user_id = message.author.id
    guild_id = message.guild.id
    
    # Get or create the history for the user in the specific guild
    guild_history = bot.message_history.setdefault(guild_id, {})
    user_history = guild_history.setdefault(user_id, deque())

    # 1. Clean up old message entries from the user's history
    time_window = timedelta(seconds=flood_config.get("time_window_seconds", 5))
    while user_history and now - user_history[0][0] > time_window:
        user_history.popleft()
            
    # 2. Add the new message to the history
    user_history.append((now, message.channel.id))

    # 3. Check if the thresholds have been met
    message_threshold = flood_config.get("message_threshold", 5)
    channel_threshold = flood_config.get("channel_threshold", 2)

    if len(user_history) >= message_threshold:
        # Get the number of unique channels in the recent history
        unique_channels = len({channel_id for _, channel_id in user_history})
        if unique_channels >= channel_threshold:
            logger.info(f"Flood detected for user {message.author.name} ({user_id}). "
                        f"Messages: {len(user_history)}, Channels: {unique_channels}.")
            # Clear the history for this user to prevent repeated flagging on every subsequent message
            user_history.clear()
            return True

    return False

def prune_screening_caches(bot: 'AntiScamBot') -> None:
    now = datetime.now(timezone.utc)
    for key, checked_at in list(bot.bio_check_cache.items()):
        if (now - checked_at).total_seconds() >= 300:
            del bot.bio_check_cache[key]
    window = timedelta(seconds=bot.config.get("flood_detection", {}).get("time_window_seconds", 5))
    for guild_id, users in list(bot.message_history.items()):
        for user_id, history in list(users.items()):
            while history and now - history[0][0] > window:
                history.popleft()
            if not history:
                del users[user_id]
        if not users:
            del bot.message_history[guild_id]


async def screen_member(
    bot: 'AntiScamBot',
    member: discord.Member,
    keywords_data: dict,
    profile: discord.abc.User | None = None,
) -> dict:
    """
    Performs the complete screening process for a single member.
    """
    from ui.views import ScreeningView
    config = bot.config
    screening_started_at = datetime.now(timezone.utc)
    is_whitelisted_user = data_manager.is_user_whitelisted(member.id, config)

    ban_data = await data_manager.db_get_ban(member.id)

    if ban_data and not is_whitelisted_user:
        logger.info(f"SCREEN_MEMBER: Flagged {member.name} (Master List).")
        
        # In the DB, the column is 'reason', same as the JSON key
        original_reason = ban_data.get('reason', 'No reason recorded.')
        timeout_reason = "Flagged: User is on the master federated ban list."

        # In the DB, the column is 'bio_at_import', same as the JSON key
        imported_bio = ban_data.get("bio_at_import")

        embed = discord.Embed(
            title="🚨 Flagged User (Master Ban List)",
            description=f"**User:** {member.mention} (`{member.id}`)\nThis user is on the master federated ban list.",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
        embed.add_field(name="Original Ban Reason", value=f"```{original_reason[:1000]}```", inline=False)
        
        if imported_bio and imported_bio != "N/A":
            embed.add_field(name="📝 Bio at Time of Import", value=f"```{imported_bio[:1000]}```", inline=False)

        embed.add_field(name="Status", value="User timed out. Awaiting review...", inline=True)
        _add_screening_latency(embed, screening_started_at)
        
        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}

    found_bans = []
    if not is_whitelisted_user:
        federated_guild_ids = config.get("federated_guild_ids", [])
        async def check_partner(other_guild_id):
            if other_guild_id == member.guild.id:
                return None
            other_guild = bot.get_guild(other_guild_id)
            if not other_guild:
                return None
            async with bot.screening_semaphore:
                try:
                    entry = await asyncio.wait_for(other_guild.fetch_ban(member), timeout=10)
                    return {"guild_name": other_guild.name, "reason": entry.reason or "No reason provided."}
                except discord.NotFound:
                    return None
                except Exception as exc:
                    logger.warning("Error checking ban status for %s in %s: %s", member.id, other_guild.name, exc)
                    return None

        found_bans = [entry for entry in await asyncio.gather(*(
            check_partner(guild_id) for guild_id in dict.fromkeys(federated_guild_ids)
        )) if entry]

    if found_bans:
        banned_in_servers = ", ".join([ban['guild_name'] for ban in found_bans])
        timeout_reason = truncate_audit_reason(
            f"Flagged on join: User is banned in partner server(s): {banned_in_servers}."
        )
        
        embed = discord.Embed(title="🚨 User Banned Elsewhere", description=f"**User:** {member.mention} (`{member.id}`)\nThis user is already banned in **{len(found_bans)}** other federated server(s).", color=discord.Color.red(), timestamp=datetime.now(timezone.utc))
        embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
        for ban in found_bans:
            embed.add_field(name=f"Banned In: {ban['guild_name']}", value=f"```{ban['reason'][:1000]}```", inline=False)
        embed.add_field(name="Status", value="User timed out. Awaiting review...", inline=True)
        _add_screening_latency(embed, screening_started_at)
        embed.set_footer(text=f"User ID: {member.id}")

        guild_id_str = str(member.guild.id)
        llm_defaults = config.get("llm_settings", {}).get("defaults", {})
        llm_config = config.get("llm_settings", {}).get("per_guild_settings", {}).get(guild_id_str, llm_defaults)

        if llm_config.get("automation_mode") == "full":
            mod_channel_id = config.get("action_alert_channels", {}).get(str(member.guild.id))
            alert_channel = member.guild.get_channel(mod_channel_id) if mod_channel_id else None
            if alert_channel:
                view = ScreeningView(flagged_member_id=member.id)
                alert_message = await alert_channel.send(embed=embed, view=view)
                
                delay = llm_config.get("automation_delay_seconds", 180)
                ban_reason_detail = f"Banned based on federated status in: {banned_in_servers}"
                
                logger.info(f"Scheduling automated 'Banned Elsewhere' ban for {member.name} in {delay} seconds.")
                task = bot.loop.create_task(
                    delayed_banned_elsewhere_wrapper(delay, bot, alert_message, member, ban_reason_detail)
                )
                bot.pending_ai_actions[alert_message.id] = task
                return {
                    "flagged": True,
                    "embed": embed,
                    "timeout_reason": timeout_reason,
                    "skip_alert_dispatch": True,
                    "automated_action_pending": True,
                }

            logger.warning(
                "Full automation is enabled for %s (%s), but no valid alert channel is configured. "
                "Falling back to manual enforcement.",
                member.guild.name,
                member.guild.id,
            )

        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}

    fetched_profile = profile
    if fetched_profile is None:
        try:
            fetched_profile = await bot.fetch_user(member.id)
        except discord.NotFound:
            logger.warning(f"Could not fetch profile for {member.name} ({member.id}) during screening, user may no longer exist. Proceeding without bio check.")
        except Exception as e:
            logger.error(f"Could not fetch profile for {member.name} ({member.id}) to get bio. Proceeding without it. Error: {e}")

    user_profile = fetched_profile or member
    bio = getattr(user_profile, 'bio', "")

    identity_result = await check_server_identity(bot, member, profile=fetched_profile)
    if identity_result.get("flagged"):
        timeout_reason = identity_result.get("reason")
        embed = discord.Embed(
            title="🚨 Flagged User (Malicious Server Badge)",
            description=f"{member.mention} (`{member.id}`)",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
        embed.add_field(name="🚩 Trigger", value=f"`{timeout_reason}`", inline=False)
        embed.add_field(name="Status", value="User timed out. Awaiting review...", inline=True)
        embed.add_field(name="Account Age", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
        _add_screening_latency(embed, screening_started_at)
        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}
    triggered_keywords = get_member_name_triggers(member, keywords_data, user_profile)

    local_rules = keywords_data.get("per_server_keywords", {}).get(str(member.guild.id), {})
    global_rules = keywords_data.get("global_keywords", {})

    if bio:
        triggered_keywords.extend(
            check_text_for_keywords(
                bio,
                local_rules.get("bio_and_message_keywords", {}),
                regex_source_label="Local",
            )
        )
        triggered_keywords.extend(
            check_text_for_keywords(
                bio,
                global_rules.get("bio_and_message_keywords", {}),
                regex_source_label="Global",
            )
        )

    if triggered_keywords:
        result = _build_keyword_screening_result(member, triggered_keywords, user_profile, bio)
        _add_screening_latency(result["embed"], screening_started_at)
        return result

    return {"flagged": False}

async def screen_message(message: discord.Message, keywords_data: dict) -> dict:
    if not keywords_data:
        return {"flagged": False}
    screening_started_at = datetime.now(timezone.utc)

    triggered_keywords = []
    local_rules = keywords_data.get("per_server_keywords", {}).get(str(message.guild.id), {})
    global_rules = keywords_data.get("global_keywords", {})

    triggered_keywords.extend(
        check_text_for_keywords(
            message.content,
            local_rules.get("bio_and_message_keywords", {}),
            regex_source_label="Local",
        )
    )
    triggered_keywords.extend(
        check_text_for_keywords(
            message.content,
            global_rules.get("bio_and_message_keywords", {}),
            regex_source_label="Global",
        )
    )

    if triggered_keywords:
        trigger_value = _format_trigger_value(triggered_keywords)
        embed = discord.Embed(
            title="🚨 Flagged Message",
            description=f"**User:** {message.author.mention} (`{message.author.id}`)\n"
                        f"**Channel:** {message.channel.mention}",
            color=discord.Color.dark_red(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=f"{message.author.name}", icon_url=message.author.display_avatar.url)
        embed.add_field(name="📝 Flagged Message", value=f"```{message.content[:1000]}```", inline=False)
        embed.add_field(name="🚩 Trigger", value=f"`{trigger_value}`", inline=True)
        embed.add_field(name="Status", value="Message deleted. User timed out. Awaiting review...", inline=True)
        embed.add_field(name="Account Age", value=f"<t:{int(message.author.created_at.timestamp())}:R>", inline=True)
        _add_screening_latency(embed, screening_started_at)
        
        timeout_reason = truncate_audit_reason(f"Flagged message. Triggered by: {trigger_value}")
        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}
        
    return {"flagged": False}

async def screen_bio(bot: 'AntiScamBot', member: discord.Member, keywords_data: dict) -> dict:
    if not keywords_data:
        return {"flagged": False}
    screening_started_at = datetime.now(timezone.utc)

    bio = ""
    if hasattr(member, '_user') and hasattr(member._user, 'bio'):
        bio = member._user.bio
    
    if not bio:
        try:
            user_profile = await bot.fetch_user(member.id)
            bio = getattr(user_profile, 'bio', "")
        except Exception as e:
            logger.error(f"Error fetching profile for {member.name} during bio screen: {e}", exc_info=True)
            return {"flagged": False}

    if not bio:
        return {"flagged": False}

    triggered_keywords = []
    local_rules = keywords_data.get("per_server_keywords", {}).get(str(member.guild.id), {})
    global_rules = keywords_data.get("global_keywords", {})

    triggered_keywords.extend(
        check_text_for_keywords(
            bio,
            local_rules.get("bio_and_message_keywords", {}),
            regex_source_label="Local",
        )
    )
    triggered_keywords.extend(
        check_text_for_keywords(
            bio,
            global_rules.get("bio_and_message_keywords", {}),
            regex_source_label="Global",
        )
    )

    if triggered_keywords:
        trigger_value = _format_trigger_value(triggered_keywords)
        embed = discord.Embed(
            title="🚨 Flagged User Bio",
            description=f"**User:** {member.mention} (`{member.id}`)",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
        embed.add_field(name="📝 Flagged Bio", value=f"```{bio[:1000]}```", inline=False)
        embed.add_field(name="🚩 Trigger", value=f"`{trigger_value}`", inline=True)
        embed.add_field(name="Status", value="User timed out. Awaiting review...", inline=True)
        embed.add_field(name="Account Age", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
        _add_screening_latency(embed, screening_started_at)
        
        timeout_reason = truncate_audit_reason(f"Flagged user bio. Triggered by: {trigger_value}")
        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}
        
    return {"flagged": False}

# --- SCREENING HELPERS ---
def test_text_against_regex(text_to_check: str, regex_patterns: list[str], regex_source_label: str | None = None) -> list[str]:
    """
    Tests a given string against a list of regex patterns.
    Returns a list of the patterns that matched.
    """
    if not text_to_check or not regex_patterns:
        return []

    triggered_patterns = []

    for index, pattern in enumerate(regex_patterns, start=1):
        try:
            if re.search(pattern, text_to_check):
                if regex_source_label:
                    triggered_patterns.append(f"{regex_source_label} regex #{index}: {pattern}")
                else:
                    triggered_patterns.append(f"Regex #{index}: {pattern}")
        except re.error as e:
            logger.warning(f"Invalid regex pattern encountered during test: '{pattern}' - {e}")
            continue
            
    return triggered_patterns

URL_CANDIDATE = re.compile(
    r"(?i)(?<![\w@.\-])(?:https?://[^\s<>()\[\]{}\"'`]+|"
    r"(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,}"
    r"(?::[0-9]+)?(?:[/?#][^\s<>()\[\]{}\"'`]*)?)"
)


def exclude_trusted_urls(text: str, domain_patterns: list[str]) -> str:
    """Exempt complete trusted URL tokens, never other text or lookalike hosts."""
    if not domain_patterns:
        return text

    def replace(match):
        token = match.group(0)
        url = token.rstrip(".,!?;:)]}")
        if "\\" in url:
            return token
        try:
            parsed = urlsplit(url if "://" in url else "https://" + url)
            if parsed.username is not None or not parsed.hostname:
                return token
            host = parsed.hostname.lower().removeprefix("www.")
            for pattern in domain_patterns:
                try:
                    if re.fullmatch(pattern, host, re.IGNORECASE):
                        return " " + token[len(url):]
                except re.error:
                    logger.warning("Invalid trusted-domain regex: %s", pattern)
        except ValueError:
            pass
        return token

    return URL_CANDIDATE.sub(replace, text)


def check_text_for_keywords(text_to_check: str, ruleset: dict, regex_source_label: str | None = None) -> list[str]:
    """
    Checks a given string against a specific ruleset, correctly handling
    both "smart" (whole word) and "substring" (simple) keyword checks.
    """
    if not text_to_check or not ruleset:
        return []
    
    text_to_check = exclude_trusted_urls(text_to_check, ruleset.get("whitelisted_domains_regex", []))

    triggered = []
    normalized_text = unidecode(text_to_check).lower()

    # --- Substring/Simple Keywords (Aggressive Match) ---
    substring_keywords = ruleset.get("substring", []) + ruleset.get("simple_keywords", [])
    for keyword in substring_keywords:
        if keyword.lower() in normalized_text:
            triggered.append(keyword)

    # --- Smart/Whole Word Keywords (Precise Match) ---
    smart_keywords = ruleset.get("smart", [])
    for keyword in smart_keywords:
        pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
        if re.search(pattern, normalized_text):
            triggered.append(keyword)

    # --- Regex Pattern Check (Against ORIGINAL Text) ---
    regex_patterns = ruleset.get("regex_patterns", [])
    if regex_patterns:
        matched_regex_patterns = test_text_against_regex(
            text_to_check,
            regex_patterns,
            regex_source_label=regex_source_label,
        )
        if matched_regex_patterns:
            triggered.extend(matched_regex_patterns)

    return list(dict.fromkeys(triggered))
            
async def check_server_identity(bot: 'AntiScamBot', member: discord.Member, profile: discord.abc.User | None = None) -> dict:
    def normalize_primary_guild(identity_source):
        if not identity_source:
            return None

        if isinstance(identity_source, dict):
            raw_id = identity_source.get("identity_guild_id") or identity_source.get("id")
            tag = identity_source.get("tag")
            identity_enabled = identity_source.get("identity_enabled")
        else:
            raw_id = getattr(identity_source, "id", None)
            tag = getattr(identity_source, "tag", None)
            identity_enabled = getattr(identity_source, "identity_enabled", None)

        normalized_tag = tag.strip() if isinstance(tag, str) else None
        if normalized_tag == "":
            normalized_tag = None

        try:
            normalized_id = int(raw_id) if raw_id is not None else None
        except (TypeError, ValueError):
            normalized_id = None

        normalized = {
            "guild_id": normalized_id,
            "identity_enabled": identity_enabled,
            "tag": normalized_tag
        }

        if all(value is None for value in normalized.values()):
            return None

        return normalized

    async def fetch_identity_via_profile():
        bot_token = os.getenv("ANTISCAM_BOT_TOKEN")
        if not bot_token:
            return None

        url = f"https://discord.com/api/v9/users/{member.id}/profile"
        headers = {"Authorization": f"Bot {bot_token}"}

        try:
            timeout = aiohttp.ClientTimeout(total=PROFILE_FETCH_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        payload = await response.json()
                        primary_guild = payload.get("user", {}).get("primary_guild")
                        return normalize_primary_guild(primary_guild)
                    if response.status != 404:
                        logger.warning(
                            f"Server identity profile fetch failed for {member.name} ({member.id}) "
                            f"with status {response.status}."
                        )
        except asyncio.TimeoutError:
            logger.warning(
                "Server identity profile fetch timed out for %s (%s) after %s seconds.",
                member.name,
                member.id,
                PROFILE_FETCH_TIMEOUT_SECONDS,
            )
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error while fetching server identity for {member.name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while fetching server identity for {member.name}: {e}", exc_info=True)

        return None

    def evaluate_identity(identity_info):
        if not identity_info:
            return None

        identity_enabled = identity_info.get("identity_enabled")
        tag = identity_info.get("tag")
        guild_id = identity_info.get("guild_id")
        is_public = identity_enabled is not False

        if is_public and guild_id and guild_id in getattr(bot, "scam_server_ids", []):
            return f"User has a server badge from a known scam server (ID: {guild_id})."

        if is_public and tag:
            lowered_tag = tag.lower()
            suspicious_tags = [
                susp.lower() for susp in getattr(bot, "suspicious_identity_tags", []) if isinstance(susp, str)
            ]
            if any(susp_tag in lowered_tag for susp_tag in suspicious_tags):
                return f"User has a suspicious server badge tag: '{tag}'."

        return None

    identity_source = getattr(member, "primary_guild", None)
    if not identity_source and profile is not None:
        identity_source = getattr(profile, "primary_guild", None)

    identity_info = normalize_primary_guild(identity_source)

    if not identity_info:
        identity_info = await fetch_identity_via_profile()

    reason = evaluate_identity(identity_info)

    if reason:
        logger.info(f"FLAGGED {member.name} for malicious server identity: {reason}")
        return {"flagged": True, "reason": reason}

    return {"flagged": False}

async def perform_automated_banned_elsewhere_ban(bot: 'AntiScamBot', alert_message: discord.Message, member: discord.Member, ban_reason_detail: str):
    from ui.views import ScreeningView
    guild = alert_message.guild
    reason = truncate_audit_reason(f"[Automated Action] {ban_reason_detail} | AlertID:{alert_message.id}")

    try:
        delete_days = get_delete_days_for_guild(bot, guild)
        await guild.ban(member, reason=reason, delete_message_seconds=delete_days * 86400)
        logger.info(f"AUTOMATED BAN of {member.name} in {guild.name} (Reason: Banned Elsewhere).")

        embed = alert_message.embeds[0]
        embed.color = discord.Color.red()
        for i, field in enumerate(embed.fields):
            if field.name == "Status":
                embed.set_field_at(i, name="Status", value="🔴 Banned (Automated)", inline=True)
                break
        
        view = ScreeningView(flagged_member_id=member.id)
        view.update_buttons_for_state('banned')
        await alert_message.edit(embed=embed, view=view)

    except Exception as e:
        logger.error(f"Failed to execute automated 'Banned Elsewhere' ban for {member.name}: {e}")

async def delayed_banned_elsewhere_wrapper(delay: int, bot: 'AntiScamBot', alert_message: discord.Message, member: discord.Member, ban_reason_detail: str):
    try:
        await asyncio.sleep(delay)
        
        try:
            await alert_message.channel.fetch_message(alert_message.id)
            if member.guild.get_member(member.id) is None:
                logger.info(f"Automated 'Banned Elsewhere' action for {member.name} cancelled: User no longer in server.")
                return
        except discord.NotFound:
            logger.info(f"Automated 'Banned Elsewhere' action for {member.name} cancelled: Alert was deleted.")
            return

        logger.info(f"Delay complete for {member.name}. Performing automated 'Banned Elsewhere' ban.")
        await perform_automated_banned_elsewhere_ban(bot, alert_message, member, ban_reason_detail)

    except asyncio.CancelledError:
        logger.info(f"Delayed 'Banned Elsewhere' action for {member.name} was cancelled by a moderator.")
    finally:
        if alert_message.id in bot.pending_ai_actions:
            del bot.pending_ai_actions[alert_message.id]

async def run_full_scan(bot: 'AntiScamBot', interaction: discord.Interaction):
    from ui.views import ScreeningView
    config = bot.config
    guild = interaction.guild
    
    results_channel_id = config.get("action_alert_channels", {}).get(str(guild.id))
    results_channel = guild.get_channel(results_channel_id)

    if not results_channel:
        await interaction.followup.send(f"❌ **Scan Aborted:** Scan results channel not configured for {guild.name}.", ephemeral=True)
        if guild.id in bot.active_scans:
            del bot.active_scans[guild.id]
        return
    if not guild.chunked:
        await guild.chunk()

    keywords_data = await data_manager.load_keywords()
    if not keywords_data:
        await interaction.followup.send("❌ **Scan Aborted:** Could not load keywords file. Please check logs.", ephemeral=True)
        if guild.id in bot.active_scans:
            del bot.active_scans[guild.id]
        return
    
    total_members = guild.member_count
    progress_message = None
    checked_count, flagged_count = 0, 0
    update_interval = 50
    
    event_listeners_cog = bot.get_cog("EventListeners")
    gemini_is_available = event_listeners_cog.gemini_is_available if event_listeners_cog else False

    try:
        progress_message = await interaction.channel.send(f"🔍 Scan initiated. Preparing to scan {total_members} members in **{guild.name}**...")
        logger.info(f"Full member scan initiated by {interaction.user.name} for guild '{guild.name}'.")
        for i, member in enumerate(guild.members):
            if asyncio.current_task().cancelled():
                raise asyncio.CancelledError
            checked_count += 1
            if member.bot:
                continue
            whitelisted_roles = config.get("whitelisted_roles_per_guild", {}).get(str(guild.id), [])
            if any(role.id in whitelisted_roles for role in member.roles):
                continue
            
            result = await screen_member(bot, member, keywords_data)
            
            if result.get("flagged"):
                flagged_count += 1
                try:
                    timeout_minutes = get_timeout_minutes_for_guild(bot, member.guild)
                    await member.timeout(timedelta(minutes=timeout_minutes), reason=result.get("timeout_reason", "Flagged by scan."))
                    if result.get("skip_alert_dispatch"):
                        continue
                    view = ScreeningView(flagged_member_id=member.id)
                    embed = result.get("embed")
                    embed.set_footer(text=f"User ID: {member.id}")

                    guild_id_str = str(member.guild.id)
                    llm_defaults = config.get("llm_settings", {}).get("defaults", {})
                    llm_config = config.get("llm_settings", {}).get("per_guild_settings", {}).get(guild_id_str, llm_defaults)

                    if gemini_is_available and llm_config.get("automation_mode", "off") != "off":
                        # AI-powered workflow for the scan
                        profile = member
                        bio = next((field.value for field in embed.fields if "Bio" in field.name), "")
                        identity_context = format_member_identity_context(member, profile)
                        llm_handler.schedule_analysis(
                            bot=bot,
                            alert_channel=results_channel,
                            embed=embed,
                            view=view,
                            flagged_member=member,
                            content_type="Bio/Identity (Scan)",
                            content=f"{identity_context}\nBio: {bio}",
                            trigger=result.get("timeout_reason")
                        )
                    else:
                        # Manual-only workflow
                        allowed_mentions = discord.AllowedMentions(users=[member])
                        await results_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)

                except Exception as e:
                    logger.error(f"Failed to take action on scanned member {member.name}: {e}")
            if checked_count % update_interval == 0:
                progress_text = f"Scan in progress... {checked_count}/{total_members} members checked. **{flagged_count}** flagged so far."
                await progress_message.edit(content=f"🔍 {progress_text}")
                logger.info(f"Scan progress for {guild.name}: {progress_text}")
            if i % 50 == 0:
                await asyncio.sleep(1)
        summary_text = f"Scan Complete for {guild.name}! Scanned {checked_count} members. Flagged {flagged_count} accounts."
        discord_summary = f"✅ **Scan Complete for {guild.name}!**\n- Scanned **{checked_count}** members.\n- Flagged a total of **{flagged_count}** suspicious accounts."
        if progress_message:
            await progress_message.edit(content=discord_summary)
        logger.info(summary_text)
    except asyncio.CancelledError:
        logger.info(f"Scan task for guild {guild.id} was cancelled by command.")
        if progress_message:
            await progress_message.edit(content=f"🟡 **Scan Cancelled!**\n- Scanned **{checked_count}** members in **{guild.name}** before stopping.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during the full scan for {guild.name}: {e}", exc_info=True)
        if progress_message:
            await progress_message.edit(content="❌ **Scan Failed!**\n- An unexpected error occurred. Please check the logs.")
    finally:
        if guild.id in bot.active_scans:
            del bot.active_scans[guild.id]
            logger.info(f"Scan task for guild {guild.id} removed from active tracker.")
