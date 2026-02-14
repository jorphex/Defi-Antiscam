# /antiscam/utils/helpers.py

import discord
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from antiscam import AntiScamBot

def get_timeout_minutes_for_guild(bot: 'AntiScamBot', guild: discord.Guild) -> int:
    """Gets the configured timeout duration in minutes for a specific guild."""
    config = bot.config
    per_guild_settings = config.get("timeout_duration_minutes_per_guild", {})
    guild_id_str = str(guild.id)
    if guild_id_str in per_guild_settings:
        return per_guild_settings[guild_id_str]
    
    return config.get("timeout_duration_minutes_default", 10)

def get_delete_days_for_guild(bot: 'AntiScamBot', guild: discord.Guild) -> int:
    """Gets the configured message deletion days for a specific guild."""
    config = bot.config
    per_guild_settings = config.get("delete_messages_on_ban_days_per_guild", {})
    guild_id_str = str(guild.id)
    if guild_id_str in per_guild_settings:
        return per_guild_settings[guild_id_str]
    
    return config.get("delete_messages_on_ban_days_default", 1)