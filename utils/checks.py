# /antiscam/utils/checks.py

import discord
import asyncio
from typing import TYPE_CHECKING

from config import logger

if TYPE_CHECKING:
    from antiscam import AntiScamBot

# --- PERMISSION CHECKS ---
def is_bot_owner():
    """A check decorator to ensure the user is the bot's owner."""
    async def predicate(interaction: discord.Interaction) -> bool:
        bot: 'AntiScamBot' = interaction.client
        owner_id = bot.config.get("bot_owner_id") or getattr(bot, "owner_id", None)
        if owner_id and interaction.user.id == owner_id:
            return True
        await interaction.response.send_message("❌ This command can only be used by the bot owner.", ephemeral=True)
        return False
    return discord.app_commands.check(predicate)

async def check_moderator(interaction: discord.Interaction) -> bool:
    """Shared authorization for commands and moderation buttons."""
    bot: 'AntiScamBot' = interaction.client
    config = bot.config

    owner_id = config.get("bot_owner_id") or getattr(bot, "owner_id", None)
    if owner_id and interaction.user.id == owner_id:
        return True

    if not interaction.guild:
        return False

    if interaction.guild.id not in config.get("federated_guild_ids", []):
        await interaction.response.send_message("❌ This command can only be used in a federated server.", ephemeral=True)
        return False

    whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(interaction.guild.id), [])
    if not whitelisted_mod_roles:
        await interaction.response.send_message("❌ Moderator roles are not configured for this server.", ephemeral=True)
        return False

    user_role_ids = {role.id for role in interaction.user.roles}
    if any(role_id in whitelisted_mod_roles for role_id in user_role_ids):
        return True

    await interaction.response.send_message("❌ You do not have the required role to use this command.", ephemeral=True)
    return False

def has_mod_role():
    return discord.app_commands.check(check_moderator)

async def is_federated_moderator(bot: 'AntiScamBot', user_id_to_check: int, *, strict: bool = False) -> bool:
    """Checks if a user ID belongs to a moderator in ANY federated server concurrently."""
    config = bot.config
    
    all_mod_roles = {
        role_id
        for role_list in config.get("moderator_roles_per_guild", {}).values()
        for role_id in role_list
    }

    if not all_mod_roles:
        return False

    async def check_guild(guild_id: int):
        guild = bot.get_guild(guild_id)
        if not guild:
            if strict:
                raise RuntimeError(f"Cannot verify moderator status in guild {guild_id}")
            return False
        
        try:
            member = await asyncio.wait_for(guild.fetch_member(user_id_to_check), timeout=10)
            if not member:
                return False
            return any(role.id in all_mod_roles for role in member.roles)
        except discord.NotFound:
            return False
        except Exception as e:
            logger.warning(f"Could not fetch member {user_id_to_check} in guild {guild.name} for is_federated_moderator check: {e}")
            if strict:
                raise
            return False

    tasks = [
        asyncio.create_task(check_guild(guild_id))
        for guild_id in config.get("federated_guild_ids", [])
    ]

    try:
        for future in asyncio.as_completed(tasks):
            if await future:
                logger.info(f"is_federated_moderator check PASSED for {user_id_to_check}.")
                return True
        return False
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def federated_ban_block_reason(bot: 'AntiScamBot', user, moderator) -> str | None:
    """Apply the same protected-target policy at command entry and action execution."""
    if user.id == moderator.id:
        return "You cannot ban yourself."
    if user.id == getattr(getattr(bot, "user", None), "id", None) or getattr(user, "bot", False):
        return "Bot accounts cannot be targeted."
    owner_id = bot.config.get("bot_owner_id") or getattr(bot, "owner_id", None)
    if owner_id and user.id == owner_id:
        return "The bot owner cannot be targeted."
    import data_manager
    if data_manager.is_user_whitelisted(user.id, bot.config):
        return "User is on the global whitelist."
    try:
        if await is_federated_moderator(bot, user.id, strict=True):
            return "Federated moderators cannot be targeted."
    except Exception:
        return "Could not verify protected moderator status. Try again when all servers are accessible."
    return None
