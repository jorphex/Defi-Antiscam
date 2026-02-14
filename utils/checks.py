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
        app_info = await interaction.client.application_info()
        if interaction.user.id == app_info.owner.id:
            return True
        await interaction.response.send_message("❌ This command can only be used by the bot owner.", ephemeral=True)
        return False
    return discord.app_commands.check(predicate)

def has_mod_role():
    """
    A check decorator that passes if the command user has a federated moderator role
    in the current guild, OR if the user is the bot owner.
    """
    async def predicate(interaction: discord.Interaction) -> bool:
        bot: 'AntiScamBot' = interaction.client
        config = bot.config
        
        app_info = await bot.application_info()
        if interaction.user.id == app_info.owner.id:
            return True

        if not interaction.guild: return False

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

    return discord.app_commands.check(predicate)

async def has_federated_mod_role(interaction: discord.Interaction) -> bool:
    """Checks if the user has a whitelisted moderator role for the current guild."""
    bot: 'AntiScamBot' = interaction.client
    config = bot.config
    
    federated_guild_ids = config.get("federated_guild_ids", [])
    if interaction.guild.id not in federated_guild_ids:
        await interaction.response.send_message("❌ This command can only be used in a federated server.", ephemeral=True)
        return False
        
    whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(interaction.guild.id), [])
    user_role_ids = {role.id for role in interaction.user.roles}
    if not any(role_id in whitelisted_mod_roles for role_id in user_role_ids):
        await interaction.response.send_message("❌ You do not have the required role to use this command.", ephemeral=True)
        return False
    return True

async def is_federated_moderator(bot: 'AntiScamBot', user_id_to_check: int) -> bool:
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
            return False
        
        try:
            member = await guild.fetch_member(user_id_to_check)
            if not member: return False
            return any(role.id in all_mod_roles for role in member.roles)
        except discord.NotFound:
            return False
        except Exception as e:
            logger.warning(f"Could not fetch member {user_id_to_check} in guild {guild.name} for is_federated_moderator check: {e}")
            return False

    tasks = [check_guild(guild_id) for guild_id in config.get("federated_guild_ids", [])]
    
    for future in asyncio.as_completed(tasks):
        result = await future
        if result:
            logger.info(f"is_federated_moderator check PASSED for {user_id_to_check}.")
            for task in tasks:
                if not task.done():
                    task.cancel()
            return True
    
    return False
