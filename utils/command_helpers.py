# /antiscam/utils/command_helpers.py

import discord
import re
from typing import TYPE_CHECKING

import data_manager
from config import logger

if TYPE_CHECKING:
    from antiscam import AntiScamBot

# --- COMMAND HELPERS ---
async def add_global_keyword_to_list(interaction: discord.Interaction, keyword: str, primary_key: str, secondary_key: str = None):
    keyword = keyword.lower().strip()
    if not keyword:
        await interaction.followup.send("❌ Keyword cannot be empty.")
        return

    keywords_data = await data_manager.load_keywords()
    if keywords_data is None:
        await interaction.followup.send("❌ Could not load the keywords file. Please check the bot's logs.")
        return

    global_keywords = keywords_data.setdefault("global_keywords", {})
    
    target_list = None
    if secondary_key:
        primary_dict = global_keywords.setdefault(primary_key, {})
        target_list = primary_dict.setdefault(secondary_key, [])
    else:
        target_list = global_keywords.setdefault(primary_key, [])

    if keyword in target_list:
        await interaction.followup.send(f"⚠️ The keyword '{keyword}' is already in the global list.")
        return

    target_list.append(keyword)
    await data_manager.save_keywords(keywords_data)

    logger.info(f"OWNER {interaction.user.name} added global keyword '{keyword}'.")
    await interaction.followup.send(f"✅ Keyword '{keyword}' has been successfully added to the GLOBAL list.")

async def remove_global_keyword_from_list(interaction: discord.Interaction, keyword: str, primary_key: str, secondary_key: str = None):
    keyword = keyword.lower().strip()
    if not keyword:
        await interaction.followup.send("❌ Keyword cannot be empty.")
        return

    keywords_data = await data_manager.load_keywords()
    global_keywords = keywords_data.get("global_keywords", {})
    
    target_list = None
    if secondary_key:
        target_list = global_keywords.get(primary_key, {}).get(secondary_key, [])
    else:
        target_list = global_keywords.get(primary_key, [])

    if keyword in target_list:
        target_list.remove(keyword)
        await data_manager.save_keywords(keywords_data)
        logger.info(f"OWNER {interaction.user.name} removed global keyword '{keyword}'.")
        await interaction.followup.send(f"✅ Keyword '{keyword}' has been removed from the GLOBAL list.")
    else:
        await interaction.followup.send(f"❌ Keyword '{keyword}' was not found in the GLOBAL list.")

def format_keyword_list(ruleset: dict, list_title: str) -> str:
    output = f"**{list_title}**\n"
    has_keywords = False
    
    substring_keywords = ruleset.get("username_keywords", {}).get("substring", [])
    if substring_keywords:
        has_keywords = True
        output += f"__Username (Substring Match):__\n`{', '.join(substring_keywords)}`\n"
        
    smart_keywords = ruleset.get("username_keywords", {}).get("smart", [])
    if smart_keywords:
        has_keywords = True
        output += f"__Username (Smart Match):__\n`{', '.join(smart_keywords)}`\n"
        
    bio_keywords = ruleset.get("bio_and_message_keywords", {}).get("simple_keywords", [])
    if bio_keywords:
        has_keywords = True
        output += f"__Bio & Message (Smart Match):__\n`{', '.join(bio_keywords)}`\n"
        
    regex_patterns = ruleset.get("bio_and_message_keywords", {}).get("regex_patterns", [])
    if regex_patterns:
        has_keywords = True
        bulleted_patterns = '\n'.join([f"**`{i}`**. `{pattern}`" for i, pattern in enumerate(regex_patterns, 1)])
        output += f"__Bio & Message (Regex):__\n{bulleted_patterns}\n"

    if not has_keywords:
        output += "*No keywords configured in this category.*\n"
        
    return output + "\n"

async def add_keyword_to_list(interaction: discord.Interaction, keyword: str, primary_key: str, secondary_key: str = None):
    keyword = keyword.lower().strip()
    if not keyword:
        await interaction.followup.send("❌ Keyword cannot be empty.")
        return

    keywords_data = await data_manager.load_keywords()
    guild_id_str = str(interaction.guild.id)

    server_keywords_parent = keywords_data.setdefault("per_server_keywords", {})
    server_keywords = server_keywords_parent.setdefault(guild_id_str, {
        "username_keywords": {"substring": [], "smart": []},
        "bio_and_message_keywords": {"simple_keywords": [], "regex_patterns": []}
    })
    
    target_list = None
    if secondary_key:
        primary_dict = server_keywords.setdefault(primary_key, {})
        target_list = primary_dict.setdefault(secondary_key, [])
    else:
        target_list = server_keywords.setdefault(primary_key, [])

    if keyword in target_list:
        await interaction.followup.send(f"⚠️ The keyword '{keyword}' is already in this server's local list.")
        return

    target_list.append(keyword)
    await data_manager.save_keywords(keywords_data)

    logger.info(f"Moderator {interaction.user.name} in {interaction.guild.name} added keyword '{keyword}'.")
    await interaction.followup.send(f"✅ Keyword '{keyword}' has been added to this server's local list.")

async def add_regex_to_list(interaction: discord.Interaction, pattern: str, is_global: bool):
    try:
        re.compile(pattern)
    except re.error as e:
        logger.warning(f"Moderator {interaction.user.name} tried to add an invalid regex: {pattern}. Error: {e}")
        await interaction.followup.send(f"❌ **Invalid Regex:** That pattern is not valid.\n`{e}`\nPlease test your pattern with `/test-regex` first.")
        return

    keywords_data = await data_manager.load_keywords()
    target_list = None
    list_name = ""

    if is_global:
        global_keywords = keywords_data.setdefault("global_keywords", {})
        bio_keywords = global_keywords.setdefault("bio_and_message_keywords", {})
        target_list = bio_keywords.setdefault("regex_patterns", [])
        list_name = "GLOBAL"
    else:
        guild_id_str = str(interaction.guild.id)
        server_keywords_parent = keywords_data.setdefault("per_server_keywords", {})
        server_keywords = server_keywords_parent.setdefault(guild_id_str, {})
        bio_keywords = server_keywords.setdefault("bio_and_message_keywords", {})
        target_list = bio_keywords.setdefault("regex_patterns", [])
        list_name = "local"

    if pattern in target_list:
        await interaction.followup.send(f"⚠️ That regex pattern is already in the {list_name} list.")
        return

    target_list.append(pattern)
    await data_manager.save_keywords(keywords_data)
    logger.info(f"User {interaction.user.name} added {list_name} regex: '{pattern}'")
    await interaction.followup.send(f"✅ Regex pattern has been successfully added to the **{list_name}** list.")

async def remove_keyword_from_list(interaction: discord.Interaction, keyword: str, primary_key: str, secondary_key: str = None):
    keyword = keyword.lower().strip()
    if not keyword:
        await interaction.followup.send("❌ Keyword cannot be empty.")
        return

    keywords_data = await data_manager.load_keywords()
    guild_id_str = str(interaction.guild.id)
    
    server_keywords = keywords_data.get("per_server_keywords", {}).get(guild_id_str, {})
    
    target_list = None
    if secondary_key:
        target_list = server_keywords.get(primary_key, {}).get(secondary_key, [])
    else:
        target_list = server_keywords.get(primary_key, [])

    if keyword in target_list:
        target_list.remove(keyword)
        await data_manager.save_keywords(keywords_data)
        logger.info(f"Moderator {interaction.user.name} in {interaction.guild.name} removed keyword '{keyword}'.")
        await interaction.followup.send(f"✅ Keyword '{keyword}' has been removed from this server's local list.")
    else:
        await interaction.followup.send(f"❌ Keyword '{keyword}' was not found in this server's local list.")

async def remove_regex_from_list_by_id(interaction: discord.Interaction, index: int, is_global: bool):
    if index <= 0:
        await interaction.followup.send("❌ Index must be a positive number. Use `/list-keywords` to find the correct ID.")
        return

    keywords_data = await data_manager.load_keywords()
    target_list = None
    list_name = ""
    
    if is_global:
        target_list = keywords_data.get("global_keywords", {}).get("bio_and_message_keywords", {}).get("regex_patterns", [])
        list_name = "GLOBAL"
    else:
        guild_id_str = str(interaction.guild.id)
        target_list = keywords_data.get("per_server_keywords", {}).get(guild_id_str, {}).get("bio_and_message_keywords", {}).get("regex_patterns", [])
        list_name = "local"

    real_index = index - 1

    if target_list and 0 <= real_index < len(target_list):
        removed_pattern = target_list.pop(real_index)
        await data_manager.save_keywords(keywords_data)
        logger.info(f"User {interaction.user.name} removed {list_name} regex by ID #{index}: '{removed_pattern}'")
        await interaction.followup.send(f"✅ Regex pattern **`{index}`** has been removed from the **{list_name}** list.\n> `{removed_pattern}`")
    else:
        await interaction.followup.send(f"❌ Invalid ID **`{index}`**. There is no regex pattern with that ID in the {list_name} list. Use `/list-keywords` to see available IDs.")

async def update_onboard_command_visibility(bot: 'AntiScamBot', guild: discord.Guild):
    try:
        onboard_command = bot.tree.get_command("onboard-server")
        if not onboard_command:
            logger.warning("Could not find the 'onboard-server' command to update its permissions.")
            return

        sync_status = await data_manager.load_sync_status()
        
        if guild.id in sync_status["synced_guild_ids"]:
            owner_id = bot.config.get("bot_owner_id")
            if not owner_id:
                logger.warning(f"Cannot hide /onboard-server in {guild.name} because bot_owner_id is not set.")
                return
            permissions = {discord.Object(id=owner_id): True}
            await bot.tree.edit_command_permissions(guild=guild, command=onboard_command, permissions=permissions)
            logger.info(f"Hid '/onboard-server' for non-owners in synced guild {guild.name}.")
        else:
            mod_role_ids = bot.config.get("moderator_roles_per_guild", {}).get(str(guild.id), [])
            owner_id = bot.config.get("bot_owner_id")
            permissions = {}
            for role_id in mod_role_ids:
                permissions[discord.Object(id=role_id)] = True
            if owner_id:
                permissions[discord.Object(id=owner_id)] = True
            
            if not permissions:
                logger.warning(f"No moderator roles configured for {guild.name}, /onboard-server will be hidden.")

            await bot.tree.edit_command_permissions(guild=guild, command=onboard_command, permissions=permissions)
            logger.info(f"Set visibility for '/onboard-server' for moderators in unsynced guild {guild.name}.")
    except Exception as e:
        logger.error(f"Failed to update visibility for /onboard-server in {guild.name}: {e}", exc_info=True)