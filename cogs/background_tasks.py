# /antiscam/cogs/background_tasks.py

import discord
from discord.ext import commands, tasks
import os
import aiohttp
import base64
import json
import shutil
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from config import logger
import data_manager

if TYPE_CHECKING:
    from antiscam import AntiScamBot

# --- Configuration for the GitHub Repo ---
GITHUB_TOKEN = os.getenv("GITHUB_PAT")
REPO_OWNER = "imimim-username"  # The owner of the repository
REPO_NAME = "scraped_bios"      # The name of the repository
USER_ID_TO_PING = 369605002613751818
CHANNEL_ID_TO_SEND = 1416358313708486748

class BackgroundTasks(commands.Cog):
    def __init__(self, bot: 'AntiScamBot'):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        # This ensures the task runs once on startup, then begins its loop.
        self.sync_external_bans.start()
        self.refresh_config_cache.start()

    def cog_unload(self):
        self.sync_external_bans.cancel()
        self.refresh_config_cache.cancel()

    @tasks.loop(hours=24)
    async def sync_external_bans(self):
        logger.info("BACKGROUND TASK: Starting external ban list sync...")
        if not GITHUB_TOKEN:
            logger.error("BACKGROUND TASK: GITHUB_PAT not found. Sync cancelled.")
            return

        summary_title = "External Ban List Sync Report"
        summary_description = ""
        summary_color = discord.Color.green()
        new_users_added_count = 0
        total_profiles_scanned = 0
        current_db_count = 0

        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }

        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                # Step 1: Get the list of files (Unchanged)
                api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"
                async with session.get(api_url) as response:
                    if response.status != 200:
                        logger.error(f"BACKGROUND TASK: Failed to list repo contents. Status: {response.status}")
                        return
                    files_data = await response.json()
                
                jsonl_files = [file['path'] for file in files_data if file['name'].endswith('.jsonl')]
                if not jsonl_files:
                    logger.info("BACKGROUND TASK: No .jsonl files found in the repository.")
                    return
                
                logger.info(f"BACKGROUND TASK: Found {len(jsonl_files)} files to process: {jsonl_files}")

                # Use a dict to ensure uniqueness within this specific sync session
                unique_import_users = {}
                bot_owner_id = self.bot.config.get("bot_owner_id", 0)

                # Step 2: Fetch RAW content (Unchanged logic, just updated data structure)
                for file_path in jsonl_files:
                    raw_url = f"https://raw.githubusercontent.com/{REPO_OWNER}/{REPO_NAME}/main/{file_path}"
                    
                    async with session.get(raw_url) as response:
                        if response.status != 200:
                            logger.error(f"BACKGROUND TASK: Failed to fetch raw file content for '{file_path}'. Status: {response.status}")
                            continue
                        content_str = await response.text()
                    
                    for line in content_str.splitlines():
                        line = line.strip()
                        if not line: continue
                        try:
                            profile = json.loads(line)
                            user_data = profile.get("user", {})
                            user_id_raw = user_data.get("id")
                            total_profiles_scanned += 1
                            
                            if not user_id_raw: continue
                            
                            user_id = str(user_id_raw)
                            
                            # Skip if we already processed this ID in this loop
                            if user_id in unique_import_users:
                                continue

                            bio_text = user_data.get("bio", "N/A")

                            # <<< CHANGE: Create a tuple for the SQL insert >>>
                            # Tuple order: (user_id, username, reason, origin_id, origin_name, mod_id, timestamp, bio)
                            unique_import_users[user_id] = (
                                user_id,
                                user_data.get("username", "N/A"),
                                "Added via automated external list sync.",
                                0, # origin_guild_id
                                "External Import (imimim)", # origin_guild_name
                                bot_owner_id, # initiating_moderator_id
                                datetime.now(timezone.utc).isoformat(), # timestamp
                                bio_text # bio_at_import
                            )

                        except json.JSONDecodeError:
                            logger.warning(f"BACKGROUND TASK: Skipping invalid JSON line in '{file_path}'.")

                # Step 3: Bulk Import to DB
                # Get current count before import
                existing_ban_count = await data_manager.db_get_ban_count()

                # Perform the bulk insert. The DB handles deduplication via INSERT OR IGNORE.
                # We convert the dict values to a list of tuples.
                new_users_added_count = await data_manager.db_bulk_import_bans(list(unique_import_users.values()))
                
                # Get new total count
                current_db_count = await data_manager.db_get_ban_count()

                summary_description = (
                    f"**Profiles Scanned:** `{total_profiles_scanned}`\n"
                    f"**Pre-Sync Count:** `{existing_ban_count}`\n"
                    f"**New Users Added:** `{new_users_added_count}`"
                )

                if new_users_added_count > 0:
                    logger.info(f"✅ Merge complete! Database updated with {new_users_added_count} new entries.")
                else:
                    logger.info("No new users to add from external list.")

        except Exception as e:
            logger.error(f"BACKGROUND TASK: An unexpected error occurred during sync: {e}", exc_info=True)
            summary_title = "Sync Failed: Unexpected Error"
            summary_description = f"An error occurred during the sync process.\n\n`{e}`"
            summary_color = discord.Color.red()
        
        finally:
            log_channel_id = self.bot.config.get("log_channel_id")
            if log_channel_id and (log_channel := self.bot.get_channel(log_channel_id)):
                embed = discord.Embed(
                    title=f"🔄 {summary_title}",
                    description=summary_description,
                    color=summary_color,
                    timestamp=datetime.now(timezone.utc)
                )
                if new_users_added_count > 0:
                    embed.add_field(name="New Total Bans", value=f"`{current_db_count}`")
                
                await log_channel.send(embed=embed)

            try:
                channel = self.bot.get_channel(CHANNEL_ID_TO_SEND) or await self.bot.fetch_channel(CHANNEL_ID_TO_SEND)
                if channel:
                    timestamp = f"<t:{int(datetime.now(timezone.utc).timestamp())}:R>"
                    if new_users_added_count > 0:
                        message = (
                            f"<@{USER_ID_TO_PING}>, the external ban list sync is complete. "
                            f"**{new_users_added_count}** new user(s) were added to the master database {timestamp}."
                        )
                    else:
                        message = (
                            f"ℹ️ The external ban list sync just completed {timestamp}. "
                            "No new users were found to add to the master database."
                        )
                    await channel.send(message)
            except Exception as e:
                logger.error(f"Failed to send sync notification to user: {e}")

    @sync_external_bans.before_loop
    async def before_sync(self):
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=60)
    async def refresh_config_cache(self):
        """Periodically refresh config/keywords to pick up file edits without hot-path I/O."""
        try:
            before_state = data_manager.get_cache_state()
            new_config = data_manager.load_federation_config()
            if new_config:
                self.bot.config = new_config
            keywords_data = await data_manager.load_keywords()
            if keywords_data:
                self.bot.suspicious_identity_tags = keywords_data.get("global_keywords", {}).get("suspicious_identity_tags", [])
            self.bot.scam_server_ids = data_manager.load_scam_servers()
            after_state = data_manager.get_cache_state()
            if before_state["config"]["mtime"] != after_state["config"]["mtime"]:
                self.bot.last_config_reload_at = datetime.now(timezone.utc)
            if before_state["keywords"]["mtime"] != after_state["keywords"]["mtime"]:
                self.bot.last_keywords_reload_at = datetime.now(timezone.utc)
        except Exception as e:
            logger.error(f"CONFIG REFRESH: Failed to reload config/keywords: {e}", exc_info=True)

    @refresh_config_cache.before_loop
    async def before_refresh(self):
        await self.bot.wait_until_ready()

async def setup(bot: 'AntiScamBot'):
    await bot.add_cog(BackgroundTasks(bot))
