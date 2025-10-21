import discord
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING

import data_manager
from utils.checks import is_bot_owner, is_federated_moderator
from utils.command_helpers import (
    add_global_keyword_to_list, remove_global_keyword_from_list,
    add_regex_to_list, remove_regex_from_list_by_id
)
from config import logger
from ui.views import AnnouncementModal


import asyncio
from screening_handler import get_delete_days_for_guild 

if TYPE_CHECKING:
    from antiscam import AntiScamBot

# --- OWNER SLASH COMMANDS ---
class OwnerCommands(commands.Cog):
    def __init__(self, bot: 'AntiScamBot'):
        self.bot = bot

    @app_commands.command(name="zreloadconfig", description="[Owner Only] Reloads configuration files from disk.")
    @is_bot_owner()
    async def reloadconfig(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"OWNER COMMAND: {interaction.user.name} triggered a configuration reload.")
    
        self.bot.config = data_manager.load_federation_config()
        keywords_data = await data_manager.load_keywords()
        self.bot.scam_server_ids = data_manager.load_scam_servers()
        self.bot.system_prompt = data_manager.load_system_prompt()

        if keywords_data:
            self.bot.suspicious_identity_tags = keywords_data.get("global_keywords", {}).get("suspicious_identity_tags", [])

        if not self.bot.config or keywords_data is None:
            await interaction.followup.send("❌ **Failed to reload.** Check logs for errors with config or keyword files.")
            return

        federated_guild_ids = self.bot.config.get("federated_guild_ids", [])
        server_keywords = keywords_data.get("per_server_keywords", {})
        updated = False

        for guild_id in federated_guild_ids:
            guild_id_str = str(guild_id)
            if guild_id_str not in server_keywords:
                logger.info(f"New server ID {guild_id_str} found in config. Populating keywords.json...")
                server_keywords[guild_id_str] = {
                    "username_keywords": {"substring": [], "smart": []},
                    "bio_and_message_keywords": {"simple_keywords": [], "regex_patterns": []}
                }
                updated = True
    
        if updated:
            await data_manager.save_keywords(keywords_data)

        await interaction.followup.send(
            f"✅ **Config, keywords, and threat lists reloaded successfully.**\n"
            f"Now managing **{len(self.bot.config.get('federated_guild_ids', []))}** federated servers.\n"
            f"Loaded **{len(self.bot.scam_server_ids)}** known scam server IDs.\n"
            f"Loaded **{len(self.bot.suspicious_identity_tags)}** suspicious identity tags."
        )

    @app_commands.command(name="zadd-global-name-substring", description="[Owner Only] Adds a SUBSTRING keyword to the GLOBAL list.")
    @app_commands.describe(keyword="The keyword to add globally (e.g., 'admin').")
    @is_bot_owner()
    async def add_global_username_substring(self, interaction: discord.Interaction, keyword: str):
        await interaction.response.defer(ephemeral=True)
        await add_global_keyword_to_list(interaction, keyword, "username_keywords", "substring")

    @app_commands.command(name="zadd-global-name-smart", description="[Owner Only] Adds a SMART keyword to the GLOBAL list.")
    @app_commands.describe(keyword="The keyword to add globally (e.g., 'mod').")
    @is_bot_owner()
    async def add_global_username_smart(self, interaction: discord.Interaction, keyword: str):
        await interaction.response.defer(ephemeral=True)
        await add_global_keyword_to_list(interaction, keyword, "username_keywords", "smart")

    @app_commands.command(name="zadd-global-bio-msg-keyword", description="[Owner Only] Adds a BIO keyword to the GLOBAL list.")
    @app_commands.describe(keyword="The keyword or phrase to add globally.")
    @is_bot_owner()
    async def add_global_bio_keyword(self, interaction: discord.Interaction, keyword: str):
        await interaction.response.defer(ephemeral=True)
        await add_global_keyword_to_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

    @app_commands.command(name="zadd-global-regex", description="[OWNER ONLY] Adds a regex pattern to the GLOBAL list.")
    @app_commands.describe(pattern="The exact regex pattern to add globally.")
    @is_bot_owner()
    async def add_global_regex(self, interaction: discord.Interaction, pattern: str):
        await interaction.response.defer(ephemeral=True)
        await add_regex_to_list(interaction, pattern, is_global=True)

    @app_commands.command(name="zrm-global-name-substring", description="[Owner Only] Removes a SUBSTRING keyword from the GLOBAL list.")
    @app_commands.describe(keyword="The exact keyword to remove globally.")
    @is_bot_owner()
    async def remove_global_username_substring(self, interaction: discord.Interaction, keyword: str):
        await interaction.response.defer(ephemeral=True)
        await remove_global_keyword_from_list(interaction, keyword, "username_keywords", "substring")

    @app_commands.command(name="zrm-global-name-smart", description="[Owner Only] Removes a SMART keyword from the GLOBAL list.")
    @app_commands.describe(keyword="The exact keyword to remove globally.")
    @is_bot_owner()
    async def remove_global_username_smart(self, interaction: discord.Interaction, keyword: str):
        await interaction.response.defer(ephemeral=True)
        await remove_global_keyword_from_list(interaction, keyword, "username_keywords", "smart")

    @app_commands.command(name="zrm-global-bio-msg-keyword", description="[Owner Only] Removes a BIO keyword from the GLOBAL list.")
    @app_commands.describe(keyword="The exact keyword to remove globally.")
    @is_bot_owner()
    async def remove_global_bio_keyword(self, interaction: discord.Interaction, keyword: str):
        await interaction.response.defer(ephemeral=True)
        await remove_global_keyword_from_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

    @app_commands.command(name="zrm-global-regex-by-id", description="[OWNER ONLY] Removes a regex from the GLOBAL list by its ID.")
    @app_commands.describe(index="The numerical ID of the global regex pattern to remove.")
    @is_bot_owner()
    async def remove_global_regex_by_id(self, interaction: discord.Interaction, index: int):
        await interaction.response.defer(ephemeral=True)
        await remove_regex_from_list_by_id(interaction, index, is_global=True)

    @app_commands.command(name="admin-backfill-banlist", description="[OWNER ONLY] Populates the master ban list from historical audit logs.")
    @is_bot_owner()
    async def admin_backfill_banlist(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        progress_message = await interaction.channel.send("🔍 **Phase 1/4: Collecting historical ban data...**")
        await interaction.followup.send("✅ **Starting historical backfill.** This is a complex operation and may take several minutes. Progress is being updated in the channel above.", ephemeral=True)
    
        config = self.bot.config
        bot_owner_id = config.get("bot_owner_id")
        potential_bans = {}
        unbanned_users = set()

        ninety_days_ago = datetime.now(timezone.utc) - timedelta(days=90)
    
        for guild_id in config.get("federated_guild_ids", []):
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logger.warning(f"Backfill: Could not find guild {guild_id}, skipping.")
                continue

            await progress_message.edit(content=f"⏳ **Phase 1/4:** Processing ban logs for **{guild.name}**...")
            try:
                async for entry in guild.audit_logs(action=discord.AuditLogAction.ban, after=ninety_days_ago, limit=None):
                    moderator = entry.user
                    target_user = entry.target

                    if target_user.id == self.bot.user.id or (bot_owner_id and target_user.id == bot_owner_id):
                        continue

                    is_authorized = False
                    if moderator.id == self.bot.user.id:
                        is_authorized = True
                    elif not moderator.bot and isinstance(moderator, discord.Member):
                        whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(guild.id), [])
                        if any(role.id in whitelisted_mod_roles for role in moderator.roles):
                            is_authorized = True
                
                    if is_authorized:
                        if target_user.id not in potential_bans or entry.created_at > datetime.fromisoformat(potential_bans[target_user.id]["ban_timestamp_utc"]):
                            potential_bans[target_user.id] = {
                                "username_at_ban": target_user.name,
                                "ban_timestamp_utc": entry.created_at.isoformat(),
                                "origin_guild_id": guild.id,
                                "origin_guild_name": guild.name,
                                "reason": entry.reason or "No reason provided.",
                                "initiating_moderator_id": moderator.id
                            }
            
                await progress_message.edit(content=f"⏳ **Phase 2/4:** Processing unban logs for **{guild.name}**...")
                async for entry in guild.audit_logs(action=discord.AuditLogAction.unban, after=ninety_days_ago, limit=None):
                    moderator = entry.user
                    is_authorized = False
                    if moderator.id == self.bot.user.id:
                        is_authorized = True
                    elif not moderator.bot and isinstance(moderator, discord.Member):
                        whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(guild.id), [])
                        if any(role.id in whitelisted_mod_roles for role in moderator.roles):
                            is_authorized = True
                
                    if is_authorized:
                        unbanned_users.add(entry.target.id)

            except discord.Forbidden:
                logger.error(f"Backfill: Missing 'View Audit Log' permission in {guild.name}. Skipping.")
                await interaction.followup.send(f"❌ Missing 'View Audit Log' permission in **{guild.name}**. That server could not be processed.", ephemeral=True)
                continue
            except Exception as e:
                logger.error(f"Backfill: An unexpected error occurred while processing {guild.name}: {e}", exc_info=True)
                await interaction.followup.send(f"❌ An error occurred while processing **{guild.name}**. Check logs.", ephemeral=True)
                continue

        await progress_message.edit(content=f"⚙️ **Phase 3/4:** Reconciling `{len(potential_bans)}` bans against `{len(unbanned_users)}` unbans...")
        reconciled_bans = {
            user_id: ban_data
            for user_id, ban_data in potential_bans.items()
            if user_id not in unbanned_users
        }

        await progress_message.edit(content=f"🛡️ **Phase 4/4:** Performing final sanity checks on `{len(reconciled_bans)}` reconciled bans...")
    
        all_whitelisted_roles = {
            role_id
            for role_list in config.get("whitelisted_roles_per_guild", {}).values()
            for role_id in role_list
        }

        fed_bans = await data_manager.load_fed_bans()
        initial_ban_count = len(fed_bans)
        newly_added_count = 0

        for user_id, ban_data in reconciled_bans.items():
            if str(user_id) in fed_bans:
                continue

            # Is the user currently a federated moderator?
            if await is_federated_moderator(user_id):
                logger.info(f"Backfill: Ignoring ban for {user_id} as they are now a federated moderator.")
                continue

            is_untouchable = False
            for guild_id in config.get("federated_guild_ids", []):
                guild = self.bot.get_guild(guild_id)
                if not guild: continue
            
                try:
                    member = guild.get_member(user_id) or await guild.fetch_member(user_id)
                
                    # Do they have a whitelisted role?
                    if any(role.id in all_whitelisted_roles for role in member.roles):
                        logger.info(f"Backfill: Ignoring ban for {user_id} as they have a whitelisted role in {guild.name}.")
                        is_untouchable = True
                        break

                    # Do they have a role higher than the bot?
                    if member.top_role.position >= guild.me.top_role.position:
                        logger.info(f"Backfill: Ignoring ban for {user_id} as they have a superior role in {guild.name}.")
                        is_untouchable = True
                        break
                except discord.NotFound:
                    continue # User not in this guild, so no roles to check.
        
            if is_untouchable:
                continue

            fed_bans[str(user_id)] = ban_data
            newly_added_count += 1

        await data_manager.save_fed_bans(fed_bans)
        final_ban_count = len(fed_bans)
        newly_added_count = final_ban_count - initial_ban_count

        completion_embed = discord.Embed(
            title="✅ Historical Backfill Complete",
            description="The master federated ban list has been populated with historical data.",
            color=discord.Color.green()
        )
        completion_embed.add_field(name="Initial Bans", value=f"`{initial_ban_count}`", inline=True)
        completion_embed.add_field(name="Newly Added", value=f"`{newly_added_count}`", inline=True)
        completion_embed.add_field(name="Total Bans", value=f"`{final_ban_count}`", inline=True)
        completion_embed.set_footer(text="This command can now be removed from the code.")

        await progress_message.edit(content=None, embed=completion_embed)
        logger.info(f"Historical backfill complete. Added {newly_added_count} new bans to the master list.")

    @app_commands.command(name="zsync", description="[OWNER ONLY] Syncs the command tree with Discord.")
    @is_bot_owner()
    async def zsync(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        synced = await self.bot.tree.sync()
        await interaction.followup.send(f"✅ Synced {len(synced)} command(s) globally.")
        logger.info(f"Command tree synced by {interaction.user.name}. Synced {len(synced)} commands.")




    @app_commands.command(name="zsync-origin-bans", description="[OWNER ONLY] Fixes bans missing from their origin server due to a past bug.")
    @is_bot_owner()
    async def zsync_origin_bans(self, interaction: discord.Interaction):
        """
        Scans the master ban list and applies any bans that are missing from their origin server.
        """
        await interaction.response.defer(ephemeral=True)

        fed_bans = await data_manager.load_fed_bans()
        if not fed_bans:
            await interaction.followup.send("The federated ban list is empty. No action needed.", ephemeral=True)
            return

        # --- Simple Confirmation View ---
        class ConfirmSyncView(discord.ui.View):
            def __init__(self, author: discord.User):
                super().__init__(timeout=60.0)
                self.author = author
                self.value = None
            async def interaction_check(self, interaction: discord.Interaction) -> bool:
                return interaction.user.id == self.author.id
            @discord.ui.button(label="Confirm Sync", style=discord.ButtonStyle.danger)
            async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
                self.value = True
                self.stop()
                for item in self.children: item.disabled = True
                await interaction.response.edit_message(content="✅ **Confirmation received. Starting sync...**", view=self)
            @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
            async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
                self.value = False
                self.stop()
                for item in self.children: item.disabled = True
                await interaction.response.edit_message(content="Sync cancelled.", view=self)

        view = ConfirmSyncView(author=interaction.user)
        await interaction.followup.send(
            f"⚠️ **Are you sure?**\nThis will check all **{len(fed_bans)}** users in `bans.json`.\n"
            "If a user is on the list but not banned in their origin server, this script will ban them there.",
            view=view, ephemeral=True
        )
        await view.wait()

        if view.value is not True:
            return

        progress_message = await interaction.channel.send("🔍 **Sync starting...** Checking ban list against origin servers.")
        
        checked_count = 0
        missing_bans_applied = 0
        errors = 0
        update_interval = 25

        for user_id_str, ban_data in fed_bans.items():
            checked_count += 1
            origin_guild_id = ban_data.get("origin_guild_id")
            if not origin_guild_id:
                continue

            origin_guild = self.bot.get_guild(origin_guild_id)
            if not origin_guild:
                logger.warning(f"Sync: Could not find origin guild {origin_guild_id} for user {user_id_str}. Skipping.")
                continue

            user_obj = discord.Object(id=int(user_id_str))

            try:
                await origin_guild.fetch_ban(user_obj)
                # User is correctly banned, do nothing.
            except discord.NotFound:
                # BAN IS MISSING! APPLY IT.
                logger.warning(f"Sync: Found missing ban for user {user_id_str} in origin guild {origin_guild.name}. Applying now.")
                try:
                    reason = f"[SYNC ACTION] Applying missing ban from original command. Original reason: {ban_data.get('reason', 'N/A')}"
                    delete_seconds = get_delete_days_for_guild(self.bot, origin_guild) * 86400
                    await origin_guild.ban(user_obj, reason=reason[:512], delete_message_seconds=delete_seconds)
                    missing_bans_applied += 1
                except Exception as e:
                    logger.error(f"Sync: FAILED to apply missing ban for {user_id_str} in {origin_guild.name}: {e}")
                    errors += 1
            except Exception as e:
                logger.error(f"Sync: An unexpected error occurred checking ban for {user_id_str} in {origin_guild.name}: {e}")
                errors += 1

            if checked_count % update_interval == 0:
                await progress_message.edit(content=f"🔍 Sync in progress... {checked_count}/{len(fed_bans)} checked. Applied {missing_bans_applied} missing bans.")
            
            await asyncio.sleep(0.01) # Be nice to the API

        summary_embed = discord.Embed(
            title="✅ Origin Ban Sync Complete",
            color=discord.Color.green() if errors == 0 else discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )
        summary_embed.add_field(name="Total Records Checked", value=f"`{checked_count}`", inline=False)
        summary_embed.add_field(name="Missing Bans Applied", value=f"`{missing_bans_applied}`", inline=True)
        summary_embed.add_field(name="Errors Encountered", value=f"`{errors}`", inline=True)
        summary_embed.set_footer(text="Check console logs for details on any errors.")

        await progress_message.edit(content=None, embed=summary_embed)

    @app_commands.command(name="zannounce", description="[OWNER ONLY] Sends an announcement to all federated servers.")
    @is_bot_owner()
    async def zannounce(self, interaction: discord.Interaction):
        """Pops up a modal to send a system-wide announcement."""
        modal = AnnouncementModal(self.bot)
        await interaction.response.send_modal(modal)

async def setup(bot: 'AntiScamBot'):
    await bot.add_cog(OwnerCommands(bot))
