# /antiscam/cogs/mod_commands.py

import discord
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timezone
import re
from typing import TYPE_CHECKING

import data_manager
import screening_handler
from ui.views import ConfirmScanView, RegexTestModal, OnboardView, ConfirmGlobalBanView, AlreadyBannedView, ConfirmGlobalUnbanView, LookupPaginatorView, TestCurrentRegexModal, ConfirmMassBanView, ConfirmMassKickView
from utils.checks import has_mod_role, has_federated_mod_role, is_federated_moderator
from utils.command_helpers import (
    format_keyword_list, add_keyword_to_list, add_regex_to_list,
    remove_keyword_from_list, remove_regex_from_list_by_id
)
from config import logger

if TYPE_CHECKING:
    from antiscam import AntiScamBot

# --- MODERATOR SLASH COMMANDS ---
class ModCommands(commands.Cog):
    def __init__(self, bot: 'AntiScamBot'):
        self.bot = bot

    @app_commands.command(name="stats", description="Displays local and federated ban statistics.")
    @has_mod_role()
    async def stats(self, interaction: discord.Interaction):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer()
        stats = await data_manager.load_fed_stats()
        guild_id_str = str(interaction.guild.id)
        current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")
        guild_stats = stats.get(guild_id_str, {})
        bans_initiated_monthly = guild_stats.get("monthly_initiated", {}).get(current_month_key, 0)
        bans_initiated_lifetime = guild_stats.get("bans_initiated_lifetime", 0)
        bans_received_monthly = guild_stats.get("monthly_received", {}).get(current_month_key, 0)
        bans_received_lifetime = guild_stats.get("bans_received_lifetime", 0)
        total_federated_actions_lifetime = stats.get("global", {}).get("total_federated_actions_lifetime", 0)
        embed = discord.Embed(
            title="🛡️ Ban Statistics",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url if interaction.guild.icon else None)
        embed.add_field(name="Bans Initiated from This Server", value=f"**`{bans_initiated_monthly}`** (This Month)\n**`{bans_initiated_lifetime}`** (All Time)", inline=False)
        embed.add_field(name="Bans Received in This Server", value=f"**`{bans_received_monthly}`** (This Month)\n**`{bans_received_lifetime}`** (All Time)", inline=False)
        embed.add_field(name="Total Federated Bans (All Time)", value=f"**`{total_federated_actions_lifetime}`**\n*Total federated bans across all servers.*", inline=False)
        await interaction.followup.send(embed=embed)
        
    @app_commands.command(name="list-keywords", description="Lists all active screening keywords for this server.")
    @has_mod_role()
    async def list_keywords(self, interaction: discord.Interaction):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        keywords_data = await data_manager.load_keywords()
        guild_id_str = str(interaction.guild.id)

        local_rules = keywords_data.get("per_server_keywords", {}).get(guild_id_str, {})
        local_output = format_keyword_list(local_rules, "🔑 Local Keywords for this Server")

        global_rules = keywords_data.get("global_keywords", {})
        global_output = format_keyword_list(global_rules, "🌍 Global Keywords (Applied to all Servers)")

        embed = discord.Embed(
            title=f"Screening Keywords for {interaction.guild.name}",
            description=local_output + global_output,
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="add-name-keyword-substring", description="Aggressive name keyword (catches variations: 'admin' in 'daoadmin' 'admin123').")
    @has_mod_role()
    @discord.app_commands.describe(keyword="Example: 'admin' will match 'listadaoadmin' or 'admin123'.")
    async def add_username_keyword_substring(self, interaction: discord.Interaction, keyword: str):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await add_keyword_to_list(interaction, keyword, "username_keywords", "substring")

    @app_commands.command(name="add-name-keyword-smart", description="Precise name keyword (avoids false positives: 'mod' in 'mod123' not 'modern').")
    @has_mod_role()
    @discord.app_commands.describe(keyword="Example: 'mod' will match 'mod123' but IGNORE 'modern'.")
    async def add_username_keyword_smart(self, interaction: discord.Interaction, keyword: str):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await add_keyword_to_list(interaction, keyword, "username_keywords", "smart")

    @app_commands.command(name="add-bio-msg-keyword", description="Keyword for screening user bios and messages.")
    @has_mod_role()
    @discord.app_commands.describe(keyword="The keyword or phrase to add (e.g., 'dm me for help').")
    async def add_bio_keyword(self, interaction: discord.Interaction, keyword: str):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await add_keyword_to_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

    @app_commands.command(name="test-new-regex", description="Tests a new regex pattern against sample text using a pop-up form.")
    @has_mod_role()
    @app_commands.describe(pattern="The regex pattern to test. Remember to escape special characters (e.g., '\\.').")
    async def test_new_regex(self, interaction: discord.Interaction, pattern: str):
        if not await has_federated_mod_role(interaction):
            return

        try:
            compiled_regex = re.compile(pattern)
        except re.error as e:
            await interaction.response.send_message(f"❌ **Invalid Regex:** That pattern is not valid.\n`{e}`", ephemeral=True)
            return

        modal = RegexTestModal(pattern=pattern, compiled_regex=compiled_regex)
        await interaction.response.send_modal(modal)

    @app_commands.command(name="test-current-regex", description="Tests sample text against all current regex patterns using a pop-up box.")
    @has_mod_role()
    async def test_current_regex(self, interaction: discord.Interaction):
        if not await has_federated_mod_role(interaction):
            return
        
        modal = TestCurrentRegexModal()
        await interaction.response.send_modal(modal)

    @app_commands.command(name="add-regex", description="Adds a regex pattern to this server's local list. Try /test-regex first!")
    @has_mod_role()
    @discord.app_commands.describe(pattern="The exact regex pattern. Use standard regex escaping (e.g., '\\.' for a dot, '\\s' for whitespace).")
    async def add_local_regex(self, interaction: discord.Interaction, pattern: str):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await add_regex_to_list(interaction, pattern, is_global=False)

    @app_commands.command(name="rm-name-keyword-substring", description="Removes a SUBSTRING keyword from this server's local list.")
    @has_mod_role()
    @discord.app_commands.describe(keyword="The exact keyword to remove.")
    async def remove_username_keyword_substring(self, interaction: discord.Interaction, keyword: str):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await remove_keyword_from_list(interaction, keyword, "username_keywords", "substring")

    @app_commands.command(name="rm-name-keyword-smart", description="Removes a SMART keyword from this server's local list.")
    @has_mod_role()
    @discord.app_commands.describe(keyword="The exact keyword to remove.")
    async def remove_username_keyword_smart(self, interaction: discord.Interaction, keyword: str):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await remove_keyword_from_list(interaction, keyword, "username_keywords", "smart")

    @app_commands.command(name="rm-bio-msg-keyword", description="Removes a keyword from this server's local bio/message list.")
    @has_mod_role()
    @discord.app_commands.describe(keyword="The exact keyword or phrase to remove.")
    async def remove_bio_keyword(self, interaction: discord.Interaction, keyword: str):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await remove_keyword_from_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

    @app_commands.command(name="rm-regex-by-id", description="Removes a regex from the local list by its ID from /list-keywords.")
    @has_mod_role()
    @discord.app_commands.describe(index="The numerical ID of the regex pattern to remove.")
    async def remove_local_regex_by_id(self, interaction: discord.Interaction, index: int):
        if not await has_federated_mod_role(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await remove_regex_from_list_by_id(interaction, index, is_global=False)

    @app_commands.command(name="scanallmembers", description="Retroactively scans all server members against the screening list.")
    @has_mod_role()
    async def scanallmembers(self, interaction: discord.Interaction):
        if not await has_federated_mod_role(interaction):
            return
        if interaction.guild.id in self.bot.active_scans:
            await interaction.response.send_message("❌ A scan is already in progress for this server.", ephemeral=True)
            return
        member_count = interaction.guild.member_count
        view = ConfirmScanView(author=interaction.user)
        await interaction.response.send_message(f"⚠️ **Are you sure?**\nThis will scan all **{member_count}** members...", view=view, ephemeral=True)
        await view.wait()
        if view.value is True:
            scan_task = self.bot.loop.create_task(screening_handler.run_full_scan(self.bot, interaction))
            self.bot.active_scans[interaction.guild.id] = scan_task
        elif view.value is None:
            try:
                await interaction.edit_original_response(content="Scan timed out.", view=None)
            except discord.NotFound:
                pass

    @app_commands.command(name="stopscan", description="Stops an ongoing member scan for this server.")
    @has_mod_role()
    async def stopscan(self, interaction: discord.Interaction):
        if not await has_federated_mod_role(interaction):
            return
        guild_id = interaction.guild.id
        if guild_id in self.bot.active_scans:
            self.bot.active_scans[guild_id].cancel()
            logger.info(f"Moderator {interaction.user.name} stopped the scan for guild {guild_id}.")
            await interaction.response.send_message("✅ Scan cancellation requested.", ephemeral=True)
        else:
            await interaction.response.send_message("ℹ️ No scan is currently in progress.", ephemeral=True)

    @app_commands.command(name="contact-maintainer", description="Send a message to the bot maintainer for requests, feedback, or issues.")
    @has_mod_role()
    @discord.app_commands.describe(message="Your message, feedback, or request for the bot maintainer.")
    async def contact_maintainer(self, interaction: discord.Interaction, message: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction):
            return

        log_channel_id = config.get("log_channel_id")
        if not log_channel_id:
            await interaction.response.send_message("❌ The bot's log channel is not configured. Please contact the owner directly.", ephemeral=True)
            return

        log_channel = self.bot.get_channel(log_channel_id)
        if not log_channel:
            try:
                log_channel = await self.bot.fetch_channel(log_channel_id)
            except (discord.NotFound, discord.Forbidden):
                await interaction.response.send_message("❌ Could not find or access the bot's log channel. Please contact the owner directly.", ephemeral=True)
                logger.error(f"Could not fetch log channel {log_channel_id} for contact-admin command.")
                return

        bot_owner_id = config.get("bot_owner_id")
        if not bot_owner_id:
            await interaction.response.send_message("❌ The bot owner's ID is not configured. Cannot send notification.", ephemeral=True)
            return

        embed = discord.Embed(
            title="📬 Contact Request",
            description="A new message has been sent by a moderator.",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="📝 Message", value=f"```{message[:1000]}```", inline=False)
        embed.add_field(name="👤 Sender", value=f"{interaction.user.name} (`{interaction.user.id}`)", inline=True)
        embed.add_field(name="🌐 Server", value=interaction.guild.name, inline=True)
        embed.set_footer(text="This is a direct request from a federated moderator.")

        try:
            await log_channel.send(content=f"<@{bot_owner_id}>", embed=embed)
            await interaction.response.send_message("✅ Your message has been successfully sent to the bot maintainer.", ephemeral=True)
            logger.info(f"Moderator {interaction.user.name} from {interaction.guild.name} sent an admin contact request.")
        except Exception as e:
            await interaction.response.send_message("❌ An error occurred while trying to send your message. Please try again later.", ephemeral=True)
            logger.error(f"Failed to send contact message to log channel: {e}", exc_info=True)

    @app_commands.command(name="onboard-server", description="Onboards a new server by syncing the federated ban list.")
    @has_mod_role()
    async def onboard_server(self, interaction: discord.Interaction):

        sync_status = await data_manager.load_sync_status()
        if interaction.guild.id in sync_status["synced_guild_ids"]:
            await interaction.response.send_message(
                "❌ **Action Prohibited:** This server has already been onboarded. "
                "Running this command again could incorrectly re-ban users who were locally unbanned. "
                "If a full re-sync is required, please contact the bot maintainer.",
                ephemeral=True
            )
            return

        # This might be heavy with 60k users, but necessary for the current OnboardView logic.
        # Future improvement: Stream this in chunks inside the View.
        all_bans = await data_manager.db_get_all_bans()
        ban_count = len(all_bans)

        if ban_count == 0:
            await interaction.response.send_message("ℹ️ The federated ban list is currently empty. No onboarding action is needed.", ephemeral=True)
            return

        welcome_embed = discord.Embed(
            title="👋 Welcome to the Antiscam Federation",
            description=(
                "The bot screens new members, messages, and bios against a shared threat database."
            ),
            color=discord.Color.blue()
        )

        welcome_embed.add_field(
            name="🛠️ Moderator Commands Available to You",
            value=(
                "As a whitelisted moderator, you have access to several slash commands:\n"
                "• Add and remove keywords to manage this server's custom keyword list.\n"
                "• Add, remove, and test regex to manage and test regex filters.\n"
                "• List keywords to see all active global and local keywords.\n"
                "• Global ban and unban to manually ban or unban a known scammer across all federated servers.\n"
                "• Lookup to search the federated ban list for a user ID or name.\n"
                "• Scan all members to retroactively scan the server against the ban list.\n"
                "• Stats to view this server's ban statistics."
            ),
            inline=False
        )

        welcome_embed.add_field(
            name="⚙️ Server Configuration",
            value=(
                "You can request changes to this server's config by using the `/contact-maintainer` command:\n"
                "• **Automation Level**: Full automation, suggest only, or off.\n"
                "• **Alert & Notice Channels**: Specific channels for different types of notifications.\n"
                "• **Timeout Duration**: How long flagged users are timed out for pending review.\n"
                "• **Message Deletion Days**: How many days of a user's message history are purged on ban.\n"
                "• **Whitelisted Roles**: Mod roles and other roles that should be exempt from all screening."
            ),
            inline=False
        )
    
        welcome_embed.add_field(
            name="❗️ A Note on Federation",
            value=(
                "A certain level of trust is expected among federated servers, as many actions are shared and propagated to all member servers."
            ),
            inline=False
        )

        welcome_embed.add_field(
            name="🚀 Next Step: Onboarding Sync",
            value=(
                "I will apply all historical bans from the master federated list.\nThis is a **one-time action** that will ban known scammers.\n\n"
                "Click **Begin Onboarding** to start the sync, or **Cancel** to abort (you can onboard later with `/onboard-server`)."
            ),
            inline=False
        )
        welcome_embed.add_field(
            name="Bans to Apply",
            value=f"**`{ban_count}`** users will be banned.",
            inline=False
        )
        welcome_embed.set_footer(text="❗️ This cannot be undone.")

        view = OnboardView(self.bot, interaction.user, all_bans)
        await interaction.response.send_message(embed=welcome_embed, view=view)

    @app_commands.command(name="mass-kick", description="Kicks multiple users from THIS SERVER by ID.")
    @app_commands.describe(
        user_ids="List of User IDs separated by spaces (e.g. 12345 67890).",
        reason="The reason for the kick."
    )
    @has_mod_role()
    async def mass_kick(self, interaction: discord.Interaction, user_ids: str, reason: str):
        # 1. Parse IDs from string (Same logic as mass-ban)
        clean_ids = user_ids.replace(',', ' ').replace('\n', ' ').split()
        
        target_ids = []
        invalid_ids = []

        for item in clean_ids:
            if item.isdigit():
                uid = int(item)
                # Safety checks
                if uid == interaction.user.id or uid == self.bot.user.id:
                    continue 
                target_ids.append(uid)
            else:
                invalid_ids.append(item)

        target_ids = list(set(target_ids))

        if not target_ids:
            await interaction.response.send_message("❌ No valid User IDs found in input.", ephemeral=True)
            return

        # 2. Construct Confirmation Embed
        confirm_embed = discord.Embed(
            title="⚠️ Confirm Mass Kick",
            description=f"You are about to kick **{len(target_ids)}** users from **{interaction.guild.name}**.\n\n*Note: This is a local action only. It will not ban them or affect other servers.*",
            color=discord.Color.dark_gold()
        )
        confirm_embed.add_field(name="Reason", value=f"```{reason}```", inline=False)
        
        preview_ids = "\n".join([str(uid) for uid in target_ids[:10]])
        if len(target_ids) > 10:
            preview_ids += f"\n...and {len(target_ids)-10} more."
        
        confirm_embed.add_field(name="Targets Preview", value=f"```\n{preview_ids}\n```", inline=False)

        if invalid_ids:
            confirm_embed.add_field(name="⚠️ Ignored (Invalid)", value=f"`{', '.join(invalid_ids[:5])}`", inline=False)

        # 3. Send View
        view = ConfirmMassKickView(self.bot, interaction.user, target_ids, reason)
        await interaction.response.send_message(embed=confirm_embed, view=view, ephemeral=True)
        
    @app_commands.command(name="mass-ban", description="Bans multiple users by ID. Separate IDs with spaces.")
    @app_commands.describe(
        user_ids="List of User IDs separated by spaces (e.g. 12345 67890 112233).",
        reason="The reason for the ban."
    )
    @has_mod_role()
    async def mass_ban(self, interaction: discord.Interaction, user_ids: str, reason: str):
        # 1. Parse IDs from string
        # Replace commas/newlines with spaces, then split
        clean_ids = user_ids.replace(',', ' ').replace('\n', ' ').split()
        
        target_ids = []
        invalid_ids = []

        for item in clean_ids:
            if item.isdigit():
                uid = int(item)
                # Safety checks
                if uid == interaction.user.id or uid == self.bot.user.id:
                    continue 
                target_ids.append(uid)
            else:
                invalid_ids.append(item)

        # Remove duplicates
        target_ids = list(set(target_ids))

        if not target_ids:
            await interaction.response.send_message("❌ No valid User IDs found in input.", ephemeral=True)
            return

        # 2. Construct Confirmation Embed
        confirm_embed = discord.Embed(
            title="⚠️ Confirm Mass Ban",
            description=f"You are about to globally ban **{len(target_ids)}** users.",
            color=discord.Color.dark_orange()
        )
        confirm_embed.add_field(name="Reason", value=f"```{reason}```", inline=False)
        
        # Show a preview of IDs (first 10)
        preview_ids = "\n".join([str(uid) for uid in target_ids[:10]])
        if len(target_ids) > 10:
            preview_ids += f"\n...and {len(target_ids)-10} more."
        
        confirm_embed.add_field(name="Targets Preview", value=f"```\n{preview_ids}\n```", inline=False)

        if invalid_ids:
            confirm_embed.add_field(name="⚠️ Ignored (Invalid)", value=f"`{', '.join(invalid_ids[:5])}`", inline=False)

        # 3. Send View
        view = ConfirmMassBanView(self.bot, interaction.user, target_ids, reason)
        await interaction.response.send_message(embed=confirm_embed, view=view, ephemeral=True)

    @app_commands.command(name="global-ban", description="Proactively bans a user by ID across all federated servers.")
    @has_mod_role()
    @discord.app_commands.describe(user_id="The Discord User ID of the person to ban.", reason="The reason for the ban. This will be shown in all federated alerts.")
    async def global_ban(self, interaction: discord.Interaction, user_id: str, reason: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction):
            return

        if not user_id.isdigit():
            await interaction.response.send_message("❌ **Invalid ID:** Please provide a valid Discord User ID (numbers only).", ephemeral=True)
            return
    
        target_user_id = int(user_id)

        if target_user_id == interaction.user.id:
            await interaction.response.send_message("❌ You cannot ban yourself.", ephemeral=True)
            return
    
        if target_user_id == self.bot.user.id:
            await interaction.response.send_message("❌ I cannot ban myself.", ephemeral=True)
            return

        try:
            user_to_ban = await self.bot.fetch_user(target_user_id)
        except discord.NotFound:
            await interaction.response.send_message(f"❌ **User Not Found:** No user exists with the ID `{target_user_id}`.", ephemeral=True)
            return
        except Exception as e:
            await interaction.response.send_message(f"❌ An error occurred while fetching the user: `{e}`", ephemeral=True)
            logger.error(f"Failed to fetch user for global-ban command: {e}", exc_info=True)
            return

        if user_to_ban.bot:
            await interaction.response.send_message("❌ **Action Prohibited:** You cannot target a bot account with this command.", ephemeral=True)
            return

        bot_owner_id = config.get("bot_owner_id")
        if bot_owner_id and user_to_ban.id == bot_owner_id:
            await interaction.response.send_message("❌ **Action Prohibited:** You cannot target the bot owner.", ephemeral=True)
            return

        if await is_federated_moderator(self.bot, user_to_ban.id):
            await interaction.response.send_message("❌ **Action Prohibited:** You cannot target another federated moderator. This action must be performed manually by the bot owner if necessary.", ephemeral=True)
            return
    
        existing_ban = await data_manager.db_get_ban(user_id)
        if existing_ban:
            # existing_ban is a dictionary, so we can access keys like before
            response_text = (
                "ℹ️ **User Already on Master List**\n"
                "This user is already on the federated ban list.\n\n"
                f"**Original Ban Details:**\n"
                f"- **Origin:** {existing_ban.get('origin_guild_name', 'N/A')}\n"
                f"- **Reason:** {existing_ban.get('reason', 'N/A')}\n\n"
                "Do you want to apply the ban in this server anyway?"
            )
            view = AlreadyBannedView(self.bot, interaction.user, user_to_ban, reason)
            await interaction.response.send_message(response_text, view=view, ephemeral=True)
            return
    
        confirm_embed = discord.Embed(
            title="⚠️ Confirm Global Ban",
            description="You are about to issue a federated ban for the following user. This action cannot be easily undone and will affect **all** federated servers.",
            color=discord.Color.orange()
        )
        confirm_embed.set_author(name=f"{user_to_ban.name} (`{user_to_ban.id}`)", icon_url=user_to_ban.display_avatar.url)
        confirm_embed.add_field(name="Reason", value=f"```{reason}```", inline=False)

        view = ConfirmGlobalBanView(self.bot, interaction.user, user_to_ban, reason)
        await interaction.response.send_message(embed=confirm_embed, view=view, ephemeral=True)

    @app_commands.command(name="global-unban", description="Proactively unbans a user by ID across all federated servers.")
    @app_commands.describe(user_id="The Discord User ID of the person to unban.", reason="The reason for the unban.")
    @has_mod_role()
    async def global_unban(self, interaction: discord.Interaction, user_id: str, reason: str):
        if not user_id.isdigit():
            await interaction.response.send_message("❌ **Invalid ID:** Please provide a valid Discord User ID.", ephemeral=True)
            return
        
        target_user_id = int(user_id)

        # Safety check: Is the user actually on the ban list?
        existing_ban = await data_manager.db_get_ban(user_id)
        if not existing_ban:
            await interaction.response.send_message(
                f"ℹ️ **Action Not Needed:** The user with ID `{user_id}` is not on the master federated ban list.",
                ephemeral=True
            )
            return

        try:
            user_to_unban = await self.bot.fetch_user(target_user_id)
        except discord.NotFound:
            # If user doesn't exist, we can still proceed with unban based on ID
            user_to_unban = discord.Object(id=target_user_id)
            # Use data from DB for name if possible
            user_to_unban.name = existing_ban.get("username", f"ID: {user_id}")
            user_to_unban.display_avatar = None
        except Exception as e:
            await interaction.response.send_message(f"❌ An error occurred while fetching the user: `{e}`", ephemeral=True)
            logger.error(f"Failed to fetch user for global-unban command: {e}", exc_info=True)
            return

        # Another safety check
        bot_owner_id = self.bot.config.get("bot_owner_id")
        if bot_owner_id and user_to_unban.id == bot_owner_id:
            await interaction.response.send_message("❌ **Action Prohibited:** The bot owner must be unbanned manually by themself in each server.", ephemeral=True)
            return

        confirm_embed = discord.Embed(
            title="⚠️ Confirm Global Unban",
            description="You are about to remove this user from the federated ban list. This will unban them from **all** federated servers.",
            color=discord.Color.orange()
        )
        if hasattr(user_to_unban, 'display_avatar') and user_to_unban.display_avatar:
             confirm_embed.set_author(name=f"{user_to_unban.name} (`{user_to_unban.id}`)", icon_url=user_to_unban.display_avatar.url)
        else:
             confirm_embed.set_author(name=f"{user_to_unban.name} (`{user_to_unban.id}`)")
        confirm_embed.add_field(name="Reason", value=f"```{reason}```", inline=False)

        view = ConfirmGlobalUnbanView(self.bot, interaction.user, user_to_unban, reason)
        await interaction.response.send_message(embed=confirm_embed, view=view, ephemeral=True)
        
    @app_commands.command(name="lookup", description="Looks up a user ID or username in the federated ban list.")
    @discord.app_commands.describe(query="The User ID or username to search for.")
    @has_mod_role()
    async def lookup(self, interaction: discord.Interaction, query: str):
        await interaction.response.defer()

        total_bans = await data_manager.db_get_ban_count()
        if not total_bans:
            await interaction.followup.send("The federated ban list is currently empty.", ephemeral=True)
            return

        query = query.strip()
        results = await data_manager.db_search_bans(query)

        if not results:
            await interaction.followup.send(f"No records found matching your query: `{query}`", ephemeral=True)
            return

        mentioned_users = [discord.Object(id=int(user_id)) for user_id, _ in results]
        allowed = discord.AllowedMentions(users=mentioned_users)

        view = LookupPaginatorView(author=interaction.user, query=query, results=results)
        initial_embed = view.create_embed()
        
        await interaction.followup.send(embed=initial_embed, view=view, allowed_mentions=allowed)

async def setup(bot: 'AntiScamBot'):
    """The setup function for the cog."""
    await bot.add_cog(ModCommands(bot))
