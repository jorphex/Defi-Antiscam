# /antiscam/cogs/mod_commands.py

import discord
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timezone
import re
from typing import TYPE_CHECKING

import data_manager
import screening_handler
from ui.views import ConfirmScanView, RegexTestModal, OnboardView, ConfirmGlobalBanView, LookupPaginatorView
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
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
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
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
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
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        await interaction.response.defer(ephemeral=True)
        await add_keyword_to_list(interaction, keyword, "username_keywords", "substring")

    @app_commands.command(name="add-name-keyword-smart", description="Precise name keyword (avoids false positives: 'mod' in 'mod123' not 'modern').")
    @has_mod_role()
    @discord.app_commands.describe(keyword="Example: 'mod' will match 'mod123' but IGNORE 'modern'.")
    async def add_username_keyword_smart(self, interaction: discord.Interaction, keyword: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        await interaction.response.defer(ephemeral=True)
        await add_keyword_to_list(interaction, keyword, "username_keywords", "smart")

    @app_commands.command(name="add-bio-msg-keyword", description="Keyword for screening user bios and messages.")
    @has_mod_role()
    @discord.app_commands.describe(keyword="The keyword or phrase to add (e.g., 'dm me for help').")
    async def add_bio_keyword(self, interaction: discord.Interaction, keyword: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        await interaction.response.defer(ephemeral=True)
        await add_keyword_to_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

    @app_commands.command(name="test-regex", description="Tests a regex pattern against sample text using a pop-up form.")
    @has_mod_role()
    @discord.app_commands.describe(pattern="The regex pattern to test. Remember to escape special characters (e.g., '\\.').")
    async def test_regex(self, interaction: discord.Interaction, pattern: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config):
            return

        try:
            compiled_regex = re.compile(pattern, re.IGNORECASE)
        except re.error as e:
            await interaction.response.send_message(f"❌ **Invalid Regex:** That pattern is not valid.\n`{e}`", ephemeral=True)
            return

        modal = RegexTestModal(pattern=pattern, compiled_regex=compiled_regex)
        await interaction.response.send_modal(modal)

    @app_commands.command(name="add-regex", description="Adds a regex pattern to this server's local list. Try /test-regex first!")
    @has_mod_role()
    @discord.app_commands.describe(pattern="The exact regex pattern. Use standard regex escaping (e.g., '\\.' for a dot, '\\s' for whitespace).")
    async def add_local_regex(self, interaction: discord.Interaction, pattern: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        await interaction.response.defer(ephemeral=True)
        await add_regex_to_list(interaction, pattern, is_global=False)

    @app_commands.command(name="rm-name-keyword-substring", description="Removes a SUBSTRING keyword from this server's local list.")
    @has_mod_role()
    @discord.app_commands.describe(keyword="The exact keyword to remove.")
    async def remove_username_keyword_substring(self, interaction: discord.Interaction, keyword: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        await interaction.response.defer(ephemeral=True)
        await remove_keyword_from_list(interaction, keyword, "username_keywords", "substring")

    @app_commands.command(name="rm-name-keyword-smart", description="Removes a SMART keyword from this server's local list.")
    @has_mod_role()
    @discord.app_commands.describe(keyword="The exact keyword to remove.")
    async def remove_username_keyword_smart(self, interaction: discord.Interaction, keyword: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        await interaction.response.defer(ephemeral=True)
        await remove_keyword_from_list(interaction, keyword, "username_keywords", "smart")

    @app_commands.command(name="rm-bio-msg-keyword", description="Removes a keyword from this server's local bio/message list.")
    @has_mod_role()
    @discord.app_commands.describe(keyword="The exact keyword or phrase to remove.")
    async def remove_bio_keyword(self, interaction: discord.Interaction, keyword: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        await interaction.response.defer(ephemeral=True)
        await remove_keyword_from_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

    @app_commands.command(name="rm-regex-by-id", description="Removes a regex from the local list by its ID from /list-keywords.")
    @has_mod_role()
    @discord.app_commands.describe(index="The numerical ID of the regex pattern to remove.")
    async def remove_local_regex_by_id(self, interaction: discord.Interaction, index: int):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        await interaction.response.defer(ephemeral=True)
        await remove_regex_from_list_by_id(interaction, index, is_global=False)

    @app_commands.command(name="scanallmembers", description="Retroactively scans all server members against the screening list.")
    @has_mod_role()
    async def scanallmembers(self, interaction: discord.Interaction):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        if interaction.guild.id in config.active_scans:
            await interaction.response.send_message("❌ A scan is already in progress for this server.", ephemeral=True)
            return
        member_count = interaction.guild.member_count
        view = ConfirmScanView(author=interaction.user)
        await interaction.response.send_message(f"⚠️ **Are you sure?**\nThis will scan all **{member_count}** members...", view=view, ephemeral=True)
        await view.wait()
        if view.value is True:
            scan_task = self.bot.loop.create_task(screening_handler.run_full_scan(interaction))
            config.active_scans[interaction.guild.id] = scan_task
        else:
            await interaction.followup.send("Scan cancelled or timed out.", ephemeral=True)

    @app_commands.command(name="stopscan", description="Stops an ongoing member scan for this server.")
    @has_mod_role()
    async def stopscan(self, interaction: discord.Interaction):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config): return
        guild_id = interaction.guild.id
        if guild_id in config.config.active_scans:
            config.config.active_scans[guild_id].cancel()
            config.logger.info(f"Moderator {interaction.user.name} stopped the scan for guild {guild_id}.")
            await interaction.response.send_message("✅ Scan cancellation requested.", ephemeral=True)
        else:
            await interaction.response.send_message("ℹ️ No scan is currently in progress.", ephemeral=True)

    @app_commands.command(name="contact-maintainer", description="Send a message to the bot maintainer for requests, feedback, or issues.")
    @has_mod_role()
    @discord.app_commands.describe(message="Your message, feedback, or request for the bot maintainer.")
    async def contact_maintainer(self, interaction: discord.Interaction, message: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config):
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
                config.logger.error(f"Could not fetch log channel {log_channel_id} for contact-admin command.")
                return

        bot_owner_id = config.get("bot_owner_id")
        if not bot_owner_id:
            await interaction.response.send_message("❌ The bot owner's ID is not configured. Cannot send notification.", ephemeral=True)
            return

        embed = discord.Embed(
            title="📬 Contact Request",
            description=f"A new message has been sent by a moderator.",
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
            config.logger.info(f"Moderator {interaction.user.name} from {interaction.guild.name} sent an admin contact request.")
        except Exception as e:
            await interaction.response.send_message("❌ An error occurred while trying to send your message. Please try again later.", ephemeral=True)
            config.logger.error(f"Failed to send contact message to log channel: {e}", exc_info=True)

    @app_commands.command(name="onboard-server", description="Onboards a new server by syncing the federated ban list.")
    @has_mod_role()
    async def onboard_server(self, interaction: discord.Interaction):
        config = self.bot.config

        sync_status = await data_manager.load_sync_status()
        if interaction.guild.id in sync_status["synced_guild_ids"]:
            await interaction.response.send_message(
                "❌ **Action Prohibited:** This server has already been onboarded. "
                "Running this command again could incorrectly re-ban users who were locally unbanned. "
                "If a full re-sync is required, please contact the bot administrator.",
                ephemeral=True
            )
            return

        fed_bans = await data_manager.load_fed_bans()
        ban_count = len(fed_bans)

        if ban_count == 0:
            await interaction.response.send_message("ℹ️ The federated ban list is currently empty. No onboarding action is needed.", ephemeral=True)
            return

        welcome_embed = discord.Embed(
            title="👋 Welcome!",
            description=(
                "This server is now part of my federated Defi Antiscam protection. "
                "I will screen new members, messages, and bios against a shared list of threats.\n\n"
                "**Next Step: Onboarding**\n"
                "To protect this server immediately, I will now apply all historical bans from the master federated ban list. "
                "This is a one-time action."
            ),
            color=discord.Color.blue()
        )
        welcome_embed.add_field(
            name="Bans to Apply",
            value=f"**`{ban_count}`** users will be banned.",
            inline=False
        )
        welcome_embed.set_footer(text="Click 'Begin Onboarding' to start the process. This cannot be undone.")

        view = OnboardView(author=interaction.user, fed_bans=fed_bans)
        await interaction.response.send_message(embed=welcome_embed, view=view)

    @app_commands.command(name="global-ban", description="Proactively bans a user by ID across all federated servers.")
    @has_mod_role()
    @discord.app_commands.describe(user_id="The Discord User ID of the person to ban.", reason="The reason for the ban. This will be shown in all federated alerts.")
    async def global_ban(self, interaction: discord.Interaction, user_id: str, reason: str):
        config = self.bot.config
        if not await has_federated_mod_role(interaction, config):
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
            config.logger.error(f"Failed to fetch user for global-ban command: {e}", exc_info=True)
            return

        if user_to_ban.bot:
            await interaction.response.send_message("❌ **Action Prohibited:** You cannot target a bot account with this command.", ephemeral=True)
            return

        bot_owner_id = config.get("bot_owner_id")
        if bot_owner_id and user_to_ban.id == bot_owner_id:
            await interaction.response.send_message("❌ **Action Prohibited:** You cannot target the bot owner.", ephemeral=True)
            return

        if await is_federated_moderator(user_to_ban.id):
            await interaction.response.send_message("❌ **Action Prohibited:** You cannot target another federated moderator. This action must be performed manually by the bot owner if necessary.", ephemeral=True)
            return
    
        fed_bans = await data_manager.load_fed_bans()
        if user_id in fed_bans:
            ban_data = fed_bans[user_id]
            await interaction.response.send_message(
                "ℹ️ **Action Not Needed:** This user is already on the master federated ban list.\n\n"
                f"**Original Ban Details:**\n"
                f"- **Origin:** {ban_data.get('origin_guild_name', 'N/A')}\n"
                f"- **Reason:** {ban_data.get('reason', 'N/A')}",
                ephemeral=True
            )
            return
    
        confirm_embed = discord.Embed(
            title="⚠️ Confirm Global Ban",
            description=f"You are about to issue a federated ban for the following user. This action cannot be easily undone and will affect **all** federated servers.",
            color=discord.Color.orange()
        )
        confirm_embed.set_author(name=f"{user_to_ban.name} (`{user_to_ban.id}`)", icon_url=user_to_ban.display_avatar.url)
        confirm_embed.add_field(name="Reason", value=f"```{reason}```", inline=False)

        view = ConfirmGlobalBanView(author=interaction.user, user_to_ban=user_to_ban, reason=reason)
        await interaction.response.send_message(embed=confirm_embed, view=view, ephemeral=True)

    @app_commands.command(name="lookup", description="Looks up a user ID or username in the federated ban list.")
    @discord.app_commands.describe(query="The User ID or username to search for.")
    @has_mod_role()
    async def lookup(self, interaction: discord.Interaction, query: str):
        await interaction.response.defer()

        fed_bans = await data_manager.load_fed_bans()
        if not fed_bans:
            await interaction.followup.send("The federated ban list is currently empty.", ephemeral=True)
            return

        query = query.strip()
        results = []

        if query.isdigit():
            # --- ID Search (Exact Match) ---
            user_id_str = query
            if user_id_str in fed_bans:
                results.append((user_id_str, fed_bans[user_id_str]))
        else:
            # --- Username Search (Case-insensitive, Partial Match) ---
            query_lower = query.lower()
            for user_id, data in fed_bans.items():
                username = data.get("username_at_ban", "").lower()
                if query_lower in username:
                    results.append((user_id, data))

        if not results:
            await interaction.followup.send(f"No records found matching your query: `{query}`", ephemeral=True)
            return

        mentioned_users = [discord.Object(id=int(user_id)) for user_id, data in results]
        allowed = discord.AllowedMentions(users=mentioned_users)

        view = LookupPaginatorView(author=interaction.user, query=query, results=results)
        initial_embed = view.create_embed()
    
        await interaction.followup.send(embed=initial_embed, view=view, allowed_mentions=allowed)