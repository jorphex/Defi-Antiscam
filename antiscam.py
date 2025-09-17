import discord
from discord.ext import commands
import json
import re
from typing import List, Optional
from unidecode import unidecode
from datetime import datetime, timezone, timedelta
import asyncio
from dotenv import load_dotenv
import os
import sys
import logging

# --- LOGGING ---
logger = logging.getLogger('discord')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# --- CONFIGURATION ---
load_dotenv()
BOT_TOKEN = os.getenv("ANTISCAM_BOT_TOKEN")
KEYWORDS_FILE = "keywords.json"
FED_STATS_FILE = "stats.json"
FED_CONFIG_FILE = "config.json"
FED_BANS_FILE = "bans.json"
SYNC_STATUS_FILE = "sync_status.json"
active_scans = {}
stats_lock = asyncio.Lock()
keywords_lock = asyncio.Lock()
config_lock = asyncio.Lock()
fed_bans_lock = asyncio.Lock()
sync_status_lock = asyncio.Lock()
bio_check_cache = {}
_has_synced_once = False

# --- DATA HANDLING & KEYWORD MATCHING ---
def load_federation_config():
    if os.path.exists(FED_CONFIG_FILE):
        with open(FED_CONFIG_FILE, 'r') as f:
            try: return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Could not decode {FED_CONFIG_FILE}.")
                return {}
    else:
        logger.error(f"{FED_CONFIG_FILE} not found.")
        return {}

async def load_fed_bans():
    """Loads the master federated ban list from its JSON file."""
    async with fed_bans_lock:
        if not os.path.exists(FED_BANS_FILE):
            return {}
        with open(FED_BANS_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Could not decode {FED_BANS_FILE}.")
                return {}

async def save_fed_bans(data: dict):
    """Saves the master federated ban list to its JSON file."""
    async with fed_bans_lock:
        with open(FED_BANS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

async def load_sync_status():
    """Loads the list of synced guild IDs."""
    async with sync_status_lock:
        if not os.path.exists(SYNC_STATUS_FILE):
            return {"synced_guild_ids": []}
        with open(SYNC_STATUS_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Could not decode {SYNC_STATUS_FILE}.")
                return {"synced_guild_ids": []}

async def save_sync_status(data: dict):
    """Saves the list of synced guild IDs."""
    async with sync_status_lock:
        with open(SYNC_STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

async def load_fed_stats():
    async with stats_lock:
        if os.path.exists(FED_STATS_FILE):
            with open(FED_STATS_FILE, 'r') as f:
                try: return json.load(f)
                except json.JSONDecodeError: return {}
        return {}

async def save_fed_stats(data: dict):
    async with stats_lock:
        with open(FED_STATS_FILE, 'w') as f:
            json.dump(data, f, indent=4)

async def load_keywords():
    async with keywords_lock:
        if os.path.exists(KEYWORDS_FILE):
            with open(KEYWORDS_FILE, 'r', encoding='utf-8') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    logger.error(f"Could not decode {KEYWORDS_FILE}.")
                    return None
        else:
            logger.error(f"{KEYWORDS_FILE} not found.")
            return None

async def save_keywords(keywords_data: dict):
    async with keywords_lock:
        with open(KEYWORDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(keywords_data, f, indent=4)


# --- UI ---
class ScreeningView(discord.ui.View):
    def __init__(self, flagged_member_id: int):
        super().__init__(timeout=None)
        self.flagged_member_id = flagged_member_id

    def update_buttons_for_state(self, state: str):
        """Updates button disabled status based on the user's state.
        States: 'initial', 'banned', 'kicked'
        """
        if state == 'initial':
            # User is in the server (likely timed out), awaiting action
            self.ban_button.disabled = False
            self.kick_button.disabled = False
            self.ignore_button.disabled = False
            self.unban_button.disabled = True
        elif state == 'banned':
            # User has been banned from the server
            self.ban_button.disabled = True
            self.kick_button.disabled = True # Can't kick a banned user
            self.ignore_button.disabled = True
            self.unban_button.disabled = False
        elif state == 'kicked':
            # User has been kicked, a final action for this alert
            self.ban_button.disabled = True
            self.kick_button.disabled = True
            self.ignore_button.disabled = True
            self.unban_button.disabled = True

    async def get_user_and_member(self, interaction: discord.Interaction) -> tuple[Optional[discord.User], Optional[discord.Member]]:
        """
        Fetches the User object and, if possible, the Member object.
        The User object is almost always available, while the Member is only if they are in the server.
        """
        if not self.flagged_member_id:
            try:
                embed_footer = interaction.message.embeds[0].footer.text
                match = re.search(r'User ID: (\d+)', embed_footer)
                if match:
                    self.flagged_member_id = int(match.group(1))
                else:
                    await interaction.followup.send("❌ Could not find user ID in the alert footer.", ephemeral=True)
                    return None, None
            except (IndexError, TypeError, ValueError, AttributeError):
                await interaction.followup.send("❌ Could not parse user ID from the alert.", ephemeral=True)
                return None, None

        user = bot.get_user(self.flagged_member_id)
        if not user:
            try:
                user = await bot.fetch_user(self.flagged_member_id)
            except discord.NotFound:
                await interaction.followup.send("❌ User ID is invalid or the user account was deleted.", ephemeral=True)
                return None, None
        
        member = interaction.guild.get_member(self.flagged_member_id)
        
        return user, member
                
    async def update_embed(self, interaction: discord.Interaction, status: str, color: discord.Color):
        embed = interaction.message.embeds[0]
        embed.color = color
        for i, field in enumerate(embed.fields):
            if field.name == "Status":
                embed.set_field_at(i, name="Status", value=status, inline=True)
                break
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)

    @discord.ui.button(label="Ban", style=discord.ButtonStyle.red, custom_id="screening_ban")
    async def ban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        result = await self.get_user_and_member(interaction)
        if result == (None, None):
            return
        user, member = result

        try:
            original_embed = interaction.message.embeds[0]
            descriptive_reason = "Reason not parsed from alert." # Fallback

            if "User Banned Elsewhere" in original_embed.title:
                for field in original_embed.fields:
                    if "Banned In" in field.name:
                        descriptive_reason = f"User already banned in {field.name.split(': ')[1]}."
                        break
            elif "Flagged User" in original_embed.title:
                for field in original_embed.fields:
                    if "Trigger" in field.name:
                        descriptive_reason = f"Flagged by keyword screening. Trigger: {field.value.strip('`')}."
                        break
            elif "Flagged Message" in original_embed.title:
                for field in original_embed.fields:
                    if "Trigger" in field.name:
                        descriptive_reason = f"Flagged for a message. Trigger: {field.value.strip('`')}."
                        break
            
            reason_text = f"[Federated Action] {descriptive_reason} | AlertID:{interaction.message.id}"
            
            delete_days = get_delete_days_for_guild(interaction.guild)
            delete_seconds = delete_days * 86400
            
            await interaction.guild.ban(user, reason=reason_text, delete_message_seconds=delete_seconds)
            
            self.update_buttons_for_state('banned')
            
            status_text = "✅ Banned"
            if not member:
                status_text += " (User had left)"
                
            await self.update_embed(interaction, status_text, discord.Color.red())
        except Exception as e:
            logger.error(f"Failed to ban user {self.flagged_member_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error banning: {e}", ephemeral=True)

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.primary, custom_id="screening_kick")
    async def kick_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        result = await self.get_user_and_member(interaction)
        if result == (None, None):
            return
        _, member = result
        
        if not member:
            await self.update_embed(interaction, "❌ Kick Failed (User left)", discord.Color.greyple())
            self.kick_button.disabled = True
            await interaction.followup.edit_message(message_id=interaction.message.id, view=self)
            await interaction.followup.send("❌ Cannot kick a user who is not in the server.", ephemeral=True)
            return

        try:
            reason_text = "Kicked by Moderator via screening alert."
            await member.kick(reason=reason_text)
            self.update_buttons_for_state('kicked')
            await self.update_embed(interaction, "👢 Kicked", discord.Color.blue())
        except Exception as e:
            logger.error(f"Failed to kick member {self.flagged_member_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An error occurred while kicking: {e}", ephemeral=True)

    @discord.ui.button(label="Unban", style=discord.ButtonStyle.grey, custom_id="screening_unban", disabled=True)
    async def unban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        user_to_unban = discord.Object(id=self.flagged_member_id)
        try:
            reason_text = "[Federated Action] Unbanned by Moderator via screening alert."
            await interaction.guild.unban(user_to_unban, reason=reason_text)
            self.update_buttons_for_state('initial')
            await self.update_embed(interaction, "🟡 Unbanned", discord.Color.gold())
        except Exception as e:
            logger.error(f"Failed to unban member {self.flagged_member_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error unbanning: {e}", ephemeral=True)

    @discord.ui.button(label="Ignore", style=discord.ButtonStyle.grey, custom_id="screening_ignore")
    async def ignore_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        result = await self.get_user_and_member(interaction)
        if result == (None, None):
            return
        _, member = result
        
        if member:
            try:
                await member.timeout(None, reason="Flag marked as safe by Moderator.")
                logger.info(f"Removed timeout for {member.name} after ignore.")
            except Exception as e:
                logger.warning(f"Could not remove timeout for {member.name} on ignore: {e}")
        
        try:
            await interaction.message.delete()
            await interaction.followup.send("✅ Alert dismissed.", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to delete screening message: {e}", exc_info=True)
            if not interaction.is_done():
                 await interaction.followup.send("❌ An error occurred during cleanup.", ephemeral=True)

class FederatedAlertView(discord.ui.View):
    def __init__(self, banned_user_id: int):
        super().__init__(timeout=None)
        self.banned_user_id = banned_user_id

    @discord.ui.button(label="Unban Locally", style=discord.ButtonStyle.secondary, custom_id="fed_alert_unban")
    async def unban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        user_to_unban = discord.Object(id=self.banned_user_id)
        try:
            reason_text = "[Local Action] Federated ban reversed by local Moderator."
            await interaction.guild.unban(user_to_unban, reason=reason_text)
            embed = interaction.message.embeds[0]
            embed.color = discord.Color.green()
            embed.description += f"\n\n**UPDATE:** User was unbanned from this server by a local contributor."
            button.disabled = True
            await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)
            logger.info(f"Federated ban for {self.banned_user_id} was reversed in {interaction.guild.name} by {interaction.user.name}.")
        except Exception as e:
            logger.error(f"Failed to reverse federated ban for {self.banned_user_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error unbanning: {e}", ephemeral=True)

class ConfirmGlobalBanView(discord.ui.View):
    def __init__(self, author: discord.User, user_to_ban: discord.User, reason: str):
        super().__init__(timeout=60.0)
        self.author = author
        self.user_to_ban = user_to_ban
        self.reason = reason

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You cannot interact with this confirmation.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Global Ban", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="✅ **Confirmation received. Propagating global ban...**", embed=None, view=self)

        try:
            config = bot.config
            origin_mod_channel_id = config.get("mod_alert_channels", {}).get(str(interaction.guild.id))
            if origin_mod_channel_id:
                origin_mod_channel = bot.get_channel(origin_mod_channel_id) or await bot.fetch_channel(origin_mod_channel_id)
                if origin_mod_channel:
                    confirm_embed = discord.Embed(
                        title="✅ Proactive Global Ban Initiated",
                        description=f"Ban was initiated from this server and has been broadcast to all federated servers.",
                        color=discord.Color.blue(),
                        timestamp=datetime.now(timezone.utc)
                    )
                    confirm_embed.set_author(name=f"{self.user_to_ban.name} (`{self.user_to_ban.id}`)", icon_url=self.user_to_ban.display_avatar.url)
                    confirm_embed.add_field(name="Reason", value=f"```{self.reason}```", inline=False)
                    await origin_mod_channel.send(embed=confirm_embed)

            await propagate_ban(
                origin_guild=interaction.guild,
                user_to_ban=self.user_to_ban,
                moderator=interaction.user,
                reason=self.reason
            )
            
            await interaction.followup.send(f"✅ **Success!** The global ban for **{self.user_to_ban.name}** has been initiated and propagated.", ephemeral=True)
            logger.info(f"Moderator {interaction.user.name} initiated a proactive global ban for {self.user_to_ban.name} from {interaction.guild.name}.")

        except Exception as e:
            await interaction.followup.send(f"❌ **Error:** An unexpected error occurred during propagation. Please check the logs.", ephemeral=True)
            logger.error(f"Error during proactive global ban propagation: {e}", exc_info=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Global ban cancelled.", embed=None, view=self)

class ConfirmScanView(discord.ui.View):
    def __init__(self, author: discord.User):
        super().__init__(timeout=60.0)
        self.author = author
        self.value = None
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You cannot interact with this confirmation.", ephemeral=True)
            return False
        return True
    @discord.ui.button(label="Confirm Scan", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        self.stop()
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(content="✅ **Confirmation received. Starting scan...** See below for progress updates.", view=self)
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        self.stop()
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(content="Scan cancelled.", view=self)

class RegexTestModal(discord.ui.Modal, title="Regex Test"):
    def __init__(self, pattern: str, compiled_regex: re.Pattern):
        super().__init__()
        self.pattern = pattern
        self.compiled_regex = compiled_regex

    sample_text = discord.ui.TextInput(
        label="Sample Text",
        style=discord.TextStyle.paragraph,
        placeholder="Paste the raw, multi-line sample text here...",
        required=True,
        max_length=1000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        text_to_test = self.sample_text.value
        match = self.compiled_regex.search(text_to_test)

        if match:
            embed = discord.Embed(title="✅ Regex Test: Match Found", color=discord.Color.green())
            embed.add_field(name="Pattern", value=f"`{self.pattern}`", inline=False)
            embed.add_field(name="Sample Text", value=f"```{text_to_test}```", inline=False)
            embed.add_field(name="Matched Text", value=f"`{match.group(0)}`", inline=False)
        else:
            embed = discord.Embed(title="❌ Regex Test: No Match", color=discord.Color.orange())
            embed.add_field(name="Pattern", value=f"`{self.pattern}`", inline=False)
            embed.add_field(name="Sample Text", value=f"```{text_to_test}```", inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        logger.error(f"Error in RegexTestModal: {error}", exc_info=True)
        await interaction.response.send_message("An unexpected error occurred. Please check the logs.", ephemeral=True)

class OnboardView(discord.ui.View):
    def __init__(self, author: discord.User, fed_bans: dict):
        super().__init__(timeout=300.0) # 5 minute timeout to confirm
        self.author = author
        self.fed_bans = fed_bans

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You are not the one who initiated this command.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Begin Onboarding", style=discord.ButtonStyle.danger)
    async def begin_onboarding(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        progress_embed = discord.Embed(
            title="⏳ Onboarding in Progress...",
            description="Applying historical bans. This may take some time.",
            color=discord.Color.orange()
        )
        progress_embed.add_field(name="Checked", value="`0`", inline=True)
        progress_embed.add_field(name="Applied", value="`0`", inline=True)
        progress_embed.add_field(name="Failed", value="`0`", inline=True)
        
        progress_message = await interaction.followup.send(embed=progress_embed, wait=True)

        target_guild = interaction.guild
        total_bans = len(self.fed_bans)
        applied_count = 0
        already_banned_count = 0
        failed_count = 0
        update_interval = 25

        for i, (user_id_str, ban_data) in enumerate(self.fed_bans.items()):
            user_id = int(user_id_str)
            user_obj = discord.Object(id=user_id)

            try:
                await target_guild.fetch_ban(user_obj)
                already_banned_count += 1
                continue
            except discord.NotFound:
                try:
                    reason = f"Federated ban sync. Original reason: {ban_data.get('reason', 'N/A')}"
                    delete_days = get_delete_days_for_guild(target_guild)
                    delete_seconds = delete_days * 86400
                    await target_guild.ban(user_obj, reason=reason[:512], delete_message_seconds=delete_seconds)
                    applied_count += 1
                except Exception as e:
                    logger.warning(f"Failed to onboard-ban user {user_id} in {target_guild.name}: {e}")
                    failed_count += 1
            
            if (i + 1) % update_interval == 0 or (i + 1) == total_bans:
                progress_embed.set_field_at(0, name="Checked", value=f"`{i+1} / {total_bans}`", inline=True)
                progress_embed.set_field_at(1, name="Applied", value=f"`{applied_count}`", inline=True)
                progress_embed.set_field_at(2, name="Failed", value=f"`{failed_count}`", inline=True)
                await progress_message.edit(embed=progress_embed)

        completion_embed = discord.Embed(
            title="✅ Onboarding Complete",
            description=f"The server is now up to date with the federated ban list.",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        )
        completion_embed.add_field(name="Bans Applied", value=f"`{applied_count}`", inline=True)
        completion_embed.add_field(name="Already Banned", value=f"`{already_banned_count}`", inline=True)
        completion_embed.add_field(name="Failed", value=f"`{failed_count}`", inline=True)
        
        await progress_message.edit(content=None, embed=completion_embed)

        sync_status = await load_sync_status()
        if target_guild.id not in sync_status["synced_guild_ids"]:
            sync_status["synced_guild_ids"].append(target_guild.id)
            await save_sync_status(sync_status)
        
        await update_onboard_command_visibility(interaction.guild)
        logger.info(f"Server {interaction.guild.name} has been successfully onboarded and permissions updated.")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Onboarding cancelled.", view=self, embed=None)

class LookupPaginatorView(discord.ui.View):
    def __init__(self, author: discord.User, query: str, results: list):
        super().__init__(timeout=300.0)
        self.author = author
        self.query = query
        self.results = results
        
        self.current_page = 0
        self.items_per_page = 5
        self.total_pages = (len(self.results) - 1) // self.items_per_page + 1

        mentioned_users = [discord.Object(id=int(user_id)) for user_id, data in self.results]
        self.allowed = discord.AllowedMentions(users=mentioned_users)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You are not the one who initiated this command.", ephemeral=True)
            return False
        return True

    def create_embed(self) -> discord.Embed:
        """Creates the embed for the current page."""
        start_index = self.current_page * self.items_per_page
        end_index = start_index + self.items_per_page
        page_results = self.results[start_index:end_index]

        embed = discord.Embed(
            title=f"Ban List Search Results for \"{self.query}\"",
            description=f"Found **{len(self.results)}** matching record(s).",
            color=discord.Color.blue()
        )

        page_content = []
        for user_id, data in page_results:
            entry = (
                f"**Username:** {data.get('username_at_ban', 'N/A')}\n"
                f"**User ID:** `{user_id}`\n"
                f"**Origin:** {data.get('origin_guild_name', 'N/A')}\n"
                f"**Reason:** {data.get('reason', 'N/A')}"
            )
            page_content.append(entry)
        
        embed.description += "\n\n" + "\n--------------------\n".join(page_content)
        embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")
        
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1
        
        return embed

    @discord.ui.button(label="◄ Previous", style=discord.ButtonStyle.secondary, custom_id="lookup_prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            await interaction.response.edit_message(embed=self.create_embed(), view=self, allowed_mentions=self.allowed)

    @discord.ui.button(label="Next ►", style=discord.ButtonStyle.secondary, custom_id="lookup_next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            await interaction.response.edit_message(embed=self.create_embed(), view=self, allowed_mentions=self.allowed)

# --- BOT SETUP ---
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.moderation = True
intents.message_content = True

class AntiScamBot(discord.Client):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)
        self.config = load_federation_config() 

bot = AntiScamBot(intents=intents)


# --- PERMISSION CHECKS ---
def is_bot_owner():
    """A check decorator to ensure the user is the bot's owner."""
    async def predicate(interaction: discord.Interaction) -> bool:
        app_info = await interaction.client.application_info()
        if interaction.user.id == app_info.owner.id:
            return True
        return False
    return discord.app_commands.check(predicate)

def has_mod_role():
    """
    A check decorator that passes if the command user has a federated moderator role
    in the current guild, OR if the user is the bot owner.
    """
    async def predicate(interaction: discord.Interaction) -> bool:
        config = interaction.client.config
        
        bot_owner_id = config.get("bot_owner_id")
        if bot_owner_id and interaction.user.id == bot_owner_id:
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

async def has_federated_mod_role(interaction: discord.Interaction, config: dict) -> bool:
    """Checks if the user has a whitelisted moderator role for the current guild."""
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

async def is_federated_moderator(user_id_to_check: int) -> bool:
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
            member = None
            for attempt in range(3):
                try:
                    member = await guild.fetch_member(user_id_to_check)
                    break
                except discord.HTTPException as e:
                    if e.status == 503 and attempt < 2:
                        await asyncio.sleep(2 * (attempt + 1))
                        continue
                    raise
            
            if not member: return False
            return any(role.id in all_mod_roles for role in member.roles)
        
        except discord.NotFound:
            return False
        except Exception as e:
            logger.warning(f"Could not fetch member {user_id_to_check} in guild {guild.name} for is_federated_moderator check: {e}")
            return False

    tasks = [asyncio.create_task(check_guild(guild_id)) for guild_id in config.get("federated_guild_ids", [])]
    
    for future in asyncio.as_completed(tasks):
        result = await future
        if result:
            logger.info(f"is_federated_moderator check PASSED for {user_id_to_check}.")
            for task in tasks:
                if not task.done():
                    task.cancel()
            return True
    
    return False


# --- EVENT LISTENERS ---
@bot.event
async def on_ready():
    global _has_synced_once 

    logger.info(f'{bot.user.name} has connected to Discord!')
    logger.info(f"Operating in {len(bot.guilds)} federated guilds.")
    bot.add_view(ScreeningView(flagged_member_id=None))
    bot.add_view(FederatedAlertView(banned_user_id=None))
    # try:
    #     synced = await bot.tree.sync()
    #     logger.info(f"Synced {len(synced)} slash command(s).")
    # except Exception as e:
    #     logger.error(f"Failed to sync slash commands: {e}")

    if not _has_synced_once:
        try:
            logger.info("Performing one-time command sync...")
            synced = await bot.tree.sync()
            logger.info(f"Synced {len(synced)} command(s) globally.")
            _has_synced_once = True # Set the flag so it doesn't run again on reconnect
        except Exception as e:
            logger.error(f"Failed to perform one-time sync: {e}", exc_info=True)


@bot.event
async def on_guild_join(guild: discord.Guild):
    """Called when the bot joins a new guild."""
    logger.info(f"Joined new guild: {guild.name} ({guild.id}). Setting up command permissions.")

@bot.event
async def on_member_join(member: discord.Member):
    config = bot.config
    if member.guild.id not in config.get("federated_guild_ids", []): return
    
    await asyncio.sleep(2)
    try:
        member = await member.guild.fetch_member(member.id)
    except discord.NotFound:
        logger.warning(f"Member {member.name} left before they could be processed.")
        return

    whitelisted_roles = config.get("whitelisted_roles_per_guild", {}).get(str(member.guild.id), [])
    if any(role.id in whitelisted_roles for role in member.roles):
        logger.info(f"Member {member.name} has a whitelisted role. Skipping screen.")
        return

    keywords_data = await load_keywords()
    if not keywords_data:
        logger.error("Could not load keywords for on_member_join screen. Aborting check.")
        return

    result = await screen_member(member, config, keywords_data)

    if result.get("flagged"):
        logger.info(f"FLAGGED member on join: {member.name} in {member.guild.name}.")
        mod_channel_id = config.get("mod_alert_channels", {}).get(str(member.guild.id))
        alert_channel = member.guild.get_channel(mod_channel_id) if mod_channel_id else None
        
        if alert_channel:
            try:
                timeout_minutes = get_timeout_minutes_for_guild(member.guild)
                await member.timeout(timedelta(minutes=timeout_minutes), reason=result.get("timeout_reason"))
                view = ScreeningView(flagged_member_id=member.id)
                embed = result.get("embed")
                embed.set_footer(text=f"User ID: {member.id}")
                allowed_mentions = discord.AllowedMentions(users=[member])
                await alert_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)
            except Exception as e:
                logger.error(f"Failed to take action on flagged member {member.name}: {e}")
    
    if not result.get("flagged"):
        logger.info(f"Member {member.name} in {member.guild.name} passed all screenings.")

@bot.event
async def on_message(message: discord.Message):
    """Scans messages in real-time by calling the core screening function."""
    config = bot.config
    
    if message.author == bot.user or message.webhook_id is not None or message.interaction_metadata is not None: return
    
    if not message.guild or message.guild.id not in config.get("federated_guild_ids", []) or message.author.bot: return
    if not isinstance(message.author, discord.Member): return
    
    whitelisted_roles = config.get("whitelisted_roles_per_guild", {}).get(str(message.guild.id), [])
    if any(role.id in whitelisted_roles for role in message.author.roles): return

    keywords_data = await load_keywords()
    if not keywords_data: return

    # --- Primary Defense: Screen the message content first ---
    result = await screen_message(message, config, keywords_data)

    # --- Secondary Defense: If message is clean, screen the user's bio ---
    if not result.get("flagged"):
        author_id = message.author.id
        current_time = datetime.now(timezone.utc)
        
        if author_id in bio_check_cache:
            last_checked = bio_check_cache[author_id]
            if (current_time - last_checked).total_seconds() < 300: # 300 seconds = 5 minutes
                return # Already checked recently, do nothing.

        bio_result = await screen_bio(message.author, config, keywords_data)
        
        bio_check_cache[author_id] = current_time

        if bio_result.get("flagged"):
            embed = bio_result.get("embed")
            embed.description = (
                f"**User:** {message.author.mention} (`{message.author.id}`)\n"
                f"This user sent a valid message in {message.channel.mention}, but their bio was flagged upon inspection."
            )
            result = bio_result

    if result.get("flagged"):
        author = message.author
        logger.info(f"FLAGGED event for {author.name} in #{message.channel.name} (Trigger: {'Bio' if 'Bio' in result['embed'].title else 'Message'}).")
        
        # Don't delete the message if the bio was the trigger as the message itself is harmless.
        if "Message" in result["embed"].title:
            try: await message.delete()
            except Exception as e: logger.error(f"Error deleting flagged message: {e}")
        
        try:
            timeout_minutes = get_timeout_minutes_for_guild(author.guild)
            await author.timeout(timedelta(minutes=timeout_minutes), reason=result.get("timeout_reason", "Flagged content."))
        except discord.HTTPException as e:
            if e.code == 40007: # User is not a member of this guild
                logger.warning(f"Could not timeout {author.name} because they have already left the server.")
            else:
                logger.error(f"Failed to timeout {author.name}: {e}")
        except Exception as e:
             logger.error(f"An unexpected error occurred while trying to timeout {author.name}: {e}")
        
        mod_channel_id = config.get("mod_alert_channels", {}).get(str(message.guild.id))
        alert_channel = message.guild.get_channel(mod_channel_id) if mod_channel_id else None
        if alert_channel:
            try:
                view = ScreeningView(flagged_member_id=author.id)
                embed = result.get("embed")
                embed.set_footer(text=f"User ID: {author.id}")
                allowed_mentions = discord.AllowedMentions(users=[author])
                await alert_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)
            except Exception as e:
                logger.error(f"Failed to send alert for {author.name}: {e}")

@bot.event
async def on_member_ban(guild: discord.Guild, user: discord.User):
    config = bot.config
    if guild.id not in config.get("federated_guild_ids", []): return
    
    stats = await load_fed_stats()
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")
        
    moderator, ban_reason = None, "No reason provided."
    await asyncio.sleep(2)

    try:
        async for entry in guild.audit_logs(action=discord.AuditLogAction.ban, limit=5):
            if entry.target.id == user.id:
                moderator, ban_reason = entry.user, entry.reason or "No reason provided."
                break
    except Exception as e:
        logger.error(f"Error fetching audit logs in {guild.name}: {e}", exc_info=True)
        return
        
    if not moderator:
        logger.info(f"Ban of {user} in {guild.name} could not be attributed. No federated action.")
        return
    
    if "Federated ban sync" in ban_reason:
        logger.info(f"Ignoring on_member_ban event for sync-related ban of {user.name}.")
        return
    
    if "Proactive ban initiated by" in ban_reason:
        logger.info(f"Ignoring on_member_ban event for proactive ban of {user.name}.")
        return
    
    if ban_reason.startswith("Federated ban from"):
        logger.info(f"Ignoring federated ban echo for {user} in {guild.name}.")
        return

    is_authorized, authorization_method = False, "Unknown"
    detailed_reason_field = None

    if moderator.id == bot.user.id:
        alert_id_match = re.search(r"AlertID:(\d+)", ban_reason)
        if alert_id_match:
            is_authorized, authorization_method = True, "Authorized via Bot Alert"
            alert_message_id = int(alert_id_match.group(1))
            alert_message = None
            guild_id_str = str(guild.id)

            primary_channel_id = config.get("mod_alert_channels", {}).get(guild_id_str)
            if primary_channel_id:
                try:
                    channel = bot.get_channel(primary_channel_id) or await bot.fetch_channel(primary_channel_id)
                    alert_message = await channel.fetch_message(alert_message_id)
                except (discord.NotFound, discord.Forbidden):
                    alert_message = None # Not found here, will try next channel

            if not alert_message:
                scan_channel_id = config.get("mod_scan_results_channels", {}).get(guild_id_str)
                if scan_channel_id and scan_channel_id != primary_channel_id:
                    try:
                        channel = bot.get_channel(scan_channel_id) or await bot.fetch_channel(scan_channel_id)
                        alert_message = await channel.fetch_message(alert_message_id)
                    except (discord.NotFound, discord.Forbidden):
                        alert_message = None # Not found here either
                        
            if alert_message and alert_message.embeds:
                original_embed = alert_message.embeds[0]
                for field in original_embed.fields:
                    if "Message" in field.name or "Bio" in field.name or "Banned In" in field.name:
                        detailed_reason_field = {"name": f"Original {field.name}", "value": field.value}
                        break
                if not detailed_reason_field:
                     for field in original_embed.fields:
                        if "Trigger" in field.name:
                            detailed_reason_field = {"name": "Original Trigger", "value": field.value}
                            break

            else:
                logger.warning(f"Could not fetch original alert message {alert_message_id} in any configured channel for guild {guild.name}.")
        
    elif not moderator.bot:
        if isinstance(moderator, discord.Member):
            whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(guild.id), [])
            if any(role.id in whitelisted_mod_roles for role in moderator.roles):
                is_authorized = True
                authorization_method = "Manual Ban by a whitelisted Moderator"
            else:
                logger.warning(f"User {user} was banned by {moderator}, but they do not have a whitelisted role.")
                return
        else:
            logger.warning(f"User {user} was banned by {moderator} who is no longer in the server. Cannot verify roles.")
            return
    else:
        logger.info(f"Ban of {user} in {guild.name} was by an unauthorized bot ({moderator.name}).")
        return
    
    if not is_authorized:
        logger.warning(f"Ban of {user} by {moderator} did not pass authorization checks.")
        return
    
    guild_id_str = str(guild.id)
    if guild_id_str not in stats: stats[guild_id_str] = {}
    stats[guild_id_str]["bans_initiated_lifetime"] = stats[guild_id_str].get("bans_initiated_lifetime", 0) + 1
    if "monthly_initiated" not in stats[guild_id_str]: stats[guild_id_str]["monthly_initiated"] = {}
    stats[guild_id_str]["monthly_initiated"][current_month_key] = stats[guild_id_str]["monthly_initiated"].get(current_month_key, 0) + 1

    display_reason = ""
    if not detailed_reason_field:
        default_reason = config.get("manual_ban_default_reason", "Scam")
        display_reason = ban_reason if ban_reason != "No reason provided." else default_reason
        detailed_reason_field = {"name": "Ban Reason", "value": f"```{display_reason[:1000]}```"}
    else:
        display_reason = detailed_reason_field["value"].strip("`")
    
    fed_bans = await load_fed_bans()
    
    fed_bans[str(user.id)] = {
        "username_at_ban": user.name,
        "ban_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "origin_guild_id": guild.id,
        "origin_guild_name": guild.name,
        "reason": display_reason,
        "initiating_moderator_id": moderator.id
    }
    await save_fed_bans(fed_bans)

    if "global" not in stats: stats["global"] = {}
    stats["global"]["total_federated_actions_lifetime"] = stats["global"].get("total_federated_actions_lifetime", 0) + 1
    
    log_channel_id = config.get("log_channel_id")
    log_channel = bot.get_channel(log_channel_id) if log_channel_id else None
    if log_channel:
        embed = discord.Embed(title="🛡️ Federated Ban", description=f"**User:** {user.name} ({user.mention}, `{user.id}`)\n**Origin:** {guild.name}\n**Authorization:** {authorization_method}\n**Reason:** ```{ban_reason[:1000]}```", color=discord.Color.brand_red(), timestamp=datetime.now(timezone.utc))
        embed.set_author(name=user.name, icon_url=user.display_avatar.url)
        await log_channel.send(embed=embed)
        
    logger.info(f"INITIATING FEDERATED BAN for {user} from origin {guild.name}.")
    
    if authorization_method == "Manual Ban by a whitelisted Moderator":
        origin_mod_channel_id = config.get("mod_alert_channels", {}).get(str(guild.id))
        if origin_mod_channel_id:
            origin_mod_channel = guild.get_channel(origin_mod_channel_id)
            if not origin_mod_channel:
                try:
                    origin_mod_channel = await bot.fetch_channel(origin_mod_channel_id)
                except (discord.NotFound, discord.Forbidden):
                    logger.warning(f"Could not find or access origin mod channel {origin_mod_channel_id}")
                    origin_mod_channel = None

            if origin_mod_channel:
                embed_desc = (
                    f"The manual ban for **{user.name}** (`{user.id}`) has been broadcast to all federated servers.\n\n"
                    f"**Reason:**\n```{display_reason[:1000]}```"
                )
                origin_alert_embed = discord.Embed(
                    title="✅ Manual Ban Propagated", 
                    description=embed_desc, 
                    color=discord.Color.blue(), 
                    timestamp=datetime.now(timezone.utc)
                )
                allowed_mentions = discord.AllowedMentions(users=[user])
                await origin_mod_channel.send(embed=origin_alert_embed, allowed_mentions=allowed_mentions)

    for guild_id in config.get("federated_guild_ids", []):
        if guild_id == guild.id: continue
        target_guild = bot.get_guild(guild_id)
        if not target_guild: continue
        bot_member = target_guild.me
        if not bot_member.guild_permissions.ban_members:
            logger.warning(f"Missing 'Ban Members' permission in {target_guild.name}. Skipping federated ban.")
            if log_channel: await log_channel.send(f"⚠️ Failed to ban `{user}` in `{target_guild.name}` - Missing Permissions.")
            continue
        try:
            await target_guild.fetch_ban(user)
            logger.info(f"User {user} is already banned in {target_guild.name}.")
        except discord.NotFound:
            try:
                fed_reason = f"Federated ban from {guild.name}. Reason: {ban_reason}"

                delete_days = get_delete_days_for_guild(target_guild)
                delete_seconds = delete_days * 86400 # 24h * 60m * 60s

                await target_guild.ban(user, reason=fed_reason[:512], delete_message_seconds=delete_seconds)
                logger.info(f"SUCCESS: Banned {user} from {target_guild.name}.")
                if log_channel: await log_channel.send(f"✅ Banned `{user}` in `{target_guild.name}`.")
                
                target_guild_id_str = str(target_guild.id)
                if target_guild_id_str not in stats: stats[target_guild_id_str] = {}
                stats[target_guild_id_str]["bans_received_lifetime"] = stats[target_guild_id_str].get("bans_received_lifetime", 0) + 1
                if "monthly_received" not in stats[target_guild_id_str]: stats[target_guild_id_str]["monthly_received"] = {}
                stats[target_guild_id_str]["monthly_received"][current_month_key] = stats[target_guild_id_str]["monthly_received"].get(current_month_key, 0) + 1

                mod_channel_id = config.get("mod_alert_channels", {}).get(str(target_guild.id))
                if mod_channel_id:
                    mod_channel = target_guild.get_channel(mod_channel_id)
                    if not mod_channel:
                        try:
                            mod_channel = await bot.fetch_channel(mod_channel_id)
                        except (discord.NotFound, discord.Forbidden):
                            logger.warning(f"Could not find or access mod channel {mod_channel_id} in {target_guild.name}")
                            mod_channel = None

                    if mod_channel:
                        alert_embed = discord.Embed(
                            title="🛡️ Federated Ban Received",
                            description=f"**User:** {user.name} ({user.mention}, `{user.id}`)\n"
                                        f"**Action:** Automatically banned from this server.\n"
                                        f"**Origin:** **{guild.name}**",
                            color=discord.Color.dark_red(),
                            timestamp=datetime.now(timezone.utc)
                        )
                        alert_embed.add_field(
                            name=detailed_reason_field["name"],
                            value=detailed_reason_field["value"],
                            inline=False
                        )
                        alert_embed.set_author(name=user.name, icon_url=user.display_avatar.url)
                        
                        view = FederatedAlertView(banned_user_id=user.id)
                        
                        allowed_mentions = discord.AllowedMentions(users=[user])
                        await mod_channel.send(embed=alert_embed, view=view, allowed_mentions=allowed_mentions)

            except Exception as e:
                logger.error(f"Error during federated ban propagation to {target_guild.name}: {e}", exc_info=True)
                if log_channel: await log_channel.send(f"❌ Failed to ban `{user}` in `{target_guild.name}` - Error: `{e}`")
    
    await save_fed_stats(stats)

async def propagate_ban(origin_guild: discord.Guild, user_to_ban: discord.User, moderator: discord.User, reason: str):
    """Handles the logic of propagating a ban to all federated servers."""
    config = bot.config
    stats = await load_fed_stats()
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")
    log_channel_id = config.get("log_channel_id")
    log_channel = bot.get_channel(log_channel_id) if log_channel_id else None

    origin_guild_id_str = str(origin_guild.id)
    if origin_guild_id_str not in stats: stats[origin_guild_id_str] = {}
    stats[origin_guild_id_str]["bans_initiated_lifetime"] = stats[origin_guild_id_str].get("bans_initiated_lifetime", 0) + 1
    if "monthly_initiated" not in stats[origin_guild_id_str]: stats[origin_guild_id_str]["monthly_initiated"] = {}
    stats[origin_guild_id_str]["monthly_initiated"][current_month_key] = stats[origin_guild_id_str]["monthly_initiated"].get(current_month_key, 0) + 1
    
    if "global" not in stats: stats["global"] = {}
    stats["global"]["total_federated_actions_lifetime"] = stats["global"].get("total_federated_actions_lifetime", 0) + 1

    logger.info(f"INITIATING FEDERATED BAN for {user_to_ban.name} from origin {origin_guild.name} by {moderator.name}.")

    fed_bans = await load_fed_bans()
    fed_bans[str(user_to_ban.id)] = {
        "username_at_ban": user_to_ban.name,
        "ban_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "origin_guild_id": origin_guild.id,
        "origin_guild_name": origin_guild.name,
        "reason": reason,
        "initiating_moderator_id": moderator.id
    }
    await save_fed_bans(fed_bans)
    
    detailed_reason_field = {"name": "Ban Reason", "value": f"```{reason[:1000]}```"}

    for guild_id in config.get("federated_guild_ids", []):
        target_guild = bot.get_guild(guild_id)
        if not target_guild: continue
        
        if target_guild.id == origin_guild.id:
            try:
                await target_guild.fetch_ban(user_to_ban)
                logger.info(f"User {user_to_ban.name} was already banned in the origin guild {target_guild.name}.")
            except discord.NotFound:
                try:
                    delete_seconds = get_delete_days_for_guild(target_guild) * 86400
                    await target_guild.ban(user_to_ban, reason=f"Proactive ban initiated by {moderator.name}. Reason: {reason}", delete_message_seconds=delete_seconds)
                    logger.info(f"Successfully banned user in origin guild {target_guild.name} as part of proactive ban.")
                except discord.HTTPException as e:
                    if e.code == 50013: # Missing Permissions
                        logger.warning(f"Missing 'Ban Members' permission in origin guild {target_guild.name} for proactive ban.")
                    else:
                        logger.error(f"Failed to ban user in origin guild during proactive ban: {e}")
            continue # Always move to the next server after handling the origin.

        bot_member = target_guild.me
        if not bot_member.guild_permissions.ban_members:
            logger.warning(f"Missing 'Ban Members' permission in {target_guild.name}. Skipping federated ban.")
            if log_channel: await log_channel.send(f"⚠️ Failed to ban `{user_to_ban.name}` in `{target_guild.name}` - Missing Permissions.")
            continue
            
        try:
            await target_guild.fetch_ban(user_to_ban)
            logger.info(f"User {user_to_ban.name} is already banned in {target_guild.name}.")
        except discord.NotFound:
            try:
                fed_reason = f"Federated ban from {origin_guild.name}. Reason: {reason}"
                delete_days = get_delete_days_for_guild(target_guild)
                delete_seconds = delete_days * 86400

                await target_guild.ban(user_to_ban, reason=fed_reason[:512], delete_message_seconds=delete_seconds)
                logger.info(f"SUCCESS: Banned {user_to_ban.name} from {target_guild.name}.")
                if log_channel: await log_channel.send(f"✅ Banned `{user_to_ban.name}` in `{target_guild.name}`.")
                
                target_guild_id_str = str(target_guild.id)
                if target_guild_id_str not in stats: stats[target_guild_id_str] = {}
                stats[target_guild_id_str]["bans_received_lifetime"] = stats[target_guild_id_str].get("bans_received_lifetime", 0) + 1
                if "monthly_received" not in stats[target_guild_id_str]: stats[target_guild_id_str]["monthly_received"] = {}
                stats[target_guild_id_str]["monthly_received"][current_month_key] = stats[target_guild_id_str]["monthly_received"].get(current_month_key, 0) + 1

                mod_channel_id = config.get("mod_alert_channels", {}).get(str(target_guild.id))
                if mod_channel_id:
                    mod_channel = target_guild.get_channel(mod_channel_id)
                    if not mod_channel:
                        try:
                            mod_channel = await bot.fetch_channel(mod_channel_id)
                        except (discord.NotFound, discord.Forbidden):
                            logger.warning(f"Could not find or access mod channel {mod_channel_id} in {target_guild.name}")
                            mod_channel = None

                    if mod_channel:
                        alert_embed = discord.Embed(
                            title="🛡️ Federated Ban Received",
                            description=f"**User:** {user_to_ban.name} ({user_to_ban.mention}, `{user_to_ban.id}`)\n"
                                        f"**Action:** Automatically banned from this server.\n"
                                        f"**Origin:** **{origin_guild.name}**",
                            color=discord.Color.dark_red(),
                            timestamp=datetime.now(timezone.utc)
                        )
                        alert_embed.add_field(
                            name=detailed_reason_field["name"],
                            value=detailed_reason_field["value"],
                            inline=False
                        )
                        alert_embed.set_author(name=user_to_ban.name, icon_url=user_to_ban.display_avatar.url)
                        
                        view = FederatedAlertView(banned_user_id=user_to_ban.id)
                        
                        allowed_mentions = discord.AllowedMentions(users=[user_to_ban])
                        await mod_channel.send(embed=alert_embed, view=view, allowed_mentions=allowed_mentions)

            except Exception as e:
                logger.error(f"Error during federated ban propagation to {target_guild.name}: {e}", exc_info=True)
                if log_channel: await log_channel.send(f"❌ Failed to ban `{user_to_ban.name}` in `{target_guild.name}` - Error: `{e}`")
    
    await save_fed_stats(stats)

@bot.event
async def on_member_unban(guild: discord.Guild, user: discord.User):
    config = bot.config
    if guild.id not in config.get("federated_guild_ids", []): return

    moderator, unban_reason = None, "No reason provided."
    await asyncio.sleep(2)
    try:
        async for entry in guild.audit_logs(action=discord.AuditLogAction.unban, limit=5):
            if entry.target.id == user.id:
                moderator, unban_reason = entry.user, entry.reason or "No reason provided."
                break
    except Exception as e:
        logger.error(f"Error fetching audit logs for unban in {guild.name}: {e}", exc_info=True)
        return

    if not moderator:
        logger.info(f"Unban of {user} in {guild.name} could not be attributed.")
        return

    is_authorized, authorization_method = False, "Unknown"
    should_propagate = False # Default to local action

    if unban_reason.startswith("[Local Action]"):
        logger.info(f"Processing local-only unban for {user} in {guild.name}.")
        is_authorized = True
        authorization_method = "Local Unban via Federated Alert"
        should_propagate = False
    elif moderator.id == bot.user.id:
        if unban_reason.startswith("[Federated Action]"):
            is_authorized, authorization_method = True, "Authorized via Bot Alert"
            should_propagate = True # This is a global action
        else:
            logger.info(f"Ignoring federated unban echo for {user} in {guild.name}.")
            return
    elif not moderator.bot:
        if isinstance(moderator, discord.Member):
            whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(guild.id), [])
            if any(role.id in whitelisted_mod_roles for role in moderator.roles):
                is_authorized, authorization_method = True, "Manual Unban by a whitelisted Moderator"
                should_propagate = True
            else:
                logger.warning(f"User {user} was unbanned by {moderator}, but they do not have a whitelisted role.")
                return
        else:
            logger.warning(f"User {user} was unbanned by {moderator} who is no longer in the server. Cannot verify roles.")
            return
    else:
        logger.info(f"Unban of {user} in {guild.name} was by an unauthorized bot ({moderator.name}).")
        return

    if not is_authorized:
        logger.warning(f"Unban of {user} by {moderator} did not pass authorization checks.")
        return

    stats = await load_fed_stats()
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")

    if "global" not in stats: stats["global"] = {}
    stats["global"]["total_federated_actions_lifetime"] = max(0, stats["global"].get("total_federated_actions_lifetime", 0) - 1)
    
    # Only enter the propagation block if it's a global action.
    if should_propagate:
        fed_bans = await load_fed_bans()
        user_id_str = str(user.id)
        if user_id_str in fed_bans:
            del fed_bans[user_id_str]
            await save_fed_bans(fed_bans)
            logger.info(f"Removed user {user.id} from the master federated ban list.")
            
        log_channel_id = config.get("log_channel_id")
        log_channel = bot.get_channel(log_channel_id) if log_channel_id else None
        if log_channel:
            embed = discord.Embed(title="ℹ️ Federated Unban", description=f"**User:** {user.name} ({user.mention}, `{user.id}`)\n**Origin:** {guild.name}\n**Authorization:** {authorization_method}", color=discord.Color.blue(), timestamp=datetime.now(timezone.utc))
            embed.set_author(name=user.name, icon_url=user.display_avatar.url)
            await log_channel.send(embed=embed)

        logger.info(f"INITIATING FEDERATED (GLOBAL) UNBAN for {user} from origin {guild.name}.")
        origin_mod_channel_id = config.get("mod_alert_channels", {}).get(str(guild.id))
        if origin_mod_channel_id:
            origin_mod_channel = guild.get_channel(origin_mod_channel_id)
            if not origin_mod_channel:
                try:
                    origin_mod_channel = await bot.fetch_channel(origin_mod_channel_id)
                except (discord.NotFound, discord.Forbidden):
                    logger.warning(f"Could not find or access origin mod channel {origin_mod_channel_id}")
                    origin_mod_channel = None

            if origin_mod_channel:
                embed_desc = (
                    f"The unban for **{user.name}** (`{user.id}`) has been broadcast to all federated servers.\n\n"
                    f"**Reason:**\n```{unban_reason[:1000]}```"
                )
                origin_alert_embed = discord.Embed(
                    title="✅ Global Unban Propagated", 
                    description=embed_desc, 
                    color=discord.Color.light_grey(), 
                    timestamp=datetime.now(timezone.utc)
                )
                await origin_mod_channel.send(embed=origin_alert_embed)

        for guild_id in config.get("federated_guild_ids", []):
            if guild_id == guild.id: continue
            target_guild = bot.get_guild(guild_id)
            if not target_guild: continue
            try:
                await target_guild.fetch_ban(user)
                try:
                    await target_guild.unban(user, reason=f"Federated unban from {guild.name}.")
                    logger.info(f"SUCCESS: Unbanned {user} from {target_guild.name}.")
                    if log_channel: await log_channel.send(f"✅ Unbanned `{user}` in `{target_guild.name}`.")
                    
                    target_guild_id_str = str(target_guild.id)
                    if target_guild_id_str in stats:
                        stats[target_guild_id_str]["bans_received_lifetime"] = max(0, stats[target_guild_id_str].get("bans_received_lifetime", 0) - 1)
                        if "monthly_received" in stats[target_guild_id_str] and current_month_key in stats[target_guild_id_str]["monthly_received"]:
                            stats[target_guild_id_str]["monthly_received"][current_month_key] = max(0, stats[target_guild_id_str]["monthly_received"].get(current_month_key, 0) - 1)
                    
                    mod_channel_id = config.get("mod_alert_channels", {}).get(str(target_guild.id))
                    if mod_channel_id:
                        mod_channel = target_guild.get_channel(mod_channel_id)
                        if not mod_channel:
                            try:
                                mod_channel = await bot.fetch_channel(mod_channel_id)
                            except (discord.NotFound, discord.Forbidden):
                                logger.warning(f"Could not find or access mod channel {mod_channel_id} in {target_guild.name}")
                                mod_channel = None

                        if mod_channel:
                            alert_embed = discord.Embed(
                                title="ℹ️ Federated Unban Received", 
                                description=f"**User:** {user.name} ({user.mention}, `{user.id}`)\n"
                                            f"**Action:** Automatically unbanned from this server.\n"
                                            f"**Origin:** **{guild.name}**", 
                                color=discord.Color.green(), 
                                timestamp=datetime.now(timezone.utc)
                            )
                            alert_embed.add_field(
                                name="Reason from Origin Server",
                                value=f"```{unban_reason[:1000]}```",
                                inline=False
                            )
                            alert_embed.set_author(name=user.name, icon_url=user.display_avatar.url)
                            
                            allowed_mentions = discord.AllowedMentions(users=[user])
                            await mod_channel.send(embed=alert_embed, allowed_mentions=allowed_mentions)
                except Exception as e:
                    logger.error(f"Error during federated unban propagation to {target_guild.name}: {e}", exc_info=True)
                    if log_channel: await log_channel.send(f"❌ Failed to unban `{user}` in `{target_guild.name}` - Error: `{e}`")
            except discord.NotFound:
                logger.info(f"User {user} was not banned in {target_guild.name}. No unban action needed.")
                continue
    
    await save_fed_stats(stats)


# --- SCREENING HELPERS ---
def get_timeout_minutes_for_guild(guild: discord.Guild) -> int:
    """Gets the configured timeout duration in minutes for a specific guild."""
    config = bot.config
    per_guild_settings = config.get("timeout_duration_minutes_per_guild", {})
    guild_id_str = str(guild.id)
    if guild_id_str in per_guild_settings:
        return per_guild_settings[guild_id_str]
    
    return config.get("timeout_duration_minutes_default", 10)

def get_delete_days_for_guild(guild: discord.Guild) -> int:
    """Gets the configured message deletion days for a specific guild."""
    config = bot.config
    per_guild_settings = config.get("delete_messages_on_ban_days_per_guild", {})
    guild_id_str = str(guild.id)
    if guild_id_str in per_guild_settings:
        return per_guild_settings[guild_id_str]
    
    return config.get("delete_messages_on_ban_days_default", 1)

def check_text_for_keywords(text_to_check: str, ruleset: dict) -> list:
    """
    Checks a given string against a specific ruleset (e.g., username rules or bio rules).
    """
    if not text_to_check or not ruleset:
        return []

    triggered = []
    normalized_text = unidecode(text_to_check).lower()

    # Username checks (substring and smart)
    for keyword in ruleset.get("substring", []):
        if keyword.lower() in normalized_text:
            triggered.append(keyword)
    for keyword in ruleset.get("smart", []):
        pattern = r'(?<![a-z])' + re.escape(keyword.lower()) + r'(?![a-z])'
        if re.search(pattern, normalized_text):
            triggered.append(keyword)

    # Bio/Message checks (simple and regex)
    for keyword in ruleset.get("simple_keywords", []):
        pattern = r'(?<![a-z])' + re.escape(keyword.lower()) + r'(?![a-z])'
        if re.search(pattern, normalized_text):
            triggered.append(keyword)
    
    texts_to_scan_regex = {text_to_check, normalized_text}
    for pattern in ruleset.get("regex_patterns", []):
        try:
            for txt in texts_to_scan_regex:
                if re.search(pattern, txt, re.IGNORECASE):
                    if "Matched Regex Pattern" not in triggered:
                        triggered.append("Matched Regex Pattern")
                    break
        except re.error as e:
            logger.warning(f"Invalid regex pattern in {KEYWORDS_FILE}: '{pattern}' - {e}")
            continue
            
    return list(set(triggered))


# --- SCREENING ---
async def screen_member(member: discord.Member, config: dict, keywords_data: dict) -> dict:
    """
    Performs the complete screening process for a single member and returns the findings.
    Does NOT perform any actions (timeout, send message).
    Returns a dictionary with flag status, embed, and timeout reason.
    """
    # --- Banned Elsewhere Check ---
    federated_guild_ids = config.get("federated_guild_ids", [])
    found_bans = []
    for other_guild_id in federated_guild_ids:
        if other_guild_id == member.guild.id: continue
        other_guild = bot.get_guild(other_guild_id)
        if not other_guild: continue
        try:
            ban_entry = await other_guild.fetch_ban(member)
            if ban_entry:
                found_bans.append({"guild_name": other_guild.name, "reason": ban_entry.reason or "No reason provided."})
        except discord.NotFound:
            continue
        except Exception as e:
            logger.error(f"Error checking ban status for {member.name} in {other_guild.name}: {e}")

    if found_bans:
        banned_in_servers = ", ".join([ban['guild_name'] for ban in found_bans])
        timeout_reason = f"Flagged on join: User is banned in partner server(s): {banned_in_servers}."
        embed = discord.Embed(title="🚨 User Banned Elsewhere", description=f"**User:** {member.mention} (`{member.id}`)\nThis user is already banned in **{len(found_bans)}** other federated server(s).", color=discord.Color.red(), timestamp=datetime.now(timezone.utc))
        embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
        for ban in found_bans:
            embed.add_field(name=f"Banned In: {ban['guild_name']}", value=f"```{ban['reason'][:1000]}```", inline=False)
        embed.add_field(name="Status", value="User timed out. Awaiting review...", inline=True)
        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}

    # --- Keyword Screening (Name and Bio) ---
    user_profile = member
    bio = ""
    try:
        full_profile = await bot.fetch_user(member.id)
        user_profile = full_profile
        bio = getattr(full_profile, 'bio', "")
    except discord.NotFound:
        logger.warning(f"Could not fetch profile for {member.name} ({member.id}) during screening, user may no longer exist. Proceeding without bio check.")
    except Exception as e:
        logger.error(f"Could not fetch profile for {member.name} ({member.id}) to get bio. Proceeding without it. Error: {e}")

    triggered_keywords = []
    name_text = f"{user_profile.name} {member.nick or ''}"
    
    local_rules = keywords_data.get("per_server_keywords", {}).get(str(member.guild.id), {})
    global_rules = keywords_data.get("global_keywords", {})

    triggered_keywords.extend(check_text_for_keywords(name_text, local_rules.get("username_keywords", {})))
    triggered_keywords.extend(check_text_for_keywords(name_text, global_rules.get("username_keywords", {})))
    if bio:
        triggered_keywords.extend(check_text_for_keywords(bio, local_rules.get("bio_and_message_keywords", {})))
        triggered_keywords.extend(check_text_for_keywords(bio, global_rules.get("bio_and_message_keywords", {})))

    triggered_keywords = list(set(triggered_keywords))

    if triggered_keywords:
        timeout_reason = "Flagged by keyword screening."
        embed = discord.Embed(title="🚨 Flagged User", description=f"{member.mention} (`{member.id}`)", color=discord.Color.orange(), timestamp=datetime.now(timezone.utc))
        embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
        if bio:
            embed.add_field(name="📝 Bio", value=bio[:1024], inline=False)
        embed.add_field(name="🚩 Trigger", value=f"`{', '.join(triggered_keywords)}`", inline=True)
        embed.add_field(name="Status", value="User timed out. Awaiting review...", inline=True)
        embed.add_field(name="Account Age", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
        return {"flagged": True, "embed": embed, "timeout_reason": "Flagged by keyword screening."}

    return {"flagged": False}

async def screen_message(message: discord.Message, config: dict, keywords_data: dict) -> dict:
    """
    Performs the screening process for a single message and returns the findings.
    Does NOT perform any actions (delete, timeout, send message).
    """
    if not keywords_data: return {"flagged": False}

    triggered_keywords = []
    local_rules = keywords_data.get("per_server_keywords", {}).get(str(message.guild.id), {})
    global_rules = keywords_data.get("global_keywords", {})

    triggered_keywords.extend(check_text_for_keywords(message.content, local_rules.get("bio_and_message_keywords", {})))
    triggered_keywords.extend(check_text_for_keywords(message.content, global_rules.get("bio_and_message_keywords", {})))
    triggered_keywords = list(set(triggered_keywords))

    if triggered_keywords:
        embed = discord.Embed(
            title="🚨 Flagged Message",
            description=f"**User:** {message.author.mention} (`{message.author.id}`)\n"
                        f"**Channel:** {message.channel.mention}",
            color=discord.Color.dark_red(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=f"{message.author.name}", icon_url=message.author.display_avatar.url)
        embed.add_field(name="📝 Flagged Message", value=f"```{message.content[:1000]}```", inline=False)
        embed.add_field(name="🚩 Trigger", value=f"`{', '.join(triggered_keywords)}`", inline=True)
        embed.add_field(name="Status", value="Message deleted. User timed out. Awaiting review...", inline=True)
        embed.add_field(name="Account Age", value=f"<t:{int(message.author.created_at.timestamp())}:R>", inline=True)
        
        timeout_reason = f"Flagged message. Triggered by: {', '.join(triggered_keywords)}"
        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}
        
    return {"flagged": False}

async def screen_bio(member: discord.Member, config: dict, keywords_data: dict) -> dict:
    """
    Performs the screening process for a member's bio and returns the findings.
    Optimized to avoid API calls if bio is already cached on the member object.
    """
    if not keywords_data: return {"flagged": False}

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

    triggered_keywords.extend(check_text_for_keywords(bio, local_rules.get("bio_and_message_keywords", {})))
    triggered_keywords.extend(check_text_for_keywords(bio, global_rules.get("bio_and_message_keywords", {})))
    triggered_keywords = list(set(triggered_keywords))

    if triggered_keywords:
        embed = discord.Embed(
            title="🚨 Flagged User Bio",
            description=f"**User:** {member.mention} (`{member.id}`)",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
        embed.add_field(name="📝 Flagged Bio", value=f"```{bio[:1000]}```", inline=False)
        embed.add_field(name="🚩 Trigger", value=f"`{', '.join(triggered_keywords)}`", inline=True)
        embed.add_field(name="Status", value="User timed out. Awaiting review...", inline=True)
        embed.add_field(name="Account Age", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
        
        timeout_reason = f"Flagged user bio. Triggered by: {', '.join(triggered_keywords)}"
        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}
        
    return {"flagged": False}

async def run_full_scan(interaction: discord.Interaction):
    config = bot.config
    guild = interaction.guild
    
    results_channel_id = config.get("mod_scan_results_channels", {}).get(str(guild.id))
    results_channel = guild.get_channel(results_channel_id)

    if not results_channel:
        await interaction.followup.send(f"❌ **Scan Aborted:** Scan results channel not configured for {guild.name}.", ephemeral=True)
        if guild.id in active_scans: del active_scans[guild.id]
        return
    if not guild.chunked:
        await guild.chunk()

    keywords_data = await load_keywords()
    if not keywords_data:
        await interaction.followup.send("❌ **Scan Aborted:** Could not load keywords file. Please check logs.", ephemeral=True)
        if guild.id in active_scans: del active_scans[guild.id]
        return
    
    total_members = guild.member_count
    progress_message = None
    checked_count, flagged_count = 0, 0
    update_interval = 100
    
    try:
        progress_message = await interaction.channel.send(f"🔍 Scan initiated. Preparing to scan {total_members} members in **{guild.name}**...")
        logger.info(f"Full member scan initiated by {interaction.user.name} for guild '{guild.name}'.")
        for member in guild.members:
            if asyncio.current_task().cancelled():
                raise asyncio.CancelledError
            checked_count += 1
            if member.bot: continue
            whitelisted_roles = config.get("whitelisted_roles_per_guild", {}).get(str(guild.id), [])
            if any(role.id in whitelisted_roles for role in member.roles):
                continue
            result = await screen_member(member, config, keywords_data)
            
            if result.get("flagged"):
                flagged_count += 1
                try:
                    timeout_minutes = get_timeout_minutes_for_guild(member.guild)
                    await member.timeout(timedelta(minutes=timeout_minutes), reason=result.get("timeout_reason", "Flagged by scan."))
                    view = ScreeningView(flagged_member_id=member.id)
                    embed = result.get("embed")
                    embed.set_footer(text=f"User ID: {member.id}")
                    allowed_mentions = discord.AllowedMentions(users=[member])
                    await results_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)

                except Exception as e:
                    logger.error(f"Failed to take action on scanned member {member.name}: {e}")
            if checked_count % update_interval == 0:
                progress_text = f"Scan in progress... {checked_count}/{total_members} members checked. **{flagged_count}** flagged so far."
                await progress_message.edit(content=f"🔍 {progress_text}")
                logger.info(f"Scan progress for {guild.name}: {progress_text}")
            await asyncio.sleep(0.05)
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
            await progress_message.edit(content=f"❌ **Scan Failed!**\n- An unexpected error occurred. Please check the logs.")
    finally:
        if guild.id in active_scans:
            del active_scans[guild.id]
            logger.info(f"Scan task for guild {guild.id} removed from active tracker.")


# --- OWNER SLASH COMMANDS ---
@bot.tree.command(name="zreloadconfig", description="[Owner Only] Reloads configuration files from disk.")
@is_bot_owner()
async def reloadconfig(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    logger.info(f"OWNER COMMAND: {interaction.user.name} triggered a configuration reload.")
    
    bot.config = load_federation_config()
    
    keywords_data = await load_keywords()
    
    if not bot.config or keywords_data is None:
        await interaction.followup.send("❌ **Failed to reload.** Check logs for errors with config or keyword files.")
        return

    federated_guild_ids = bot.config.get("federated_guild_ids", [])
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
        await save_keywords(keywords_data)

    await interaction.followup.send(
        f"✅ **Configuration reloaded successfully.**\n"
        f"Now managing **{len(federated_guild_ids)}** federated servers.\n"
        f"{'Keyword file was updated with new server entries.' if updated else 'No new servers found to add.'}"
    )

@bot.tree.command(name="zadd-global-name-substring", description="[Owner Only] Adds a SUBSTRING keyword to the GLOBAL list.")
@discord.app_commands.describe(keyword="The keyword to add globally (e.g., 'admin').")
@is_bot_owner()
async def add_global_username_substring(interaction: discord.Interaction, keyword: str):
    await interaction.response.defer(ephemeral=True)
    await add_global_keyword_to_list(interaction, keyword, "username_keywords", "substring")

@bot.tree.command(name="zadd-global-name-smart", description="[Owner Only] Adds a SMART keyword to the GLOBAL list.")
@discord.app_commands.describe(keyword="The keyword to add globally (e.g., 'mod').")
@is_bot_owner()
async def add_global_username_smart(interaction: discord.Interaction, keyword: str):
    await interaction.response.defer(ephemeral=True)
    await add_global_keyword_to_list(interaction, keyword, "username_keywords", "smart")

@bot.tree.command(name="zadd-global-bio-msg-keyword", description="[Owner Only] Adds a BIO keyword to the GLOBAL list.")
@discord.app_commands.describe(keyword="The keyword or phrase to add globally.")
@is_bot_owner()
async def add_global_bio_keyword(interaction: discord.Interaction, keyword: str):
    await interaction.response.defer(ephemeral=True)
    await add_global_keyword_to_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

@bot.tree.command(name="zadd-global-regex", description="[OWNER ONLY] Adds a regex pattern to the GLOBAL list.")
@discord.app_commands.describe(pattern="The exact regex pattern to add globally.")
@is_bot_owner()
async def add_global_regex(interaction: discord.Interaction, pattern: str):
    await interaction.response.defer(ephemeral=True)
    await add_regex_to_list(interaction, pattern, is_global=True)

@bot.tree.command(name="zrm-global-name-substring", description="[Owner Only] Removes a SUBSTRING keyword from the GLOBAL list.")
@discord.app_commands.describe(keyword="The exact keyword to remove globally.")
@is_bot_owner()
async def remove_global_username_substring(interaction: discord.Interaction, keyword: str):
    await interaction.response.defer(ephemeral=True)
    await remove_global_keyword_from_list(interaction, keyword, "username_keywords", "substring")

@bot.tree.command(name="zrm-global-name-smart", description="[Owner Only] Removes a SMART keyword from the GLOBAL list.")
@discord.app_commands.describe(keyword="The exact keyword to remove globally.")
@is_bot_owner()
async def remove_global_username_smart(interaction: discord.Interaction, keyword: str):
    await interaction.response.defer(ephemeral=True)
    await remove_global_keyword_from_list(interaction, keyword, "username_keywords", "smart")

@bot.tree.command(name="zrm-global-bio-msg-keyword", description="[Owner Only] Removes a BIO keyword from the GLOBAL list.")
@discord.app_commands.describe(keyword="The exact keyword to remove globally.")
@is_bot_owner()
async def remove_global_bio_keyword(interaction: discord.Interaction, keyword: str):
    await interaction.response.defer(ephemeral=True)
    await remove_global_keyword_from_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

@bot.tree.command(name="zrm-global-regex-by-id", description="[OWNER ONLY] Removes a regex from the GLOBAL list by its ID.")
@discord.app_commands.describe(index="The numerical ID of the global regex pattern to remove.")
@is_bot_owner()
async def remove_global_regex_by_id(interaction: discord.Interaction, index: int):
    await interaction.response.defer(ephemeral=True)
    await remove_regex_from_list_by_id(interaction, index, is_global=True)

@bot.tree.command(name="admin-backfill-banlist", description="[OWNER ONLY] Populates the master ban list from historical audit logs.")
@is_bot_owner()
async def admin_backfill_banlist(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    progress_message = await interaction.channel.send("🔍 **Phase 1/4: Collecting historical ban data...**")
    await interaction.followup.send("✅ **Starting historical backfill.** This is a complex operation and may take several minutes. Progress is being updated in the channel above.", ephemeral=True)
    
    config = bot.config
    bot_owner_id = config.get("bot_owner_id")
    potential_bans = {}
    unbanned_users = set()

    ninety_days_ago = datetime.now(timezone.utc) - timedelta(days=90)
    
    for guild_id in config.get("federated_guild_ids", []):
        guild = bot.get_guild(guild_id)
        if not guild:
            logger.warning(f"Backfill: Could not find guild {guild_id}, skipping.")
            continue

        await progress_message.edit(content=f"⏳ **Phase 1/4:** Processing ban logs for **{guild.name}**...")
        try:
            async for entry in guild.audit_logs(action=discord.AuditLogAction.ban, after=ninety_days_ago, limit=None):
                moderator = entry.user
                target_user = entry.target

                if target_user.id == bot.user.id or (bot_owner_id and target_user.id == bot_owner_id):
                    continue

                is_authorized = False
                if moderator.id == bot.user.id:
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
                if moderator.id == bot.user.id:
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

    fed_bans = await load_fed_bans()
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
            guild = bot.get_guild(guild_id)
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

    await save_fed_bans(fed_bans)
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

@bot.tree.command(name="zsync", description="[OWNER ONLY] Syncs the command tree with Discord.")
@is_bot_owner()
async def zsync(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    synced = await bot.tree.sync()
    await interaction.followup.send(f"✅ Synced {len(synced)} command(s) globally.")
    logger.info(f"Command tree synced by {interaction.user.name}. Synced {len(synced)} commands.")

    

# --- MODERATOR SLASH COMMANDS ---
@bot.tree.command(name="stats", description="Displays local and federated ban statistics.")
@has_mod_role()
async def stats(interaction: discord.Interaction):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer()
    stats = await load_fed_stats()
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
        
@bot.tree.command(name="list-keywords", description="Lists all active screening keywords for this server.")
@has_mod_role()
async def list_keywords(interaction: discord.Interaction):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)

    keywords_data = await load_keywords()
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

@bot.tree.command(name="add-name-keyword-substring", description="Aggressive name keyword (catches variations: 'admin' in 'daoadmin' 'admin123').")
@has_mod_role()
@discord.app_commands.describe(keyword="Example: 'admin' will match 'listadaoadmin' or 'admin123'.")
async def add_username_keyword_substring(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await add_keyword_to_list(interaction, keyword, "username_keywords", "substring")

@bot.tree.command(name="add-name-keyword-smart", description="Precise name keyword (avoids false positives: 'mod' in 'mod123' not 'modern').")
@has_mod_role()
@discord.app_commands.describe(keyword="Example: 'mod' will match 'mod123' but IGNORE 'modern'.")
async def add_username_keyword_smart(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await add_keyword_to_list(interaction, keyword, "username_keywords", "smart")

@bot.tree.command(name="add-bio-msg-keyword", description="Keyword for screening user bios and messages.")
@has_mod_role()
@discord.app_commands.describe(keyword="The keyword or phrase to add (e.g., 'dm me for help').")
async def add_bio_keyword(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await add_keyword_to_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

@bot.tree.command(name="test-regex", description="Tests a regex pattern against sample text using a pop-up form.")
@has_mod_role()
@discord.app_commands.describe(pattern="The regex pattern to test. Remember to escape special characters (e.g., '\\.').")
async def test_regex(interaction: discord.Interaction, pattern: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config):
        return

    try:
        compiled_regex = re.compile(pattern, re.IGNORECASE)
    except re.error as e:
        await interaction.response.send_message(f"❌ **Invalid Regex:** That pattern is not valid.\n`{e}`", ephemeral=True)
        return

    modal = RegexTestModal(pattern=pattern, compiled_regex=compiled_regex)
    await interaction.response.send_modal(modal)

@bot.tree.command(name="add-regex", description="Adds a regex pattern to this server's local list. Try /test-regex first!")
@has_mod_role()
@discord.app_commands.describe(pattern="The exact regex pattern. Use standard regex escaping (e.g., '\\.' for a dot, '\\s' for whitespace).")
async def add_local_regex(interaction: discord.Interaction, pattern: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await add_regex_to_list(interaction, pattern, is_global=False)

@bot.tree.command(name="rm-name-keyword-substring", description="Removes a SUBSTRING keyword from this server's local list.")
@has_mod_role()
@discord.app_commands.describe(keyword="The exact keyword to remove.")
async def remove_username_keyword_substring(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await remove_keyword_from_list(interaction, keyword, "username_keywords", "substring")

@bot.tree.command(name="rm-name-keyword-smart", description="Removes a SMART keyword from this server's local list.")
@has_mod_role()
@discord.app_commands.describe(keyword="The exact keyword to remove.")
async def remove_username_keyword_smart(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await remove_keyword_from_list(interaction, keyword, "username_keywords", "smart")

@bot.tree.command(name="rm-bio-msg-keyword", description="Removes a keyword from this server's local bio/message list.")
@has_mod_role()
@discord.app_commands.describe(keyword="The exact keyword or phrase to remove.")
async def remove_bio_keyword(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await remove_keyword_from_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

@bot.tree.command(name="rm-regex-by-id", description="Removes a regex from the local list by its ID from /list-keywords.")
@has_mod_role()
@discord.app_commands.describe(index="The numerical ID of the regex pattern to remove.")
async def remove_local_regex_by_id(interaction: discord.Interaction, index: int):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await remove_regex_from_list_by_id(interaction, index, is_global=False)

@bot.tree.command(name="scanallmembers", description="Retroactively scans all server members against the screening list.")
@has_mod_role()
async def scanallmembers(interaction: discord.Interaction):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    if interaction.guild.id in active_scans:
        await interaction.response.send_message("❌ A scan is already in progress for this server.", ephemeral=True)
        return
    member_count = interaction.guild.member_count
    view = ConfirmScanView(author=interaction.user)
    await interaction.response.send_message(f"⚠️ **Are you sure?**\nThis will scan all **{member_count}** members...", view=view, ephemeral=True)
    await view.wait()
    if view.value is True:
        scan_task = bot.loop.create_task(run_full_scan(interaction))
        active_scans[interaction.guild.id] = scan_task
    else:
        await interaction.followup.send("Scan cancelled or timed out.", ephemeral=True)

@bot.tree.command(name="stopscan", description="Stops an ongoing member scan for this server.")
@has_mod_role()
async def stopscan(interaction: discord.Interaction):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    guild_id = interaction.guild.id
    if guild_id in active_scans:
        active_scans[guild_id].cancel()
        logger.info(f"Moderator {interaction.user.name} stopped the scan for guild {guild_id}.")
        await interaction.response.send_message("✅ Scan cancellation requested.", ephemeral=True)
    else:
        await interaction.response.send_message("ℹ️ No scan is currently in progress.", ephemeral=True)

@bot.tree.command(name="contact-maintainer", description="Send a message to the bot maintainer for requests, feedback, or issues.")
@has_mod_role()
@discord.app_commands.describe(message="Your message, feedback, or request for the bot maintainer.")
async def contact_maintainer(interaction: discord.Interaction, message: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config):
        return

    log_channel_id = config.get("log_channel_id")
    if not log_channel_id:
        await interaction.response.send_message("❌ The bot's log channel is not configured. Please contact the owner directly.", ephemeral=True)
        return

    log_channel = bot.get_channel(log_channel_id)
    if not log_channel:
        try:
            log_channel = await bot.fetch_channel(log_channel_id)
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
        logger.info(f"Moderator {interaction.user.name} from {interaction.guild.name} sent an admin contact request.")
    except Exception as e:
        await interaction.response.send_message("❌ An error occurred while trying to send your message. Please try again later.", ephemeral=True)
        logger.error(f"Failed to send contact message to log channel: {e}", exc_info=True)

@bot.tree.command(name="onboard-server", description="Onboards a new server by syncing the federated ban list.")
@has_mod_role()
async def onboard_server(interaction: discord.Interaction):
    config = bot.config

    sync_status = await load_sync_status()
    if interaction.guild.id in sync_status["synced_guild_ids"]:
        await interaction.response.send_message(
            "❌ **Action Prohibited:** This server has already been onboarded. "
            "Running this command again could incorrectly re-ban users who were locally unbanned. "
            "If a full re-sync is required, please contact the bot administrator.",
            ephemeral=True
        )
        return

    fed_bans = await load_fed_bans()
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

@bot.tree.command(name="global-ban", description="Proactively bans a user by ID across all federated servers.")
@has_mod_role()
@discord.app_commands.describe(user_id="The Discord User ID of the person to ban.", reason="The reason for the ban. This will be shown in all federated alerts.")
async def global_ban(interaction: discord.Interaction, user_id: str, reason: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config):
        return

    if not user_id.isdigit():
        await interaction.response.send_message("❌ **Invalid ID:** Please provide a valid Discord User ID (numbers only).", ephemeral=True)
        return
    
    target_user_id = int(user_id)

    if target_user_id == interaction.user.id:
        await interaction.response.send_message("❌ You cannot ban yourself.", ephemeral=True)
        return
    
    if target_user_id == bot.user.id:
        await interaction.response.send_message("❌ I cannot ban myself.", ephemeral=True)
        return

    try:
        user_to_ban = await bot.fetch_user(target_user_id)
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

    if await is_federated_moderator(user_to_ban.id):
        await interaction.response.send_message("❌ **Action Prohibited:** You cannot target another federated moderator. This action must be performed manually by the bot owner if necessary.", ephemeral=True)
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

@bot.tree.command(name="lookup", description="Looks up a user ID or username in the federated ban list.")
@discord.app_commands.describe(query="The User ID or username to search for.")
@has_mod_role()
async def lookup(interaction: discord.Interaction, query: str):
    await interaction.response.defer()

    fed_bans = await load_fed_bans()
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



# --- COMMAND HELPERS ---
async def add_global_keyword_to_list(interaction: discord.Interaction, keyword: str, primary_key: str, secondary_key: str = None):
    """A helper to add a keyword to a specific GLOBAL list in the keywords.json file."""
    keyword = keyword.lower().strip()
    if not keyword:
        await interaction.followup.send("❌ Keyword cannot be empty.")
        return

    keywords_data = await load_keywords()
    if keywords_data is None:
        await interaction.followup.send("❌ Could not load the keywords file. Please check the bot's logs.")
        return

    global_keywords = keywords_data.get("global_keywords", {})
    
    target_list = None
    if secondary_key:
        if primary_key not in global_keywords: global_keywords[primary_key] = {}
        if secondary_key not in global_keywords[primary_key]: global_keywords[primary_key][secondary_key] = []
        target_list = global_keywords[primary_key][secondary_key]
    else:
        if primary_key not in global_keywords: global_keywords[primary_key] = []
        target_list = global_keywords[primary_key]

    if keyword in target_list:
        await interaction.followup.send(f"⚠️ The keyword '{keyword}' is already in the global list.")
        return

    target_list.append(keyword)
    keywords_data["global_keywords"] = global_keywords
    await save_keywords(keywords_data)

    logger.info(f"OWNER {interaction.user.name} added global keyword '{keyword}'.")
    await interaction.followup.send(f"✅ Keyword '{keyword}' has been successfully added to the GLOBAL list.")

async def remove_global_keyword_from_list(interaction: discord.Interaction, keyword: str, primary_key: str, secondary_key: str = None):
    """A helper to remove a keyword from a specific GLOBAL list in the keywords.json file."""
    keyword = keyword.lower().strip()
    if not keyword:
        await interaction.followup.send("❌ Keyword cannot be empty.")
        return

    keywords_data = await load_keywords()
    global_keywords = keywords_data.get("global_keywords", {})
    
    target_list = None
    if secondary_key:
        target_list = global_keywords.get(primary_key, {}).get(secondary_key, [])
    else:
        target_list = global_keywords.get(primary_key, [])

    if keyword in target_list:
        target_list.remove(keyword)
        await save_keywords(keywords_data)
        logger.info(f"OWNER {interaction.user.name} removed global keyword '{keyword}'.")
        await interaction.followup.send(f"✅ Keyword '{keyword}' has been removed from the GLOBAL list.")
    else:
        await interaction.followup.send(f"❌ Keyword '{keyword}' was not found in the GLOBAL list.")

def format_keyword_list(ruleset: dict, list_title: str) -> str:
    """Helper to format a keyword set into a readable string for Discord."""
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
    """A generic helper to add a keyword to a specific LOCAL list in the keywords.json file."""
    keyword = keyword.lower().strip()
    if not keyword:
        await interaction.followup.send("❌ Keyword cannot be empty.")
        return

    keywords_data = await load_keywords()
    guild_id_str = str(interaction.guild.id)

    if "per_server_keywords" not in keywords_data:
        keywords_data["per_server_keywords"] = {}
    if guild_id_str not in keywords_data["per_server_keywords"]:
        keywords_data["per_server_keywords"][guild_id_str] = {
            "username_keywords": {"substring": [], "smart": []},
            "bio_and_message_keywords": {"simple_keywords": [], "regex_patterns": []}
        }
    
    server_keywords = keywords_data["per_server_keywords"][guild_id_str]
    
    target_list = None
    if secondary_key:
        if primary_key not in server_keywords: server_keywords[primary_key] = {}
        if secondary_key not in server_keywords[primary_key]: server_keywords[primary_key][secondary_key] = []
        target_list = server_keywords[primary_key][secondary_key]
    else:
        if primary_key not in server_keywords: server_keywords[primary_key] = []
        target_list = server_keywords[primary_key]

    if keyword in target_list:
        await interaction.followup.send(f"⚠️ The keyword '{keyword}' is already in this server's local list.")
        return

    target_list.append(keyword)
    await save_keywords(keywords_data)

    logger.info(f"Moderator {interaction.user.name} in {interaction.guild.name} added keyword '{keyword}'.")
    await interaction.followup.send(f"✅ Keyword '{keyword}' has been added to this server's local list.")

async def add_regex_to_list(interaction: discord.Interaction, pattern: str, is_global: bool):
    """A helper to add a regex pattern to the appropriate list after validating it."""
    try:
        re.compile(pattern)
    except re.error as e:
        logger.warning(f"Moderator {interaction.user.name} tried to add an invalid regex: {pattern}. Error: {e}")
        await interaction.followup.send(f"❌ **Invalid Regex:** That pattern is not valid.\n`{e}`\nPlease test your pattern with `/test-regex` first.")
        return

    keywords_data = await load_keywords()
    target_list = None
    list_name = ""

    if is_global:
        if "global_keywords" not in keywords_data: keywords_data["global_keywords"] = {}
        if "bio_and_message_keywords" not in keywords_data["global_keywords"]: keywords_data["global_keywords"]["bio_and_message_keywords"] = {}
        if "regex_patterns" not in keywords_data["global_keywords"]["bio_and_message_keywords"]: keywords_data["global_keywords"]["bio_and_message_keywords"]["regex_patterns"] = []
        target_list = keywords_data["global_keywords"]["bio_and_message_keywords"]["regex_patterns"]
        list_name = "GLOBAL"
    else:
        guild_id_str = str(interaction.guild.id)
        if "per_server_keywords" not in keywords_data: keywords_data["per_server_keywords"] = {}
        if guild_id_str not in keywords_data["per_server_keywords"]: keywords_data["per_server_keywords"][guild_id_str] = {}
        if "bio_and_message_keywords" not in keywords_data["per_server_keywords"][guild_id_str]: keywords_data["per_server_keywords"][guild_id_str]["bio_and_message_keywords"] = {}
        if "regex_patterns" not in keywords_data["per_server_keywords"][guild_id_str]["bio_and_message_keywords"]: keywords_data["per_server_keywords"][guild_id_str]["bio_and_message_keywords"]["regex_patterns"] = []
        target_list = keywords_data["per_server_keywords"][guild_id_str]["bio_and_message_keywords"]["regex_patterns"]
        list_name = "local"

    if pattern in target_list:
        await interaction.followup.send(f"⚠️ That regex pattern is already in the {list_name} list.")
        return

    target_list.append(pattern)
    await save_keywords(keywords_data)
    logger.info(f"User {interaction.user.name} added {list_name} regex: '{pattern}'")
    await interaction.followup.send(f"✅ Regex pattern has been successfully added to the **{list_name}** list.")

async def remove_keyword_from_list(interaction: discord.Interaction, keyword: str, primary_key: str, secondary_key: str = None):
    keyword = keyword.lower().strip()
    if not keyword:
        await interaction.followup.send("❌ Keyword cannot be empty.")
        return

    keywords_data = await load_keywords()
    guild_id_str = str(interaction.guild.id)
    
    server_keywords = keywords_data.get("per_server_keywords", {}).get(guild_id_str, {})
    
    target_list = None
    if secondary_key:
        target_list = server_keywords.get(primary_key, {}).get(secondary_key, [])
    else:
        target_list = server_keywords.get(primary_key, [])

    if keyword in target_list:
        target_list.remove(keyword)
        await save_keywords(keywords_data)
        logger.info(f"Moderator {interaction.user.name} in {interaction.guild.name} removed keyword '{keyword}'.")
        await interaction.followup.send(f"✅ Keyword '{keyword}' has been removed from this server's local list.")
    else:
        await interaction.followup.send(f"❌ Keyword '{keyword}' was not found in this server's local list.")

async def remove_regex_from_list_by_id(interaction: discord.Interaction, index: int, is_global: bool):
    """A helper to remove a regex pattern from the appropriate list by its 1-based index."""
    if index <= 0:
        await interaction.followup.send("❌ Index must be a positive number. Use `/list-keywords` to find the correct ID.")
        return

    keywords_data = await load_keywords()
    target_list = None
    list_name = ""
    
    if is_global:
        target_list = keywords_data.get("global_keywords", {}).get("bio_and_message_keywords", {}).get("regex_patterns", [])
        list_name = "GLOBAL"
    else:
        guild_id_str = str(interaction.guild.id)
        target_list = keywords_data.get("per_server_keywords", {}).get(guild_id_str, {}).get("bio_and_message_keywords", {}).get("regex_patterns", [])
        list_name = "local"

    # Convert 1-based user input to 0-based list index
    real_index = index - 1

    if target_list and 0 <= real_index < len(target_list):
        removed_pattern = target_list.pop(real_index)
        await save_keywords(keywords_data)
        logger.info(f"User {interaction.user.name} removed {list_name} regex by ID #{index}: '{removed_pattern}'")
        await interaction.followup.send(f"✅ Regex pattern **`{index}`** has been removed from the **{list_name}** list.\n> `{removed_pattern}`")
    else:
        await interaction.followup.send(f"❌ Invalid ID **`{index}`**. There is no regex pattern with that ID in the {list_name} list. Use `/list-keywords` to see available IDs.")

async def update_onboard_command_visibility(guild: discord.Guild):
    """Hides the /onboard-server command for moderators if the guild is already synced."""
    try:
        # Find the /onboard-server command object in the bot's command tree.
        onboard_command = bot.tree.get_command("onboard-server")
        if not onboard_command:
            logger.warning("Could not find the 'onboard-server' command to update its permissions.")
            return

        sync_status = await load_sync_status()
        
        if guild.id in sync_status["synced_guild_ids"]:
            
            owner_id = bot.config.get("bot_owner_id")
            if not owner_id:
                logger.warning(f"Cannot hide /onboard-server in {guild.name} because bot_owner_id is not set.")
                return

            permissions = {
                discord.Object(id=owner_id): True
            }

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

# --- MAIN ---
if __name__ == "__main__":
    if not BOT_TOKEN:
        logger.critical("FATAL ERROR: ANTISCAM_BOT_TOKEN environment variable not set.")
    else:
        try:
            logger.info("Environment variables found. Starting AntiScam Bot...")
            bot.run(BOT_TOKEN, log_handler=None)
        except discord.LoginFailure:
            logger.critical("FATAL ERROR: Invalid Discord bot token.")
        except Exception as e:
            logger.critical(f"An unexpected error occurred at the top level: {e}", exc_info=True)
