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
active_scans = {}
stats_lock = asyncio.Lock()
keywords_lock = asyncio.Lock()
config_lock = asyncio.Lock()

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

    async def get_member(self, interaction: discord.Interaction) -> Optional[discord.Member]:
        if not self.flagged_member_id:
            try:
                embed_footer = interaction.message.embeds[0].footer.text
                match = re.search(r'User ID: (\d+)', embed_footer)
                if match:
                    self.flagged_member_id = int(match.group(1))
                else:
                    await interaction.followup.send("❌ Could not find user ID in the alert footer.", ephemeral=True)
                    return None
            except (IndexError, TypeError, ValueError, AttributeError):
                await interaction.followup.send("❌ Could not parse user ID from the alert.", ephemeral=True)
                return None
        try:
            return await interaction.guild.fetch_member(self.flagged_member_id)
        except discord.NotFound:
            await interaction.followup.send("❌ User not found. They may have left.", ephemeral=True)
            return None
        except Exception as e:
            logger.error(f"Failed to fetch member {self.flagged_member_id}: {e}", exc_info=True)
            await interaction.followup.send("❌ Error fetching user data.", ephemeral=True)
            return None

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
        member = await self.get_member(interaction)
        if not member: return
        try:
            reason_text = "[Federated Action] Authorized by Moderator via screening alert."
            await member.ban(reason=reason_text)
            self.update_buttons_for_state('banned')
            await self.update_embed(interaction, "✅ Banned", discord.Color.red())
        except Exception as e:
            logger.error(f"Failed to ban member {self.flagged_member_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error banning: {e}", ephemeral=True)

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.primary, custom_id="screening_kick")
    async def kick_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        member = await self.get_member(interaction)
        if not member: return
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
        member = await self.get_member(interaction)
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

    @discord.ui.button(label="Unban", style=discord.ButtonStyle.secondary, custom_id="fed_alert_unban")
    async def unban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        user_to_unban = discord.Object(id=self.banned_user_id)
        try:
            reason_text = "[Federated Action] Ban reversed by local Moderator."
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

    async def setup_hook(self) -> None:
        await self.tree.sync()
bot = AntiScamBot(intents=intents)


# --- CORE EVENT LISTENERS ---
@bot.event
async def on_ready():
    logger.info(f'{bot.user.name} has connected to Discord!')
    logger.info(f"Operating in {len(bot.guilds)} federated guilds.")
    bot.add_view(ScreeningView(flagged_member_id=None))
    bot.add_view(FederatedAlertView(banned_user_id=None))
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} slash command(s).")
    except Exception as e:
        logger.error(f"Failed to sync slash commands: {e}")

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

    result = await screen_member(member, config)

    if result.get("flagged"):
        logger.info(f"FLAGGED member on join: {member.name} in {member.guild.name}.")
        mod_channel_id = config.get("mod_alert_channels", {}).get(str(member.guild.id))
        alert_channel = member.guild.get_channel(mod_channel_id) if mod_channel_id else None
        
        if alert_channel:
            try:
                timeout_hours = config.get("default_timeout_duration_hours", 24)
                await member.timeout(timedelta(hours=timeout_hours), reason=result.get("timeout_reason"))
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

    result = await screen_message(message, config, keywords_data)

    if result.get("flagged"):
        author = message.author
        logger.info(f"FLAGGED message from {author.name} in #{message.channel.name}.")
        
        try: await message.delete()
        except Exception as e: logger.error(f"Error deleting flagged message: {e}")
        
        try:
            timeout_hours = config.get("default_timeout_duration_hours", 24)
            await author.timeout(timedelta(hours=timeout_hours), reason=result.get("timeout_reason", "Flagged message."))
        except Exception as e: logger.error(f"Failed to timeout {author.name} for flagged message: {e}")
        
        mod_channel_id = config.get("mod_alert_channels", {}).get(str(message.guild.id))
        alert_channel = message.guild.get_channel(mod_channel_id) if mod_channel_id else None
        if alert_channel:
            try:
                view = ScreeningView(flagged_member_id=author.id)
                embed = result.get("embed")
                embed.set_footer(text=f"User ID: {author.id}")
                await alert_channel.send(embed=embed, view=view)
            except Exception as e:
                logger.error(f"Failed to send message alert for {author.name}: {e}")

@bot.event
async def on_member_ban(guild: discord.Guild, user: discord.User):
    config = bot.config
    if guild.id not in config.get("federated_guild_ids", []): return
    
    stats = await load_fed_stats()
    
    guild_id_str = str(guild.id)
    if guild_id_str not in stats: stats[guild_id_str] = {}
    stats[guild_id_str]["bans_initiated_lifetime"] = stats[guild_id_str].get("bans_initiated_lifetime", 0) + 1
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")
    if "monthly_initiated" not in stats[guild_id_str]: stats[guild_id_str]["monthly_initiated"] = {}
    stats[guild_id_str]["monthly_initiated"][current_month_key] = stats[guild_id_str]["monthly_initiated"].get(current_month_key, 0) + 1
    
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
    
    is_authorized, authorization_method = False, "Unknown"
    if moderator.id == bot.user.id:
        if ban_reason.startswith("[Federated Action]"):
            is_authorized, authorization_method = True, "Authorized via Bot Alert"
        else:
            logger.info(f"Ignoring federated ban echo for {user} in {guild.name}.")
            return
    elif not moderator.bot:
        if isinstance(moderator, discord.Member):
            whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(guild.id), [])
            if any(role.id in whitelisted_mod_roles for role in moderator.roles):
                is_authorized, authorization_method = True, "Manual Ban by a whitelisted Moderator"
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
            if origin_mod_channel:
                origin_alert_embed = discord.Embed(title="🛡️ Federated Ban", description=f"The manual ban for **{user.name}** (`{user.id}`) has been broadcast to all federated servers.", color=discord.Color.blue(), timestamp=datetime.now(timezone.utc))
                await origin_mod_channel.send(embed=origin_alert_embed)

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
                await target_guild.ban(user, reason=fed_reason[:512])
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
                    if mod_channel:
                        alert_embed = discord.Embed(title="🛡️ Federated Ban", description=f"**User:** {user.name} ({user.mention}, `{user.id}`)\n**Action:** Automatically banned from this server.\n**Origin:** **{guild.name}**", color=discord.Color.dark_red(), timestamp=datetime.now(timezone.utc))
                        view = FederatedAlertView(banned_user_id=user.id)
                        allowed_mentions = discord.AllowedMentions(users=[user])
                        await mod_channel.send(embed=alert_embed, view=view, allowed_mentions=allowed_mentions)

            except Exception as e:
                logger.error(f"Error during federated ban propagation to {target_guild.name}: {e}", exc_info=True)
                if log_channel: await log_channel.send(f"❌ Failed to ban `{user}` in `{target_guild.name}` - Error: `{e}`")
    
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
    if moderator.id == bot.user.id:
        if unban_reason.startswith("[Federated Action]"):
            is_authorized, authorization_method = True, "Authorized via Bot Alert"
        else:
            logger.info(f"Ignoring federated unban echo for {user} in {guild.name}.")
            return
    elif not moderator.bot:
        if isinstance(moderator, discord.Member):
            whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(guild.id), [])
            if any(role.id in whitelisted_mod_roles for role in moderator.roles):
                is_authorized, authorization_method = True, "Manual Unban by a whitelisted Moderator"
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
    if "global" not in stats: stats["global"] = {}
    stats["global"]["total_federated_actions_lifetime"] = max(0, stats["global"].get("total_federated_actions_lifetime", 0) - 1)

    log_channel_id = config.get("log_channel_id")
    log_channel = bot.get_channel(log_channel_id) if log_channel_id else None
    if log_channel:
        embed = discord.Embed(title="ℹ️ Federated Unban", description=f"**User:** {user.name} ({user.mention}, `{user.id}`)\n**Origin:** {guild.name}\n**Authorization:** {authorization_method}", color=discord.Color.blue(), timestamp=datetime.now(timezone.utc))
        embed.set_author(name=user.name, icon_url=user.display_avatar.url)
        await log_channel.send(embed=embed)

    logger.info(f"INITIATING FEDERATED UNBAN for {user} from origin {guild.name}.")
    origin_mod_channel_id = config.get("mod_alert_channels", {}).get(str(guild.id))
    if origin_mod_channel_id:
        origin_mod_channel = guild.get_channel(origin_mod_channel_id)
        if origin_mod_channel:
            origin_alert_embed = discord.Embed(title="ℹ️ Federated Unban", description=f"The unban for **{user.name}** (`{user.id}`) has been broadcast to all federated servers.", color=discord.Color.light_grey(), timestamp=datetime.now(timezone.utc))
            await origin_mod_channel.send(embed=origin_alert_embed)

    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")
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
                    if mod_channel:
                        alert_embed = discord.Embed(title="ℹ️ Federated Unban", description=f"**User:** {user.name} ({user.mention}, `{user.id}`)\n**Action:** Automatically unbanned from this server.\n**Origin:** **{guild.name}**", color=discord.Color.green(), timestamp=datetime.now(timezone.utc))
                        allowed_mentions = discord.AllowedMentions(users=[user])
                        await mod_channel.send(embed=alert_embed, allowed_mentions=allowed_mentions)
            except Exception as e:
                logger.error(f"Error during federated unban propagation to {target_guild.name}: {e}", exc_info=True)
                if log_channel: await log_channel.send(f"❌ Failed to unban `{user}` in `{target_guild.name}` - Error: `{e}`")
        except discord.NotFound:
            logger.info(f"User {user} was not banned in {target_guild.name}. No unban action needed.")
            continue
    
    await save_fed_stats(stats)

async def screen_member(member: discord.Member, config: dict) -> dict:
    """
    Performs the complete screening process for a single member and returns the findings.
    Does NOT perform any actions (timeout, send message).
    Returns a dictionary with flag status, embed, and timeout reason.
    """
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
        embed.add_field(name="Status", value="User timed out for 1 day. Awaiting review...", inline=True)
        
        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}

    keywords_data = await load_keywords()
    if not keywords_data: return {"flagged": False}

    try:
        user_profile = await bot.fetch_user(member.id)
        bio = getattr(user_profile, 'bio', "")
    except Exception as e:
        logger.error(f"Error fetching profile for {member.name}: {e}", exc_info=True)
        bio = ""

    triggered_keywords = []
    name_text = f"{user_profile.name} {member.nick or ''}"
    
    local_rules = keywords_data.get("per_server_keywords", {}).get(str(member.guild.id), {})
    global_rules = keywords_data.get("global_keywords", {})

    triggered_keywords.extend(check_text_for_keywords(name_text, local_rules.get("username_keywords", {})))
    triggered_keywords.extend(check_text_for_keywords(name_text, global_rules.get("username_keywords", {})))
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
        embed.add_field(name="Status", value="User timed out for 1 day. Awaiting review...", inline=True)
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
            result = await screen_member(member, config)
            
            if result.get("flagged"):
                flagged_count += 1
                try:
                    timeout_hours = config.get("default_timeout_duration_hours", 24)
                    await member.timeout(timedelta(hours=timeout_hours), reason=result.get("timeout_reason", "Flagged by scan."))
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
@bot.tree.command(name="zreloadconfig", description="[Owner Only] Reloads federation config and updates keyword file.")
async def reloadconfig(interaction: discord.Interaction):
    if not await bot.is_owner(interaction.user):
        await interaction.response.send_message("❌ You do not have permission to use this command.", ephemeral=True)
        return

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

# --- OWNER GLOBAL KEYWORD COMMANDS ---
@bot.tree.command(name="zadd-global-name-substring", description="[Owner Only] Adds a SUBSTRING keyword to the GLOBAL list.")
@discord.app_commands.describe(keyword="The keyword to add globally (e.g., 'admin').")
async def add_global_username_substring(interaction: discord.Interaction, keyword: str):
    if not await bot.is_owner(interaction.user):
        return await interaction.response.send_message("❌ This command is restricted to the bot owner.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await add_global_keyword_to_list(interaction, keyword, "username_keywords", "substring")

@bot.tree.command(name="zadd-global-name-smart", description="[Owner Only] Adds a SMART keyword to the GLOBAL list.")
@discord.app_commands.describe(keyword="The keyword to add globally (e.g., 'mod').")
async def add_global_username_smart(interaction: discord.Interaction, keyword: str):
    if not await bot.is_owner(interaction.user):
        return await interaction.response.send_message("❌ This command is restricted to the bot owner.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await add_global_keyword_to_list(interaction, keyword, "username_keywords", "smart")

@bot.tree.command(name="zadd-global-bio-keyword", description="[Owner Only] Adds a BIO keyword to the GLOBAL list.")
@discord.app_commands.describe(keyword="The keyword or phrase to add globally.")
async def add_global_bio_keyword(interaction: discord.Interaction, keyword: str):
    if not await bot.is_owner(interaction.user):
        return await interaction.response.send_message("❌ This command is restricted to the bot owner.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await add_global_keyword_to_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

# --- OWNER GLOBAL REGEX COMMANDS ---
@bot.tree.command(name="zadd-global-regex", description="[OWNER ONLY] Adds a regex pattern to the GLOBAL list.")
@discord.app_commands.describe(pattern="The exact regex pattern to add globally.")
async def add_global_regex(interaction: discord.Interaction, pattern: str):
    if not await bot.is_owner(interaction.user):
        return await interaction.response.send_message("❌ This command is restricted to the bot owner.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await add_regex_to_list(interaction, pattern, is_global=True)

@bot.tree.command(name="zrm-global-name-substring", description="[Owner Only] Removes a SUBSTRING keyword from the GLOBAL list.")
@discord.app_commands.describe(keyword="The exact keyword to remove globally.")
async def remove_global_username_substring(interaction: discord.Interaction, keyword: str):
    if not await bot.is_owner(interaction.user):
        return await interaction.response.send_message("❌ This command is restricted to the bot owner.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await remove_global_keyword_from_list(interaction, keyword, "username_keywords", "substring")

@bot.tree.command(name="zrm-global-name-smart", description="[Owner Only] Removes a SMART keyword from the GLOBAL list.")
@discord.app_commands.describe(keyword="The exact keyword to remove globally.")
async def remove_global_username_smart(interaction: discord.Interaction, keyword: str):
    if not await bot.is_owner(interaction.user):
        return await interaction.response.send_message("❌ This command is restricted to the bot owner.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await remove_global_keyword_from_list(interaction, keyword, "username_keywords", "smart")

@bot.tree.command(name="zrm-global-bio-keyword", description="[Owner Only] Removes a BIO keyword from the GLOBAL list.")
@discord.app_commands.describe(keyword="The exact keyword to remove globally.")
async def remove_global_bio_keyword(interaction: discord.Interaction, keyword: str):
    if not await bot.is_owner(interaction.user):
        return await interaction.response.send_message("❌ This command is restricted to the bot owner.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await remove_global_keyword_from_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

@bot.tree.command(name="zrm-global-regex-by-id", description="[OWNER ONLY] Removes a regex from the GLOBAL list by its ID.")
@discord.app_commands.describe(index="The numerical ID of the global regex pattern to remove.")
async def remove_global_regex_by_id(interaction: discord.Interaction, index: int):
    if not await bot.is_owner(interaction.user):
        return await interaction.response.send_message("❌ This command is restricted to the bot owner.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await remove_regex_from_list_by_id(interaction, index, is_global=True)

# --- MODERATOR SLASH COMMANDS ---
@bot.tree.command(name="bancounter", description="Displays local and federated ban statistics.")
async def bancounter(interaction: discord.Interaction):
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
    embed.add_field(name="Total Network Actions (All Time)", value=f"**`{total_federated_actions_lifetime}`**\n*Total federated bans across all servers.*", inline=False)
    await interaction.followup.send(embed=embed)

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
        
@bot.tree.command(name="list-keywords", description="Lists all active screening keywords for this server.")
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

@bot.tree.command(name="add-name-keyword-substring", description="Aggressive name keyword (catches variations: 'admin' in 'daoadmin' 'admin123').")
@discord.app_commands.describe(keyword="Example: 'admin' will match 'listadaoadmin' or 'admin123'.")
async def add_username_keyword_substring(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await add_keyword_to_list(interaction, keyword, "username_keywords", "substring")

@bot.tree.command(name="add-name-keyword-smart", description="Precise name keyword (avoids false positives: 'mod' in 'mod123' not 'modern').")
@discord.app_commands.describe(keyword="Example: 'mod' will match 'mod123' but IGNORE 'modern'.")
async def add_username_keyword_smart(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await add_keyword_to_list(interaction, keyword, "username_keywords", "smart")

@bot.tree.command(name="add-bio-keyword", description="Keyword for screening user bios and messages.")
@discord.app_commands.describe(keyword="The keyword or phrase to add (e.g., 'dm me for help').")
async def add_bio_keyword(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await add_keyword_to_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

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



@bot.tree.command(name="test-regex", description="Tests a regex pattern against sample text without saving it.")
@discord.app_commands.describe(pattern="The regex pattern to test.", sample_text="The text to test the pattern against.")
async def test_regex(interaction: discord.Interaction, pattern: str, sample_text: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    
    try:
        re.compile(pattern)
        match = re.search(pattern, sample_text, re.IGNORECASE)
        
        if match:
            embed = discord.Embed(title="✅ Regex Test: Match Found", color=discord.Color.green())
            embed.add_field(name="Pattern", value=f"`{pattern}`", inline=False)
            embed.add_field(name="Sample Text", value=f"```{sample_text}```", inline=False)
            embed.add_field(name="Matched Text", value=f"`{match.group(0)}`", inline=False)
        else:
            embed = discord.Embed(title="❌ Regex Test: No Match", color=discord.Color.orange())
            embed.add_field(name="Pattern", value=f"`{pattern}`", inline=False)
            embed.add_field(name="Sample Text", value=f"```{sample_text}```", inline=False)
            
        await interaction.response.send_message(embed=embed, ephemeral=True)

    except re.error as e:
        await interaction.response.send_message(f"❌ **Invalid Regex:** That pattern is not valid.\n`{e}`", ephemeral=True)

@bot.tree.command(name="add-regex", description="Adds a regex pattern to this server's local list. Try /test-regex first!")
@discord.app_commands.describe(pattern="The exact regex pattern to add. Must be double-escaped for backslashes.")
async def add_local_regex(interaction: discord.Interaction, pattern: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await add_regex_to_list(interaction, pattern, is_global=False)

@bot.tree.command(name="rm-regex-by-id", description="Removes a regex from the local list by its ID from /list-keywords.")
@discord.app_commands.describe(index="The numerical ID of the regex pattern to remove.")
async def remove_local_regex_by_id(interaction: discord.Interaction, index: int):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await remove_regex_from_list_by_id(interaction, index, is_global=False)

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

@bot.tree.command(name="rm-name-keyword-substring", description="Removes a SUBSTRING keyword from this server's local list.")
@discord.app_commands.describe(keyword="The exact keyword to remove.")
async def remove_username_keyword_substring(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await remove_keyword_from_list(interaction, keyword, "username_keywords", "substring")

@bot.tree.command(name="rm-name-keyword-smart", description="Removes a SMART keyword from this server's local list.")
@discord.app_commands.describe(keyword="The exact keyword to remove.")
async def remove_username_keyword_smart(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await remove_keyword_from_list(interaction, keyword, "username_keywords", "smart")

@bot.tree.command(name="rm-bio-keyword", description="Removes a keyword from this server's local bio/message list.")
@discord.app_commands.describe(keyword="The exact keyword or phrase to remove.")
async def remove_bio_keyword(interaction: discord.Interaction, keyword: str):
    config = bot.config
    if not await has_federated_mod_role(interaction, config): return
    await interaction.response.defer(ephemeral=True)
    await remove_keyword_from_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

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

@bot.tree.command(name="scanallmembers", description="Retroactively scans all server members against the screening list.")
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
