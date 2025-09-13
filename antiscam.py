import discord
from discord.ext import commands
import json
import re
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

# --- Federation Configuration ---
# A list of all server IDs that are part of the federation.
# The bot will only operate in these guilds.
FEDERATED_GUILD_IDS = [
    # Server IDs here
]

# The channel where all federated ban/unban actions will be logged.
# This should be in a private, secure server that only federation admins can see.
LOG_CHANNEL_ID = # Channel ID

# --- Screening & Whitelist Configuration ---
# This is CRITICAL. It maps each server to its list of trusted moderator roles.
# A federated ban will ONLY trigger if the ban was initiated by a user with one of these roles.
MODERATOR_ROLES_PER_GUILD = {
    # Server ID A: [
        # Roles here
    ],
    # Server ID B: [
        # Roles here
    ]
}

# This maps each server to its list of roles that should be ignored by the screening features.
# This is for your team, other bots, trusted contributors, whitelisted users (eg. allowed to send links or other matching keywords) etc.
WHITELISTED_ROLES_PER_GUILD = {
    # Server ID A: [
        # Roles here
    ],
    # Server ID B: [
        # Roles here
    ]
}

# This maps each server to its specific channel for real-time alerts.
MOD_ALERT_CHANNELS = {
    # Server ID A: # Server A mod channel
    # Server ID A: # Server B mod channel
}

# This maps each server to its channel for the results of /scanallmembers.
MOD_SCAN_RESULTS_CHANNELS = {
    # Server ID A: Channel ID for full scan results
    # Server ID B: Channel ID for full scan results
}

# A dictionary to keep track of active scan tasks per guild
active_scans = {}


# --- DATA HANDLING & KEYWORD MATCHING ---
def load_fed_stats():
    """Loads the federated ban statistics from its JSON file."""
    if os.path.exists(FED_STATS_FILE):
        with open(FED_STATS_FILE, 'r') as f:
            try: return json.load(f)
            except json.JSONDecodeError: return {}
    return {}

def save_fed_stats(data: dict):
    """Saves the federated ban statistics to its JSON file."""
    with open(FED_STATS_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def load_keywords():
    """Loads the screening keywords and regex patterns from the JSON file."""
    if os.path.exists(KEYWORDS_FILE):
        with open(KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Could not decode {KEYWORDS_FILE}. Please check its format.")
                return None
    else:
        logger.error(f"{KEYWORDS_FILE} not found. Screening will be disabled.")
        return None

def save_keywords(keywords_data: dict):
    """Saves the keyword data back to the JSON file."""
    with open(KEYWORDS_FILE, 'w', encoding='utf-8') as f:
        json.dump(keywords_data, f, indent=4)

def check_text_for_keywords(text_to_check: str, keywords_data: dict, check_type: str) -> list:
    """
    Checks a given string against keywords using configurable matching strategies
    based on a dictionary structure.
    """
    if not text_to_check or not keywords_data:
        return []

    triggered = []
    normalized_text = unidecode(text_to_check).lower()

    if check_type == 'username':
        username_rules = keywords_data.get("username_keywords", {})
        
        # Process 'substring' keywords
        for keyword in username_rules.get("substring", []):
            if keyword.lower() in normalized_text:
                triggered.append(keyword)
        
        # Process 'smart' keywords
        for keyword in username_rules.get("smart", []):
            pattern = r'(?<![a-z])' + re.escape(keyword.lower()) + r'(?![a-z])'
            if re.search(pattern, normalized_text):
                triggered.append(keyword)
    
    elif check_type == 'bio_and_message':
        rules = keywords_data.get("bio_and_message_keywords", {})
        
        # For simple keywords in bios, 'smart' matching is the best default.
        for keyword in rules.get("simple_keywords", []):
            pattern = r'(?<![a-z])' + re.escape(keyword.lower()) + r'(?![a-z])'
            if re.search(pattern, normalized_text):
                triggered.append(keyword)

        # Check regex patterns
        texts_to_scan_regex = {text_to_check, normalized_text}
        for pattern in rules.get("regex_patterns", []):
            try:
                for text in texts_to_scan_regex:
                    if re.search(pattern, text, re.IGNORECASE):
                        if "Matched Regex Pattern" not in triggered:
                            triggered.append("Matched Regex Pattern")
                        break
            except re.error as e:
                logger.warning(f"Invalid regex pattern in {KEYWORDS_FILE}: '{pattern}' - {e}")
                continue
            
    return list(set(triggered))


# --- INTERACTIVE UI CLASSES ---
class ScreeningView(discord.ui.View):
    def __init__(self, flagged_member: discord.Member):
        super().__init__(timeout=None)
        self.flagged_member = flagged_member

    async def update_embed(self, interaction: discord.Interaction, status: str, color: discord.Color):
        """Helper function to edit the original embed with a new status."""
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
        try:
            reason_text = f"[Federated Action] Authorized by Moderator via screening alert."
            await self.flagged_member.ban(reason=reason_text)
            
            self.ban_button.disabled = True
            self.ignore_button.disabled = True
            self.unban_button.disabled = False
            await self.update_embed(interaction, "✅ Banned", discord.Color.red())
        except Exception as e:
            logger.error(f"Failed to ban member {self.flagged_member.id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An error occurred while banning: {e}", ephemeral=True)

    @discord.ui.button(label="Unban", style=discord.ButtonStyle.grey, custom_id="screening_unban", disabled=True)
    async def unban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        try:
            await interaction.guild.unban(self.flagged_member, reason=f"Unbanned by contributor with whitelisted role.")
            self.unban_button.disabled = True
            await self.update_embed(interaction, "🟡 Unbanned", discord.Color.gold())
        except Exception as e:
            logger.error(f"Failed to unban member {self.flagged_member.id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An error occurred while unbanning: {e}", ephemeral=True)

    @discord.ui.button(label="Ignore", style=discord.ButtonStyle.grey, custom_id="screening_ignore")
    async def ignore_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.message.delete()
            await interaction.response.send_message("✅ Alert dismissed.", ephemeral=True, delete_after=5)
        except Exception as e:
            logger.error(f"Failed to delete screening message: {e}", exc_info=True)
            await interaction.response.send_message("❌ Failed to delete the message.", ephemeral=True)

class FederatedAlertView(discord.ui.View):
    def __init__(self, banned_user: discord.User):
        super().__init__(timeout=None) # Persistent button
        self.banned_user = banned_user

    @discord.ui.button(label="Unban", style=discord.ButtonStyle.secondary, custom_id="fed_alert_unban")
    async def unban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        try:
            await interaction.guild.unban(self.banned_user, reason=f"Federated ban reversed by local contributor with whitelisted role.")
            
            # Edit the message to confirm the action
            embed = interaction.message.embeds[0]
            embed.color = discord.Color.green()
            embed.description += f"\n\n**UPDATE:** User was unbanned from this server by local contributor."
            
            # Disable the button after use
            button.disabled = True
            await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)
            logger.info(f"Federated ban for {self.banned_user} was reversed in {interaction.guild.name} by {interaction.user.name}.")

        except discord.Forbidden:
            await interaction.followup.send("❌ I don't have permission to unban users in this server.", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to reverse federated ban for {self.banned_user.id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An error occurred while unbanning: {e}", ephemeral=True)

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
        # Disable all buttons in the view to prevent further clicks.
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="✅ **Confirmation received. Starting scan...** See below for progress updates. Use /stopscan to cancel.", view=self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        self.stop()
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(content="Scan cancelled.", view=self)

# --- BOT SETUP ---
intents = discord.Intents.default()
intents.guilds = True       # Needed for general guild information and audit logs
intents.members = True      # on_member_join and on_member_ban
intents.moderation = True   # on_member_ban to get ban events
intents.message_content = True 

# We are only using slash commands, so a prefix is not needed, but we define the bot object.
bot = commands.Bot(command_prefix="!unusedprefix!", intents=intents)


# --- CORE EVENT LISTENERS ---
@bot.event
async def on_ready():
    """Called when the bot is ready and connected to Discord."""
    logger.info(f'{bot.user.name} has connected to Discord!')
    logger.info(f"Operating in {len(bot.guilds)} federated guilds.")
    
    # This is needed to make buttons work after the bot restarts.
    bot.add_view(ScreeningView(flagged_member=None))
    bot.add_view(FederatedAlertView(banned_user=None))
    
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} slash command(s).")
    except Exception as e:
        logger.error(f"Failed to sync slash commands: {e}")

@bot.event
async def on_member_join(member: discord.Member):
    """Screens new members based on their profile information."""
    # Ignore joins in any server not in our federation
    if member.guild.id not in FEDERATED_GUILD_IDS:
        return
    
    # Add a small delay to allow the member object to be fully populated by Discord's gateway.
    await asyncio.sleep(2)

    # Now, re-fetch the member object from the guild's cache to ensure it's up-to-date.
    # This is a robust way to combat race conditions.
    try:
        member = await member.guild.fetch_member(member.id)
    except discord.NotFound:
        logger.warning(f"Member {member.name} left before they could be processed.")
        return

    # Check if the member has a whitelisted role for their specific guild
    whitelisted_roles = WHITELISTED_ROLES_PER_GUILD.get(str(member.guild.id), [])
    member_role_ids = {role.id for role in member.roles}
    if any(role_id in whitelisted_roles for role_id in member_role_ids):
        logger.info(f"Member {member.name} in {member.guild.name} has a whitelisted role. Skipping screen.")
        return

    keywords_data = load_keywords()
    if not keywords_data: return

    try:
        logger.info(f"New member joined {member.guild.name}: {member.name}. Fetching profile...")
        user_profile = await bot.fetch_user(member.id)
        bio = getattr(user_profile, 'bio', "")
    except discord.NotFound:
        logger.warning(f"Could not fetch profile for {member.name}: User not found.")
        return
    except Exception as e:
        logger.error(f"Error fetching profile for {member.name}: {e}", exc_info=True)
        bio = ""

    # name_text = f"{member.name} {member.display_name}"
    name_text = f"{user_profile.name} {member.nick or ''}"
    triggered_by_name = check_text_for_keywords(name_text, keywords_data, 'username')
    triggered_by_bio = check_text_for_keywords(bio, keywords_data, 'bio_and_message')
    triggered_keywords = list(set(triggered_by_name + triggered_by_bio))

    if triggered_keywords:
        logger.info(f"FLAGGED user {member.name} in {member.guild.name} for keywords: {', '.join(triggered_keywords)}")
        
        try:
            await member.timeout(timedelta(hours=1), reason="Flagged by screening bot for review.")
            logger.info(f"Successfully timed out {member.name} for 1 hour.")
        except Exception as e:
            logger.error(f"Failed to timeout {member.name}: {e}", exc_info=True)
        
        mod_channel_id = MOD_ALERT_CHANNELS.get(str(member.guild.id))
        if not mod_channel_id:
            logger.error(f"MOD_ALERT_CHANNELS not configured for guild ID {member.guild.id}.")
            return
        mod_channel = member.guild.get_channel(mod_channel_id)
        
        if not mod_channel:
            logger.error(f"Mod alert channel with ID {mod_channel_id} not found in {member.guild.name}.")
            return

        embed = discord.Embed(title="Flagged", description=f"{member.mention} (`{member.id}`)", color=discord.Color.orange(), timestamp=datetime.now(timezone.utc))
        embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
        if bio:
            bio_text = bio
            if len(bio_text) > 1024:
                bio_text = bio_text[:1020] + "..."
            embed.add_field(name="📝 Bio", value=bio_text, inline=False)
        embed.add_field(name="🚩 Trigger", value=f"`{', '.join(triggered_keywords)}`", inline=True)
        embed.add_field(name="Status", value="User timed out for 1 hour. Awaiting review...", inline=True)
        embed.add_field(name="Account Age", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
        
        view = ScreeningView(flagged_member=member)
        allowed_mentions = discord.AllowedMentions(users=[member])
        await mod_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)
    else:
        logger.info(f"Member {member.name} in {member.guild.name} passed screening. No keywords triggered.")
        
@bot.event
async def on_message(message: discord.Message):
    """Scans messages in real-time for blacklisted content."""
    # This is crucial to ensure that slash commands (which are messages) are not processed by this.
    # It also ignores messages from the bot itself.
    if message.author == bot.user or message.interaction_metadata is not None:
        return

    # --- Pre-Checks: Ignore messages we don't need to scan ---
    # Ignore messages not in a federated guild or from other bots
    if not message.guild or message.guild.id not in FEDERATED_GUILD_IDS or message.author.bot:
        return

    # Ignore messages in DMs
    if not isinstance(message.author, discord.Member):
        return

    # Get the per-guild whitelists and ignored categories
    whitelisted_roles = WHITELISTED_ROLES_PER_GUILD.get(str(message.guild.id), [])

    # Ignore messages from users with whitelisted roles
    member_role_ids = {role.id for role in message.author.roles}
    if any(role_id in whitelisted_roles for role_id in member_role_ids):
        return

    # --- Keyword Check ---
    keywords_data = load_keywords()
    if not keywords_data:
        return # Stop if keywords file is missing

    triggered_keywords = check_text_for_keywords(message.content, keywords_data, 'bio_and_message')

    # --- Flagging and Action ---
    if triggered_keywords:
        author = message.author
        logger.info(f"FLAGGED message from {author.name} in #{message.channel.name}. Triggered by: {', '.join(triggered_keywords)}")
        
        # Delete the offending message first
        try:
            await message.delete()
        except Exception as e:
            logger.error(f"Error deleting flagged message: {e}")

        # Apply a 1-hour timeout
        try:
            await author.timeout(timedelta(hours=1), reason="Flagged message detected by bot.")
            logger.info(f"Successfully timed out {author.name} for 1 hour.")
        except Exception as e:
            logger.error(f"Failed to timeout {author.name} for flagged message: {e}")

        # Send an alert to the server's mod channel
        mod_channel_id = MOD_ALERT_CHANNELS.get(str(message.guild.id))
        if not mod_channel_id:
            logger.error(f"MOD_ALERT_CHANNELS not configured for guild ID {message.guild.id}.")
            return
        mod_channel = message.guild.get_channel(mod_channel_id)
        
        if not mod_channel:
            logger.error(f"Mod alert channel with ID {mod_channel_id} not found in {message.guild.name}.")
            return

        embed = discord.Embed(
            title="Flagged",
            description=f"{message.author.mention} (`{message.author.id}`) in {message.channel.mention}",
            color=discord.Color.dark_red(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=f"{author.name}", icon_url=author.display_avatar.url)
        embed.add_field(name="📝 Message", value=f"```{message.content[:1000]}```", inline=False)
        embed.add_field(name="🚩 Trigger", value=f"`{', '.join(triggered_keywords)}`", inline=True)
        embed.add_field(name="Status", value="Message deleted. User timed out for 1 hour. Awaiting review...", inline=True)
        embed.add_field(name="Account Age", value=f"<t:{int(author.created_at.timestamp())}:R>", inline=True)

        view = ScreeningView(flagged_member=author)
        allowed_mentions = discord.AllowedMentions(users=[author])
        await mod_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)

@bot.event
async def on_member_ban(guild: discord.Guild, user: discord.User):
    """The core federated ban logic."""
    if guild.id not in FEDERATED_GUILD_IDS:
        return

    # --- Audit Log and Authorization Logic ---
    moderator = None
    ban_reason = "No reason provided."
    await asyncio.sleep(2)
    try:
        async for entry in guild.audit_logs(action=discord.AuditLogAction.ban, limit=5):
            if entry.target.id == user.id:
                moderator = entry.user
                ban_reason = entry.reason or "No reason provided."
                break
    except discord.Forbidden:
        logger.error(f"Missing Audit Log permissions in {guild.name}. Cannot verify ban initiator.")
        return
    except Exception as e:
        logger.error(f"Error fetching audit logs in {guild.name}: {e}", exc_info=True)
        return

    if not moderator:
        logger.info(f"Ban of {user} in {guild.name} could not be attributed. No federated action.")
        return

    is_authorized = False
    authorization_method = "Unknown"

    if moderator.id == bot.user.id:
        if ban_reason.startswith("[Federated Action]"):
            is_authorized = True
            authorization_method = "Authorized via Bot Alert"
        else:
            logger.info(f"Ignoring federated ban echo for {user} in {guild.name}.")
            return
    elif not moderator.bot:
        mod_role_ids = {role.id for role in moderator.roles}
        whitelisted_mod_roles = MODERATOR_ROLES_PER_GUILD.get(str(guild.id), [])
        if any(role_id in whitelisted_mod_roles for role_id in mod_role_ids):
            is_authorized = True
            authorization_method = "Manual Ban by a whitelisted Moderator"
        else:
            logger.warning(f"User {user} was banned by {moderator}, but they do not have a whitelisted role. No federated action.")
            return
    else:
        logger.info(f"Ban of {user} in {guild.name} was performed by an unauthorized bot ({moderator.name}). No federated action.")
        return

    if not is_authorized:
        logger.warning(f"Ban of {user} by {moderator} did not pass authorization checks. No action taken.")
        return

    # --- Increment Federated Lifetime Counter ---
    stats = load_fed_stats()
    origin_guild_id_str = str(guild.id)
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")

    # Increment "Bans Initiated" for the origin server (Lifetime and Monthly).
    if origin_guild_id_str not in stats: stats[origin_guild_id_str] = {}
    stats[origin_guild_id_str]["bans_initiated_lifetime"] = stats[origin_guild_id_str].get("bans_initiated_lifetime", 0) + 1
    
    if "monthly_initiated" not in stats[origin_guild_id_str]: stats[origin_guild_id_str]["monthly_initiated"] = {}
    stats[origin_guild_id_str]["monthly_initiated"][current_month_key] = stats[origin_guild_id_str]["monthly_initiated"].get(current_month_key, 0) + 1

    # Increment the "Total Federated Actions" global counter.
    if "global" not in stats: stats["global"] = {}
    stats["global"]["total_federated_actions_lifetime"] = stats["global"].get("total_federated_actions_lifetime", 0) + 1
    
    save_fed_stats(stats) # Save the initiated counts

    # --- Post to Central Log Channel ---
    log_channel = bot.get_channel(LOG_CHANNEL_ID)
    if log_channel:
        embed = discord.Embed(
            title="🛡️ Federated Ban Initiated",
            description=f"**User:** {user.name} ({user.mention} `{user.id}`)\n"
                        f"**Origin** {guild.name}\n"
                        f"**Authorization:** {authorization_method}\n"
                        f"**Reason:** ```{ban_reason[:1000]}```",
            color=discord.Color.brand_red(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=user.name, icon_url=user.display_avatar.url)
        await log_channel.send(embed=embed)
    
    logger.info(f"INITIATING FEDERATED BAN for {user} from origin {guild.name}.")

    # --- Send Local Alert to the ORIGIN Server ---
    if authorization_method == "Manual Ban":
        origin_mod_channel_id = MOD_ALERT_CHANNELS.get(str(guild.id))
        if origin_mod_channel_id:
            origin_mod_channel = guild.get_channel(origin_mod_channel_id)
            if origin_mod_channel:
                origin_alert_embed = discord.Embed(
                    title="🛡️ Federated Ban",
                    description=f"Manual local ban for **{user.name}** ({user.mention} `{user.id}`) broadcasted to all federated servers.",
                    color=discord.Color.blue(),
                    timestamp=datetime.now(timezone.utc)
                )
                await origin_mod_channel.send(embed=origin_alert_embed)
            else:
                logger.warning(f"Could not find mod alert channel {origin_mod_channel_id} in origin server {guild.name} to send local confirmation.")
        
    for guild_id in FEDERATED_GUILD_IDS:
        if guild_id == guild.id: continue

        target_guild = bot.get_guild(guild_id)
        if not target_guild:
            logger.error(f"Could not find target guild with ID {guild_id}.")
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

                stats = load_fed_stats() # Reload stats to be safe
                target_guild_id_str = str(target_guild.id)
                
                if target_guild_id_str not in stats: stats[target_guild_id_str] = {}
                stats[target_guild_id_str]["bans_received_lifetime"] = stats[target_guild_id_str].get("bans_received_lifetime", 0) + 1

                if "monthly_received" not in stats[target_guild_id_str]: stats[target_guild_id_str]["monthly_received"] = {}
                stats[target_guild_id_str]["monthly_received"][current_month_key] = stats[target_guild_id_str]["monthly_received"].get(current_month_key, 0) + 1
                
                save_fed_stats(stats)

                # Send Local Alert to the Target Server
                mod_channel_id = MOD_ALERT_CHANNELS.get(str(target_guild.id))
                if mod_channel_id:
                    mod_channel = target_guild.get_channel(mod_channel_id)
                    if mod_channel:
                        alert_embed = discord.Embed(
                            title="🛡️ Federated Ban",
                            description=f"{user.name} ({user.mention} `{user.id}`)\n"
                                        f"**Action:** Automatically banned from this server\n"
                                        f"**Origin:** **{guild.name}**",
                            color=discord.Color.dark_red(),
                            timestamp=datetime.now(timezone.utc)
                        )
                        await mod_channel.send(embed=alert_embed, view=FederatedAlertView(banned_user=user))
                    else:
                        logger.warning(f"Could not find mod alert channel {mod_channel_id} in {target_guild.name} to send local alert.")
            except discord.Forbidden:
                logger.error(f"Missing Ban Members permission in {target_guild.name}.")
                if log_channel: await log_channel.send(f"❌ Failed to ban `{user}` in `{target_guild.name}` - Missing Permissions.")
            except Exception as e:
                logger.error(f"Unexpected error banning {user} from {target_guild.name}: {e}", exc_info=True)
                if log_channel: await log_channel.send(f"❌ Failed to ban `{user}` in `{target_guild.name}` - Error: `{e}`")

@bot.event
async def on_member_unban(guild: discord.Guild, user: discord.User):
    """Logs when a user is unbanned in a federated server."""
    if guild.id not in FEDERATED_GUILD_IDS:
        return

    log_channel = bot.get_channel(LOG_CHANNEL_ID)
    if log_channel:
        embed = discord.Embed(
            title="ℹ️ Federated Unban",
            description=f"**User:** {user.name} ({user.mention} `{user.id}`)\n"
                        f"**Server:** {guild.name}\n\n"
                        f"Federation members may wish to review this action.",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=user.name, icon_url=user.display_avatar.url)
        await log_channel.send(embed=embed)
    logger.info(f"NOTICE: User {user} was unbanned from {guild.name}.")


# --- CORE LOGIC FUNCTIONS (for commands) ---
async def run_full_scan(interaction: discord.Interaction):
    """The long-running task that performs the full member scan for a single guild."""
    guild = interaction.guild
    results_channel_id = MOD_SCAN_RESULTS_CHANNELS.get(str(guild.id))
    results_channel = guild.get_channel(results_channel_id)
    
    if not results_channel:
        await interaction.followup.send(f"❌ **Scan Aborted:** Could not find the configured scan results channel in {guild.name}.", ephemeral=True)
        if guild.id in active_scans: del active_scans[guild.id]
        return

    keywords_data = load_keywords()
    if not keywords_data:
        await interaction.followup.send("❌ **Scan Aborted:** Could not load the keywords file.", ephemeral=True)
        if guild.id in active_scans: del active_scans[guild.id]
        return

    if not guild.chunked:
        await guild.chunk()
    total_members = guild.member_count

    progress_message = None
    checked_count, flagged_count = 0, 0
    update_interval = 100

    try:
        progress_message = await interaction.channel.send(f"🔍 Scan initiated by {interaction.user.mention}. Preparing to scan {total_members} members in **{guild.name}**...")
        logger.info(f"Full member scan initiated by {interaction.user.name} for guild '{guild.name}'.")

        for member in guild.members:
            if asyncio.current_task().cancelled():
                raise asyncio.CancelledError

            checked_count += 1
            if member.bot: continue
            
            whitelisted_roles = WHITELISTED_ROLES_PER_GUILD.get(str(guild.id), [])
            member_role_ids = {role.id for role in member.roles}
            if any(role_id in whitelisted_roles for role_id in member_role_ids):
                continue

            try:
                user_profile = await bot.fetch_user(member.id)
                bio = getattr(user_profile, 'bio', "")
            except discord.NotFound:
                continue 
            except Exception as e:
                logger.error(f"Error fetching profile for {member.name} during scan: {e}")
                bio = ""

            name_text = f"{member.name} {member.display_name}"
            triggered_by_name = check_text_for_keywords(name_text, keywords_data, 'username')
            triggered_by_bio = check_text_for_keywords(bio, keywords_data, 'bio_and_message')
            triggered_keywords = list(set(triggered_by_name + triggered_by_bio))

            if triggered_keywords:
                flagged_count += 1
                logger.info(f"FLAGGED (retro scan): {member.name} for keywords: {', '.join(triggered_keywords)}")
                try:
                    await member.timeout(timedelta(hours=1), reason="Flagged by retroactive screening scan.")
                except Exception as e:
                    logger.error(f"Failed to timeout {member.name} during scan: {e}")

                embed = discord.Embed(title="Flagged (Retro Scan)", description=f"{member.mention} (`{member.id}`)", color=discord.Color.orange(), timestamp=datetime.now(timezone.utc))
                embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
                if bio:
                    bio_text = bio
                    if len(bio_text) > 1024:
                        bio_text = bio_text[:1020] + "..."
                    embed.add_field(name="📝 Bio", value=bio_text, inline=False)
                embed.add_field(name="🚩 Trigger", value=f"`{', '.join(triggered_keywords)}`", inline=True)
                embed.add_field(name="Status", value="User timed out for 1 hour. Awaiting review...", inline=True)
                embed.add_field(name="Account Age", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
                
                view = ScreeningView(flagged_member=member)
                allowed_mentions = discord.AllowedMentions(users=[member])
                await results_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)

            if checked_count % update_interval == 0:
                progress_text = f"Scan in progress... {checked_count}/{total_members} members checked. **{flagged_count}** flagged so far. Use /stopscan to cancel."
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

# --- SLASH COMMANDS ---
@bot.tree.command(name="bancounter", description="Displays local and federated ban statistics.")
async def bancounter(interaction: discord.Interaction):
    if not await has_federated_mod_role(interaction): return

    await interaction.response.defer()

    stats = load_fed_stats()
    guild_id_str = str(interaction.guild.id)
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")

    # Get stats for the current server
    guild_stats = stats.get(guild_id_str, {})
    
    # Initiated bans (monthly and lifetime)
    bans_initiated_monthly = guild_stats.get("monthly_initiated", {}).get(current_month_key, 0)
    bans_initiated_lifetime = guild_stats.get("bans_initiated_lifetime", 0)

    # Received bans (monthly and lifetime)
    bans_received_monthly = guild_stats.get("monthly_received", {}).get(current_month_key, 0)
    bans_received_lifetime = guild_stats.get("bans_received_lifetime", 0)

    # Get the global federated action count
    total_federated_actions_lifetime = stats.get("global", {}).get("total_federated_actions_lifetime", 0)

    embed = discord.Embed(
        title="🛡️ Stats",
        description=f"Initiated here: **`{bans_initiated_monthly}`** (Month), **`{bans_initiated_lifetime}`** (All time)\n"
                    f"Received here: **`{bans_received_monthly}`** (month), **`{bans_received_lifetime}`** (All time)\n"
                    f"Network total: **`{total_federated_actions_lifetime}`** (All time)\n",
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url if interaction.guild.icon else None)

    await interaction.followup.send(embed=embed)

async def add_keyword_to_list(interaction: discord.Interaction, keyword: str, primary_key: str, secondary_key: str = None):
    """A generic helper to add a keyword to a specific list in the keywords.json file."""
    keyword = keyword.lower().strip()
    if not keyword:
        await interaction.followup.send("❌ Keyword cannot be empty.")
        return
    keywords_data = load_keywords()
    if keywords_data is None:
        await interaction.followup.send("❌ Could not load the keywords file. Please check the bot's logs.")
        return
    if secondary_key:
        if primary_key not in keywords_data: keywords_data[primary_key] = {}
        if secondary_key not in keywords_data[primary_key]: keywords_data[primary_key][secondary_key] = []
        target_list = keywords_data[primary_key][secondary_key]
    else:
        if primary_key not in keywords_data: keywords_data[primary_key] = []
        target_list = keywords_data[primary_key]
    if keyword in target_list:
        await interaction.followup.send(f"⚠️ The keyword '{keyword}' is already in that list.")
        return
    target_list.append(keyword)
    save_keywords(keywords_data)
    logger.info(f"Moderator {interaction.user.name} added keyword '{keyword}' to {primary_key}{' -> ' + secondary_key if secondary_key else ''}")
    await interaction.followup.send(f"✅ Keyword '{keyword}' has been successfully added.")

@bot.tree.command(name="add-username-keyword-substring", description="Aggressive name keyword (catches variations: 'admin' in 'daoadmin' 'admin123').")
@discord.app_commands.describe(keyword="Example: 'admin' will match 'listadaoadmin' or 'admin123'.")
async def add_username_keyword_substring(interaction: discord.Interaction, keyword: str):
    # Manual permission check for federated context
    if not await has_federated_mod_role(interaction): return
    await interaction.response.defer(ephemeral=True)
    await add_keyword_to_list(interaction, keyword, "username_keywords", "substring")

@bot.tree.command(name="add-username-keyword-smart", description="Precise name keyword (avoids false positives: 'mod' in 'mod123' not 'modern').")
@discord.app_commands.describe(keyword="Example: 'mod' will match 'mod123' but IGNORE 'modern'.")
async def add_username_keyword_smart(interaction: discord.Interaction, keyword: str):
    if not await has_federated_mod_role(interaction): return
    await interaction.response.defer(ephemeral=True)
    await add_keyword_to_list(interaction, keyword, "username_keywords", "smart")

@bot.tree.command(name="add-bio-keyword", description="Keyword for screening user bios and messages.")
@discord.app_commands.describe(keyword="The keyword or phrase to add (e.g., 'dm me for help').")
async def add_bio_keyword(interaction: discord.Interaction, keyword: str):
    if not await has_federated_mod_role(interaction): return
    await interaction.response.defer(ephemeral=True)
    await add_keyword_to_list(interaction, keyword, "bio_and_message_keywords", "simple_keywords")

@bot.tree.command(name="scanallmembers", description="Retroactively scans all server members against the screening list.")
async def scanallmembers(interaction: discord.Interaction):
    if not await has_federated_mod_role(interaction): return
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
    if not await has_federated_mod_role(interaction): return
    guild_id = interaction.guild.id
    if guild_id in active_scans:
        active_scans[guild_id].cancel()
        logger.info(f"Moderator {interaction.user.name} stopped the scan for guild {guild_id}.")
        await interaction.response.send_message("✅ Scan cancellation requested.", ephemeral=True)
    else:
        await interaction.response.send_message("ℹ️ No scan is currently in progress.", ephemeral=True)

# --- Helper for federated permission checks ---
async def has_federated_mod_role(interaction: discord.Interaction) -> bool:
    """Checks if the user has a whitelisted moderator role for the current guild."""
    if interaction.guild.id not in FEDERATED_GUILD_IDS:
        await interaction.response.send_message("❌ This command can only be used in a federated server.", ephemeral=True)
        return False
    
    whitelisted_mod_roles = MODERATOR_ROLES_PER_GUILD.get(str(interaction.guild.id), [])
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
