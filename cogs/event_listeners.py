# /antiscam/cogs/event_listeners.py

import discord
from discord.ext import commands
import asyncio
from datetime import timedelta, datetime, timezone
import re
from typing import TYPE_CHECKING

import data_manager
import llm_handler
import screening_handler
from config import logger
from ui.views import ScreeningView, FederatedAlertView, FederatedUnbanAlertView
from utils.helpers import get_timeout_minutes_for_guild
from utils.federation_handler import process_federated_ban, process_federated_unban

if TYPE_CHECKING:
    from antiscam import AntiScamBot

class EventListeners(commands.Cog):
    def __init__(self, bot: 'AntiScamBot'):
        self.bot = bot
        self.gemini_is_available = False

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f'{self.bot.user.name} has connected to Discord!')

        self.gemini_is_available = llm_handler.initialize_gemini()
        if self.gemini_is_available:
            logger.info("Gemini client initialized successfully.")

        logger.info(f"Operating in {len(self.bot.guilds)} federated guilds.")
        self.bot.add_view(ScreeningView())
        self.bot.add_view(FederatedAlertView())
        self.bot.add_view(FederatedUnbanAlertView())

        keywords_data = await data_manager.load_keywords()
        if keywords_data:
            self.bot.suspicious_identity_tags = keywords_data.get("global_keywords", {}).get("suspicious_identity_tags", [])
        
        logger.info(f"Loaded {len(self.bot.suspicious_identity_tags)} suspicious identity tags.")
        logger.info(f"Loaded {len(self.bot.scam_server_ids)} known scam server IDs.")

    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        logger.info(f"Joined new guild: {guild.name} ({guild.id}). Setting up command permissions.")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        config = self.bot.config
        bot_owner_id = config.get("bot_owner_id")
        if bot_owner_id and member.id == bot_owner_id:
            logger.info(f"Bot owner ({member.name}) joined {member.guild.name}. Skipping screening.")
            return

        if member.guild.id not in config.get("federated_guild_ids", []):
            return
    
        await asyncio.sleep(2)
        try:
            full_member = await member.guild.fetch_member(member.id)
        except discord.NotFound:
            logger.warning(f"Member {member.name} left before they could be processed.")
            return

        whitelisted_roles = config.get("whitelisted_roles_per_guild", {}).get(str(full_member.guild.id), [])
        if any(role.id in whitelisted_roles for role in full_member.roles):
            logger.info(f"Member {full_member.name} has a whitelisted role. Skipping screen.")
            return

        keywords_data = await data_manager.load_keywords()
        if not keywords_data:
            logger.error("Could not load keywords for on_member_join screen. Aborting check.")
            return

        result = await screening_handler.screen_member(self.bot, full_member, keywords_data)

        if result.get("flagged"):
            logger.info(f"FLAGGED member on join: {full_member.name} in {full_member.guild.name}.")
            mod_channel_id = config.get("action_alert_channels", {}).get(str(full_member.guild.id))
            alert_channel = full_member.guild.get_channel(mod_channel_id) if mod_channel_id else None
        
            if alert_channel:
                try:
                    timeout_minutes = get_timeout_minutes_for_guild(self.bot, full_member.guild)
                    await full_member.timeout(timedelta(minutes=timeout_minutes), reason=result.get("timeout_reason"))
                
                    view = ScreeningView(flagged_member_id=full_member.id)
                    embed = result.get("embed")
                    embed.set_footer(text=f"User ID: {full_member.id}")

                    guild_id_str = str(full_member.guild.id)
                    llm_defaults = config.get("llm_settings", {}).get("defaults", {})
                    llm_config = config.get("llm_settings", {}).get("per_guild_settings", {}).get(guild_id_str, llm_defaults)

                    if self.gemini_is_available and llm_config.get("automation_mode", "off") != "off":
                        bio = getattr(await self.bot.fetch_user(full_member.id), 'bio', "")
                        self.bot.loop.create_task(llm_handler.start_llm_analysis_task(
                            bot=self.bot,
                            alert_channel=alert_channel,
                            embed=embed,
                            view=view,
                            flagged_member=full_member,
                            content_type="Bio/Username",
                            content=f"Username: {full_member.name}\nNick: {full_member.nick}\nBio: {bio}",
                            trigger=result.get("timeout_reason")
                        ))
                    else:
                        allowed_mentions = discord.AllowedMentions(users=[full_member])
                        await alert_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)

                except Exception as e:
                    logger.error(f"Failed to take action on flagged member {full_member.name}: {e}", exc_info=True)
        elif not result:
            logger.info(f"Member {full_member.name} in {full_member.guild.name} flagged as 'Banned Elsewhere'. Automated action is pending.")
        else:
            logger.info(f"Member {full_member.name} in {full_member.guild.name} passed all screenings.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # --- Initial Checks ---
        if message.author.bot or message.author == self.bot.user:
            return
        
        config = self.bot.config
        bot_owner_id = config.get("bot_owner_id")
        if bot_owner_id and message.author.id == bot_owner_id:
            return
    
        if not message.guild or message.guild.id not in config.get("federated_guild_ids", []):
            return
        if not isinstance(message.author, discord.Member):
            return
    
        whitelisted_roles = config.get("whitelisted_roles_per_guild", {}).get(str(message.guild.id), [])
        if any(role.id in whitelisted_roles for role in message.author.roles):
            return

        # --- Logic Block ---
        result = {}

        # 1. Image Wall Detection (High Priority Content Check)
        cdn_regex_pattern = r"(?i)https?://(?:media|cdn)\.discordapp\.(?:net|com)/attachments/\d+/\d+/[^\\s]+"
        cdn_links_found = re.findall(cdn_regex_pattern, message.content)
        num_cdn_links = len(cdn_links_found)

        if num_cdn_links >= 2:
            # Check if the message is ONLY the links with minimal other text
            remaining_text = message.content
            for link in cdn_links_found:
                remaining_text = remaining_text.replace(link, "")
            
            if len(remaining_text.strip()) < 15: # Low threshold for non-link text
                timeout_reason = "Image-based scam detected (multiple CDN links)."
                embed = discord.Embed(
                    title="🖼️ Image-Based Scam Detected",
                    description=f"**User:** {message.author.mention} (`{message.author.id}`)\n"
                                f"Posted a wall of images, which is a common scam vector.",
                    color=discord.Color.dark_orange(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_author(name=f"{message.author.name}", icon_url=message.author.display_avatar.url)
                embed.add_field(name="📝 Message Content", value=f"```{message.content[:1000]}```", inline=False)
                embed.add_field(name="🚩 Trigger", value="`Image Wall Detection`", inline=True)
                embed.add_field(name="Status", value="Message deleted. User timed out. Awaiting review...", inline=True)
                
                result = {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}

        # 1. Flood Detection Check (High Priority)
        elif screening_handler.check_for_flood(self.bot, message):
            timeout_reason = "Flood detection triggered."
            embed = discord.Embed(
                title="🚨 Flood Detected",
                description=f"**User:** {message.author.mention} (`{message.author.id}`)\n"
                            f"Sent multiple messages across several channels in a short period.",
                color=discord.Color.dark_red(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_author(name=f"{message.author.name}", icon_url=message.author.display_avatar.url)
            embed.add_field(name="📝 Sample Message", value=f"```{message.content[:1000]}```", inline=False)
            embed.add_field(name="🚩 Trigger", value="`Flood Detection`", inline=True)
            embed.add_field(name="Status", value="Message deleted. User timed out. Awaiting review...", inline=True)
            
            result = {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}

        # 2. Content & Bio Screening (If no flood was detected)
        elif not result.get("flagged"):
            keywords_data = await data_manager.load_keywords()
            if not keywords_data:
                return

            content_result = await screening_handler.screen_message(message, keywords_data)
            if content_result.get("flagged"):
                result = content_result
            else:
                author_id = message.author.id
                current_time = datetime.now(timezone.utc)
                
                if author_id not in self.bot.bio_check_cache or \
                   (current_time - self.bot.bio_check_cache[author_id]).total_seconds() > 300:
                    
                    bio_result = await screening_handler.screen_bio(self.bot, message.author, keywords_data)
                    self.bot.bio_check_cache[author_id] = current_time

                    if bio_result.get("flagged"):
                        embed = bio_result.get("embed")
                        embed.description = (
                            f"**User:** {message.author.mention} (`{message.author.id}`)\n"
                            f"This user sent a valid message in {message.channel.mention}, but their bio was flagged upon inspection."
                        )
                        result = bio_result

        # 3. Unified Action Block (Handles ANY flagged result)
        if result.get("flagged"):
            author = message.author
            trigger_type = result["embed"].title
            logger.info(f"FLAGGED event for {author.name} in #{message.channel.name} (Trigger: {trigger_type}).")
        
            try:
                await message.delete()
            except Exception as e:
                logger.error(f"Error deleting flagged message: {e}")
        
            try:
                timeout_minutes = get_timeout_minutes_for_guild(self.bot, author.guild)
                await author.timeout(timedelta(minutes=timeout_minutes), reason=result.get("timeout_reason", "Flagged content."))
            except Exception as e:
                 logger.error(f"An unexpected error occurred while trying to timeout {author.name}: {e}")
        
            mod_channel_id = config.get("action_alert_channels", {}).get(str(message.guild.id))
            if mod_channel_id and (alert_channel := self.bot.get_channel(mod_channel_id)):
                try:
                    view = ScreeningView(flagged_member_id=author.id)
                    embed = result.get("embed")
                    embed.set_footer(text=f"User ID: {author.id}")

                    guild_id_str = str(message.guild.id)
                    llm_defaults = config.get("llm_settings", {}).get("defaults", {})
                    llm_config = config.get("llm_settings", {}).get("per_guild_settings", {}).get(guild_id_str, llm_defaults)

                    if self.gemini_is_available and llm_config.get("automation_mode", "off") != "off":
                        content_type = "Message"
                        content = message.content
                        if "Bio" in embed.title:
                            content_type = "Bio"
                            bio = getattr(await self.bot.fetch_user(author.id), 'bio', "")
                            content = bio
                        elif "Flood" in embed.title:
                            content_type = "Message (Flood)"

                        self.bot.loop.create_task(llm_handler.start_llm_analysis_task(
                            bot=self.bot, alert_channel=alert_channel, embed=embed, view=view,
                            flagged_member=author, content_type=content_type, content=content,
                            trigger=result.get("timeout_reason")
                        ))
                    else:
                        allowed_mentions = discord.AllowedMentions(users=[author])
                        await alert_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)
                except Exception as e:
                    logger.error(f"Failed to send alert for {author.name}: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        """
        Catches the 'Edit Bypass' exploit.
        If a user edits a message, re-submit it to the main on_message handler
        to check against regex/keywords again.
        """
        # 1. Ignore bots
        if after.author.bot:
            return

        # 2. Ignore edits that didn't change the text content.
        # Discord triggers an 'edit' event when it adds a link preview (embed) 
        # or when a message is pinned. We don't want to re-scan those.
        if before.content == after.content:
            return

        # 3. Treat the edited message ('after') as if it were a brand new message.
        # This runs it through Flood Detection, Bio Checks, and YOUR REGEX SCREENING.
        await self.on_message(after)

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        config = self.bot.config
        if guild.id not in config.get("federated_guild_ids", []):
            return
    
        await asyncio.sleep(2) # Wait for audit log
        try:
            async for entry in guild.audit_logs(action=discord.AuditLogAction.ban, limit=5):
                if entry.target.id == user.id:
                    moderator = entry.user
                    
                    raw_reason = entry.reason
                    
                    if not raw_reason:
                        ban_reason = config.get("manual_ban_default_reason", "Scam link")
                    else:
                        ban_reason = raw_reason
                    # <<< END OF FIX >>>

                    break
            else:
                logger.info(f"Ban of {user} in {guild.name} could not be attributed. No federated action.")
                return
        except Exception as e:
            logger.error(f"Error fetching audit logs in {guild.name}: {e}", exc_info=True)
            return
        
        if ban_reason and ("Federated ban" in ban_reason or "Proactive ban" in ban_reason or "[Automated Action]" in ban_reason):
            logger.debug(f"Ignoring federated/automated ban echo for {user.name}.")
            return

        if moderator.bot:
            logger.info(f"Ignoring ban by bot {moderator.name} that wasn't an explicit federated action.")
            return

        if not isinstance(moderator, discord.Member):
            logger.warning(f"User {user} was banned by {moderator} who is no longer in the server.")
            return
            
        whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(guild.id), [])
        if not any(role.id in whitelisted_mod_roles for role in moderator.roles):
            logger.warning(f"User {user} was banned by {moderator}, but they do not have a whitelisted role.")
            return

        existing_ban = await data_manager.db_get_ban(user.id)
        if existing_ban:
            logger.info(f"Ignoring manual ban of {user.name} as they are already on the master ban list.")
            return

        logger.info(f"Manual ban by {moderator.name} in {guild.name} is authorized for federation.")
        detailed_reason = {"name": "Ban Reason", "value": f"```{ban_reason[:1000]}```"}
        await process_federated_ban(self.bot, guild, user, moderator, ban_reason, detailed_reason)

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        config = self.bot.config
        if guild.id not in config.get("federated_guild_ids", []):
            return

        await asyncio.sleep(2)
        try:
            async for entry in guild.audit_logs(action=discord.AuditLogAction.unban, limit=5):
                if entry.target.id == user.id:
                    moderator, unban_reason = entry.user, entry.reason or "No reason provided."
                    break
            else:
                logger.info(f"Unban of {user} in {guild.name} could not be attributed.")
                return
        except Exception as e:
            logger.error(f"Error fetching audit logs for unban in {guild.name}: {e}", exc_info=True)
            return

        if unban_reason and ("[Local Action]" in unban_reason or "Federated unban" in unban_reason):
            logger.debug(f"Ignoring local-only or federated unban echo for {user.name}.")
            return

        if moderator.bot and moderator.id != self.bot.user.id:
            logger.info(f"Ignoring unban by unauthorized bot {moderator.name}.")
            return
        
        is_global_action = unban_reason.startswith("[Federated Action]")

        if not moderator.bot:
            if not isinstance(moderator, discord.Member):
                logger.warning(f"User {user} was unbanned by {moderator} who is no longer in the server.")
                return
            whitelisted_mod_roles = config.get("moderator_roles_per_guild", {}).get(str(guild.id), [])
            if any(role.id in whitelisted_mod_roles for role in moderator.roles):
                is_global_action = True
            else:
                logger.warning(f"User {user} was unbanned by {moderator}, but they do not have a whitelisted role.")
                return
        
        if is_global_action:
            logger.info(f"Global unban by {moderator.name} in {guild.name} is authorized for federation.")
            await process_federated_unban(self.bot, guild, user, moderator, unban_reason)
        
async def setup(bot):
    await bot.add_cog(EventListeners(bot))
