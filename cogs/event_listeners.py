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
from utils.helpers import get_delete_days_for_guild, get_timeout_minutes_for_guild
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

        keywords_data = await data_manager.load_keywords()
        if not keywords_data:
            logger.error("Could not load keywords for on_member_join screen. Aborting check.")
            return

        result = await screening_handler.screen_member(self.bot, member, keywords_data)

        if result.get("flagged"):
            logger.info(f"FLAGGED member on join: {member.name} in {member.guild.name}.")
            mod_channel_id = config.get("action_alert_channels", {}).get(str(member.guild.id))
            alert_channel = member.guild.get_channel(mod_channel_id) if mod_channel_id else None
        
            if alert_channel:
                try:
                    timeout_minutes = get_timeout_minutes_for_guild(self.bot, member.guild)
                    await member.timeout(timedelta(minutes=timeout_minutes), reason=result.get("timeout_reason"))
                
                    view = ScreeningView(flagged_member_id=member.id)
                    embed = result.get("embed")
                    embed.set_footer(text=f"User ID: {member.id}")

                    guild_id_str = str(member.guild.id)
                    llm_defaults = config.get("llm_settings", {}).get("defaults", {})
                    llm_config = config.get("llm_settings", {}).get("per_guild_settings", {}).get(guild_id_str, llm_defaults)

                    if self.gemini_is_available and llm_config.get("automation_mode", "off") != "off":
                        bio = getattr(await self.bot.fetch_user(member.id), 'bio', "")
                        self.bot.loop.create_task(llm_handler.start_llm_analysis_task(
                            bot=self.bot,
                            alert_channel=alert_channel,
                            embed=embed,
                            view=view,
                            flagged_member=member,
                            content_type="Bio/Username",
                            content=f"Username: {member.name}\nNick: {member.nick}\nBio: {bio}",
                            trigger=result.get("timeout_reason")
                        ))
                    else:
                        allowed_mentions = discord.AllowedMentions(users=[member])
                        await alert_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)

                except Exception as e:
                    logger.error(f"Failed to take action on flagged member {member.name}: {e}", exc_info=True)
        elif not result:
            logger.info(f"Member {member.name} in {member.guild.name} flagged as 'Banned Elsewhere'. Automated action is pending.")
        else:
            logger.info(f"Member {member.name} in {member.guild.name} passed all screenings.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or message.author == self.bot.user:
            return
        
        config = self.bot.config
        bot_owner_id = config.get("bot_owner_id")
        if bot_owner_id and message.author.id == bot_owner_id:
            return
    
        if not message.guild or message.guild.id not in config.get("federated_guild_ids", []): return
        if not isinstance(message.author, discord.Member): return
    
        whitelisted_roles = config.get("whitelisted_roles_per_guild", {}).get(str(message.guild.id), [])
        if any(role.id in whitelisted_roles for role in message.author.roles): return

        keywords_data = await data_manager.load_keywords()
        if not keywords_data: return

        result = await screening_handler.screen_message(message, keywords_data)

        if not result.get("flagged"):
            author_id = message.author.id
            current_time = datetime.now(timezone.utc)
        
            if author_id in self.bot.bio_check_cache:
                last_checked = self.bot.bio_check_cache[author_id]
                if (current_time - last_checked).total_seconds() < 300:
                    return

            bio_result = await screening_handler.screen_bio(self.bot, message.author, keywords_data)
            self.bot.bio_check_cache[author_id] = current_time

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
        
            if "Message" in result["embed"].title:
                try: await message.delete()
                except Exception as e: logger.error(f"Error deleting flagged message: {e}")
        
            try:
                timeout_minutes = get_timeout_minutes_for_guild(self.bot, author.guild)
                await author.timeout(timedelta(minutes=timeout_minutes), reason=result.get("timeout_reason", "Flagged content."))
            except discord.HTTPException as e:
                if e.code == 40007:
                    logger.warning(f"Could not timeout {author.name} because they have already left the server.")
                else:
                    logger.error(f"Failed to timeout {author.name}: {e}")
            except Exception as e:
                 logger.error(f"An unexpected error occurred while trying to timeout {author.name}: {e}")
        
            mod_channel_id = config.get("action_alert_channels", {}).get(str(message.guild.id))
            alert_channel = message.guild.get_channel(mod_channel_id) if mod_channel_id else None
            if alert_channel:
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

                        self.bot.loop.create_task(llm_handler.start_llm_analysis_task(
                            bot=self.bot,
                            alert_channel=alert_channel,
                            embed=embed,
                            view=view,
                            flagged_member=author,
                            content_type=content_type,
                            content=content,
                            trigger=result.get("timeout_reason")
                        ))
                    else:
                        allowed_mentions = discord.AllowedMentions(users=[author])
                        await alert_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)
                except Exception as e:
                    logger.error(f"Failed to send alert for {author.name}: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        config = self.bot.config
        if guild.id not in config.get("federated_guild_ids", []): return
    
        await asyncio.sleep(2) # Wait for audit log
        try:
            async for entry in guild.audit_logs(action=discord.AuditLogAction.ban, limit=5):
                if entry.target.id == user.id:
                    moderator, ban_reason = entry.user, entry.reason or "No reason provided."
                    break
            else:
                logger.info(f"Ban of {user} in {guild.name} could not be attributed. No federated action.")
                return
        except Exception as e:
            logger.error(f"Error fetching audit logs in {guild.name}: {e}", exc_info=True)
            return
        
        if ban_reason and ("Federated ban" in ban_reason or "Proactive ban" in ban_reason or "[Automated Action]" in ban_reason):
            logger.info(f"Ignoring federated/automated ban echo for {user.name}.")
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

        fed_bans = await data_manager.load_fed_bans()
        if str(user.id) in fed_bans:
            logger.info(f"Ignoring manual ban of {user.name} as they are already on the master ban list.")
            return

        logger.info(f"Manual ban by {moderator.name} in {guild.name} is authorized for federation.")
        detailed_reason = {"name": "Ban Reason", "value": f"```{ban_reason[:1000]}```"}
        await process_federated_ban(self.bot, guild, user, moderator, ban_reason, detailed_reason)

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        config = self.bot.config
        if guild.id not in config.get("federated_guild_ids", []): return

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
            logger.info(f"Ignoring local-only or federated unban echo for {user.name}.")
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
