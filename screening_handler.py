# /antiscam/screening_handler.py

import os
import re
import aiohttp
import asyncio
import discord
from datetime import datetime, timezone, timedelta
from unidecode import unidecode
from typing import TYPE_CHECKING

from ui.views import ScreeningView
import data_manager
from config import logger, KEYWORDS_FILE

if TYPE_CHECKING:
    from antiscam import AntiScamBot


# --- SCREENING ---
async def screen_member(bot: 'AntiScamBot', member: discord.Member, keywords_data: dict) -> dict:
    """
    Performs the complete screening process for a single member.
    """
    config = bot.config
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
        embed.set_footer(text=f"User ID: {member.id}")

        guild_id_str = str(member.guild.id)
        llm_defaults = config.get("llm_settings", {}).get("defaults", {})
        llm_config = config.get("llm_settings", {}).get("per_guild_settings", {}).get(guild_id_str, llm_defaults)

        if llm_config.get("automation_mode") == "full":
            mod_channel_id = config.get("action_alert_channels", {}).get(str(member.guild.id))
            alert_channel = member.guild.get_channel(mod_channel_id) if mod_channel_id else None
            if alert_channel:
                view = ScreeningView(flagged_member_id=member.id)
                alert_message = await alert_channel.send(embed=embed, view=view)
                
                delay = llm_config.get("automation_delay_seconds", 180)
                ban_reason_detail = f"Banned based on federated status in: {banned_in_servers}"
                
                logger.info(f"Scheduling automated 'Banned Elsewhere' ban for {member.name} in {delay} seconds.")
                task = bot.loop.create_task(
                    delayed_banned_elsewhere_wrapper(delay, bot, alert_message, member, ban_reason_detail)
                )
                bot.pending_ai_actions[alert_message.id] = task
            
            return {}

        else:
            return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}

    identity_result = await check_server_identity(bot, member)
    if identity_result.get("flagged"):
        timeout_reason = identity_result.get("reason")
        embed = discord.Embed(
            title="🚨 Flagged User (Malicious Server Badge)",
            description=f"{member.mention} (`{member.id}`)",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=f"{member.name}", icon_url=member.display_avatar.url)
        embed.add_field(name="🚩 Trigger", value=f"`{timeout_reason}`", inline=False)
        embed.add_field(name="Status", value="User timed out. Awaiting review...", inline=True)
        embed.add_field(name="Account Age", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
        return {"flagged": True, "embed": embed, "timeout_reason": timeout_reason}
    
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

async def screen_message(message: discord.Message, keywords_data: dict) -> dict:
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

async def screen_bio(bot: 'AntiScamBot', member: discord.Member, keywords_data: dict) -> dict:
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

# --- SCREENING HELPERS ---
def get_timeout_minutes_for_guild(bot: 'AntiScamBot', guild: discord.Guild) -> int:
    config = bot.config
    per_guild_settings = config.get("timeout_duration_minutes_per_guild", {})
    guild_id_str = str(guild.id)
    if guild_id_str in per_guild_settings:
        return per_guild_settings[guild_id_str]
    return config.get("timeout_duration_minutes_default", 10)

def get_delete_days_for_guild(bot: 'AntiScamBot', guild: discord.Guild) -> int:
    config = bot.config
    per_guild_settings = config.get("delete_messages_on_ban_days_per_guild", {})
    guild_id_str = str(guild.id)
    if guild_id_str in per_guild_settings:
        return per_guild_settings[guild_id_str]
    return config.get("delete_messages_on_ban_days_default", 1)

def check_text_for_keywords(text_to_check: str, ruleset: dict) -> list:
    if not text_to_check or not ruleset:
        return []

    triggered = []
    normalized_text = unidecode(text_to_check).lower()

    for keyword in ruleset.get("substring", []):
        if keyword.lower() in normalized_text:
            triggered.append(keyword)
    for keyword in ruleset.get("smart", []):
        pattern = r'(?<![a-z])' + re.escape(keyword.lower()) + r'(?![a-z])'
        if re.search(pattern, normalized_text):
            triggered.append(keyword)

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

async def check_server_identity(bot: 'AntiScamBot', member: discord.Member) -> dict:
    bot_token = os.getenv("ANTISCAM_BOT_TOKEN")
    if not bot_token:
        return {"flagged": False}

    url = f"https://discord.com/api/v9/users/{member.id}/profile"
    headers = {"Authorization": f"Bot {bot_token}"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    identity = data.get('guild_member_profile')
                    
                    if identity and identity.get('identity_enabled'):
                        guild_id = identity.get('identity_guild_id')
                        if guild_id and int(guild_id) in bot.scam_server_ids:
                            reason = f"User has a server badge from a known scam server (ID: {guild_id})."
                            logger.info(f"FLAGGED {member.name} for malicious server identity: {reason}")
                            return {"flagged": True, "reason": reason}

                        tag = identity.get('tag', '').lower()
                        if any(susp_tag in tag for susp_tag in bot.suspicious_identity_tags):
                            reason = f"User has a suspicious server badge tag: '{identity.get('tag')}'."
                            logger.info(f"FLAGGED {member.name} for malicious server identity: {reason}")
                            return {"flagged": True, "reason": reason}
    except Exception as e:
        logger.error(f"Error checking server identity for {member.name}: {e}", exc_info=True)

    return {"flagged": False}

async def perform_automated_banned_elsewhere_ban(bot: 'AntiScamBot', alert_message: discord.Message, member: discord.Member, ban_reason_detail: str):
    guild = alert_message.guild
    reason = f"[Automated Action] {ban_reason_detail} | AlertID:{alert_message.id}"

    try:
        delete_days = get_delete_days_for_guild(bot, guild)
        await guild.ban(member, reason=reason, delete_message_seconds=delete_days * 86400)
        logger.info(f"AUTOMATED BAN of {member.name} in {guild.name} (Reason: Banned Elsewhere).")

        embed = alert_message.embeds[0]
        embed.color = discord.Color.red()
        for i, field in enumerate(embed.fields):
            if field.name == "Status":
                embed.set_field_at(i, name="Status", value="🔴 Banned (Automated)", inline=True)
                break
        
        view = ScreeningView(flagged_member_id=member.id)
        view.update_buttons_for_state('banned')
        await alert_message.edit(embed=embed, view=view)

    except Exception as e:
        logger.error(f"Failed to execute automated 'Banned Elsewhere' ban for {member.name}: {e}")

async def delayed_banned_elsewhere_wrapper(delay: int, bot: 'AntiScamBot', alert_message: discord.Message, member: discord.Member, ban_reason_detail: str):
    try:
        await asyncio.sleep(delay)
        
        try:
            await alert_message.channel.fetch_message(alert_message.id)
            if member.guild.get_member(member.id) is None:
                logger.info(f"Automated 'Banned Elsewhere' action for {member.name} cancelled: User no longer in server.")
                return
        except discord.NotFound:
            logger.info(f"Automated 'Banned Elsewhere' action for {member.name} cancelled: Alert was deleted.")
            return

        logger.info(f"Delay complete for {member.name}. Performing automated 'Banned Elsewhere' ban.")
        await perform_automated_banned_elsewhere_ban(bot, alert_message, member, ban_reason_detail)

    except asyncio.CancelledError:
        logger.info(f"Delayed 'Banned Elsewhere' action for {member.name} was cancelled by a moderator.")
    finally:
        if alert_message.id in bot.pending_ai_actions:
            del bot.pending_ai_actions[alert_message.id]

async def run_full_scan(bot: 'AntiScamBot', interaction: discord.Interaction):
    config = bot.config
    guild = interaction.guild
    
    results_channel_id = config.get("action_alert_channels", {}).get(str(guild.id))
    results_channel = guild.get_channel(results_channel_id)

    if not results_channel:
        await interaction.followup.send(f"❌ **Scan Aborted:** Scan results channel not configured for {guild.name}.", ephemeral=True)
        if guild.id in bot.active_scans: del bot.active_scans[guild.id]
        return
    if not guild.chunked:
        await guild.chunk()

    keywords_data = await data_manager.load_keywords()
    if not keywords_data:
        await interaction.followup.send("❌ **Scan Aborted:** Could not load keywords file. Please check logs.", ephemeral=True)
        if guild.id in bot.active_scans: del bot.active_scans[guild.id]
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
            
            result = await screen_member(bot, member, keywords_data)
            
            if result.get("flagged"):
                flagged_count += 1
                try:
                    timeout_minutes = get_timeout_minutes_for_guild(bot, member.guild)
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
        if guild.id in bot.active_scans:
            del bot.active_scans[guild.id]
            logger.info(f"Scan task for guild {guild.id} removed from active tracker.")