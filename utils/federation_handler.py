# /antiscam/utils/federation_handler.py

import discord
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import data_manager
from config import logger
from screening_handler import get_delete_days_for_guild
from ui.views import FederatedAlertView

if TYPE_CHECKING:
    from antiscam import AntiScamBot

async def propagate_ban(bot: 'AntiScamBot', origin_guild: discord.Guild, user_to_ban: discord.User, moderator: discord.User, reason: str):
    """Handles the logic of propagating a ban to all federated servers."""
    config = bot.config
    stats = await data_manager.load_fed_stats()
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

    fed_bans = await data_manager.load_fed_bans()
    fed_bans[str(user_to_ban.id)] = {
        "username_at_ban": user_to_ban.name,
        "ban_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "origin_guild_id": origin_guild.id,
        "origin_guild_name": origin_guild.name,
        "reason": reason,
        "initiating_moderator_id": moderator.id
    }
    await data_manager.save_fed_bans(fed_bans)
    
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
                    delete_seconds = get_delete_days_for_guild(bot, target_guild) * 86400
                    await target_guild.ban(user_to_ban, reason=f"Proactive ban initiated by {moderator.name}. Reason: {reason}", delete_message_seconds=delete_seconds)
                    logger.info(f"Successfully banned user in origin guild {target_guild.name} as part of proactive ban.")
                except discord.HTTPException as e:
                    if e.code == 50013: # Missing Permissions
                        logger.warning(f"Missing 'Ban Members' permission in origin guild {target_guild.name} for proactive ban.")
                    else:
                        logger.error(f"Failed to ban user in origin guild during proactive ban: {e}")
            continue

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
                delete_days = get_delete_days_for_guild(bot, target_guild)
                delete_seconds = delete_days * 86400

                await target_guild.ban(user_to_ban, reason=fed_reason[:512], delete_message_seconds=delete_seconds)
                logger.info(f"SUCCESS: Banned {user_to_ban.name} from {target_guild.name}.")
                if log_channel: await log_channel.send(f"✅ Banned `{user_to_ban.name}` in `{target_guild.name}`.")
                
                target_guild_id_str = str(target_guild.id)
                if target_guild_id_str not in stats: stats[target_guild_id_str] = {}
                stats[target_guild_id_str]["bans_received_lifetime"] = stats[target_guild_id_str].get("bans_received_lifetime", 0) + 1
                if "monthly_received" not in stats[target_guild_id_str]: stats[target_guild_id_str]["monthly_received"] = {}
                stats[target_guild_id_str]["monthly_received"][current_month_key] = stats[target_guild_id_str]["monthly_received"].get(current_month_key, 0) + 1

                mod_channel_id = config.get("federation_notice_channels", {}).get(str(target_guild.id))
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
    
    await data_manager.save_fed_stats(stats)