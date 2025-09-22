# /antiscam/utils/federation_handler.py

import discord
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import data_manager
from config import logger
from screening_handler import get_delete_days_for_guild

if TYPE_CHECKING:
    from antiscam import AntiScamBot
    from ui.views import FederatedAlertView

async def process_federated_ban(bot: 'AntiScamBot', origin_guild: discord.Guild, user_to_ban: discord.User, moderator: discord.User, reason: str, detailed_reason_field: dict):
    """
    The single source of truth for processing, counting, and propagating a federated ban.
    """
    from ui.views import FederatedAlertView
    stats = await data_manager.load_fed_stats()
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")

    # Update the master ban list
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
    logger.info(f"Added {user_to_ban.name} to master ban list from {origin_guild.name}.")

    # Update stats for the origin server and global count
    origin_guild_id_str = str(origin_guild.id)
    guild_stats = stats.setdefault(origin_guild_id_str, {})
    guild_stats["bans_initiated_lifetime"] = guild_stats.get("bans_initiated_lifetime", 0) + 1
    monthly_initiated = guild_stats.setdefault("monthly_initiated", {})
    monthly_initiated[current_month_key] = monthly_initiated.get(current_month_key, 0) + 1
    
    global_stats = stats.setdefault("global", {})
    global_stats["total_federated_actions_lifetime"] = global_stats.get("total_federated_actions_lifetime", 0) + 1

    # Send confirmation message to the ORIGIN server
    if not moderator.bot:
        origin_mod_channel_id = bot.config.get("federation_notice_channels", {}).get(str(origin_guild.id))
        if origin_mod_channel_id and (origin_mod_channel := bot.get_channel(origin_mod_channel_id)):
            embed_desc = (
                f"Manual ban  for **{user_to_ban.name}** (`{user_to_ban.id}`) has been broadcast to all federated servers.\n\n"
                f"**Reason:**\n```{reason[:1000]}```"
            )
            origin_alert_embed = discord.Embed(
                title="✅ Manual Ban Propagated",
                description=embed_desc,
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc)
            )
            try:
                await origin_mod_channel.send(embed=origin_alert_embed)
            except Exception as e:
                logger.error(f"Failed to send manual ban confirmation to {origin_guild.name}: {e}")

    # Propagate the ban to other federated servers
    for guild_id in bot.config.get("federated_guild_ids", []):
        if guild_id == origin_guild.id:
            continue
        
        target_guild = bot.get_guild(guild_id)
        if not target_guild:
            continue

        try:
            await target_guild.fetch_ban(user_to_ban)
            logger.info(f"User {user_to_ban.name} already banned in target {target_guild.name}.")
        except discord.NotFound:
            try:
                fed_reason = f"Federated ban from {origin_guild.name}. Reason: {reason}"
                delete_seconds = get_delete_days_for_guild(bot, target_guild) * 86400
                await target_guild.ban(user_to_ban, reason=fed_reason[:512], delete_message_seconds=delete_seconds)
                logger.info(f"SUCCESS: Banned {user_to_ban.name} from {target_guild.name}.")

                # Update stats for the receiving server
                target_guild_id_str = str(target_guild.id)
                target_stats = stats.setdefault(target_guild_id_str, {})
                target_stats["bans_received_lifetime"] = target_stats.get("bans_received_lifetime", 0) + 1
                monthly_received = target_stats.setdefault("monthly_received", {})
                monthly_received[current_month_key] = monthly_received.get(current_month_key, 0) + 1

                # Send alert to the receiving server
                mod_channel_id = bot.config.get("federation_notice_channels", {}).get(str(target_guild.id))
                if mod_channel_id and (mod_channel := bot.get_channel(mod_channel_id)):
                    alert_embed = discord.Embed(
                        title="🛡️ Federated Ban Received",
                        description=f"**User:** {user_to_ban.name} ({user_to_ban.mention}, `{user_to_ban.id}`)\n"
                                    f"**Action:** Automatically banned from this server.\n"
                                    f"**Origin:** **{origin_guild.name}**",
                        color=discord.Color.dark_red(),
                        timestamp=datetime.now(timezone.utc)
                    )
                    alert_embed.add_field(name=detailed_reason_field["name"], value=detailed_reason_field["value"], inline=False)
                    alert_embed.set_author(name=user_to_ban.name, icon_url=user_to_ban.display_avatar.url)
                    alert_embed.set_footer(text=f"User ID: {user_to_ban.id}")
                    view = FederatedAlertView(banned_user_id=user_to_ban.id)
                    allowed_mentions = discord.AllowedMentions(users=[user_to_ban])
                    await mod_channel.send(embed=alert_embed, view=view, allowed_mentions=allowed_mentions)

            except Exception as e:
                logger.error(f"Error during federated ban propagation to {target_guild.name}: {e}", exc_info=True)

    await data_manager.save_fed_stats(stats)


async def process_federated_unban(bot: 'AntiScamBot', origin_guild: discord.Guild, user_to_unban: discord.User, moderator: discord.User, reason: str):
    """
    The single source of truth for processing, counting, and propagating a federated unban.
    """
    stats = await data_manager.load_fed_stats()
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")

    # Remove from the master ban list
    fed_bans = await data_manager.load_fed_bans()
    if str(user_to_unban.id) in fed_bans:
        del fed_bans[str(user_to_unban.id)]
        await data_manager.save_fed_bans(fed_bans)
        logger.info(f"Removed {user_to_unban.name} from master ban list by {moderator.name}.")
    else:
        logger.warning(f"Tried to process global unban for {user_to_unban.name}, but they were not on the master list.")
        return

    # Update stats for the origin server and global count
    origin_guild_id_str = str(origin_guild.id)
    guild_stats = stats.setdefault(origin_guild_id_str, {})
    guild_stats["unbans_initiated_lifetime"] = guild_stats.get("unbans_initiated_lifetime", 0) + 1
    monthly_unbanned = guild_stats.setdefault("monthly_unbanned", {})
    monthly_unbanned[current_month_key] = monthly_unbanned.get(current_month_key, 0) + 1
    
    global_stats = stats.setdefault("global", {})
    global_stats["total_federated_actions_lifetime"] = global_stats.get("total_federated_actions_lifetime", 0) + 1

    # Propagate the unban
    for guild_id in bot.config.get("federated_guild_ids", []):
        if guild_id == origin_guild.id:
            continue
        
        target_guild = bot.get_guild(guild_id)
        if not target_guild:
            continue

        try:
            await target_guild.fetch_ban(user_to_unban)
            fed_reason = f"Federated unban from {origin_guild.name}. Reason: {reason}"
            await target_guild.unban(user_to_unban, reason=fed_reason[:512])
            logger.info(f"SUCCESS: Unbanned {user_to_unban.name} from {target_guild.name}.")

            # Update stats for the receiving server
            target_guild_id_str = str(target_guild.id)
            target_stats = stats.setdefault(target_guild_id_str, {})
            target_stats["bans_received_lifetime"] = max(0, target_stats.get("bans_received_lifetime", 0) - 1)

            # Send alert to the target guild
            mod_channel_id = bot.config.get("federation_notice_channels", {}).get(str(target_guild.id))
            if mod_channel_id and (mod_channel := bot.get_channel(mod_channel_id)):
                alert_embed = discord.Embed(
                    title="ℹ️ Federated Unban Received",
                    description=f"**User:** {user_to_unban.name} (`{user_to_unban.id}`)\n"
                                f"**Action:** Automatically unbanned from this server.\n"
                                f"**Origin:** **{origin_guild.name}**",
                    color=discord.Color.green(),
                    timestamp=datetime.now(timezone.utc)
                )
                alert_embed.add_field(name="Reason", value=f"```{reason}```", inline=False)
                alert_embed.set_footer(text=f"User ID: {user_to_unban.id}")
                await mod_channel.send(embed=alert_embed)

        except discord.NotFound:
            logger.info(f"User {user_to_unban.name} was not banned in {target_guild.name}, skipping unban.")
        except discord.Forbidden:
            logger.error(f"Failed to unban {user_to_unban.name} in {target_guild.name} - Missing Permissions.")
        except Exception as e:
            logger.error(f"Error during federated unban propagation to {target_guild.name}: {e}", exc_info=True)

    await data_manager.save_fed_stats(stats)
