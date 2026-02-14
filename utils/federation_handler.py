# /antiscam/utils/federation_handler.py

import discord
from datetime import datetime, timezone
from typing import TYPE_CHECKING
import asyncio
import data_manager
from config import logger

if TYPE_CHECKING:
    from antiscam import AntiScamBot
    from ui.views import FederatedAlertView
    from utils.helpers import get_delete_days_for_guild

# /antiscam/utils/federation_handler.py

async def _ban_single_guild(bot, guild_id, user_to_ban, origin_guild, reason, detailed_reason_field, stats, current_month_key, is_proactive_command, moderator, semaphore):
    """Helper function to handle the ban logic for a single guild with rate limiting and retries."""
    
    async with semaphore:
        target_guild = bot.get_guild(guild_id)
        if not target_guild:
            return

        try:
            await target_guild.fetch_ban(user_to_ban)
            logger.info(f"User {user_to_ban.name} already banned in target {target_guild.name}.")
        except discord.NotFound:
            # Retry Logic
            for attempt in range(3):
                try:
                    fed_reason = f"Federated ban from {origin_guild.name}. Reason: {reason}"
                    
                    if is_proactive_command and target_guild.id == origin_guild.id:
                        fed_reason = f"Proactive ban initiated by {moderator.name}. Reason: {reason}"

                    from utils.helpers import get_delete_days_for_guild
                    delete_seconds = get_delete_days_for_guild(bot, target_guild) * 86400
                    
                    await target_guild.ban(user_to_ban, reason=fed_reason[:512], delete_message_seconds=delete_seconds)
                    logger.info(f"SUCCESS: Banned {user_to_ban.name} from {target_guild.name}.")

                    # Send alert (Alert code remains the same...)
                    mod_channel_id = bot.config.get("federation_notice_channels", {}).get(str(target_guild.id))
                    if mod_channel_id and (mod_channel := bot.get_channel(mod_channel_id)):
                        from ui.views import FederatedAlertView
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
                    
                    return target_guild.id # Success!

                except discord.DiscordServerError as e:
                    # 503s and other server errors. Wait and retry.
                    if attempt < 2:
                        logger.warning(f"Discord Server Error banning in {target_guild.name} (Attempt {attempt+1}/3): {e}. Retrying...")
                        await asyncio.sleep(1 + attempt)
                        continue
                    return None
                
                # <<< START OF FIX >>>
                except discord.HTTPException as e:
                    # Handle Rate Limit for Non-Members (30035)
                    if e.code == 30035:
                        if attempt < 2:
                            logger.warning(f"Hit Non-Member Ban Limit in {target_guild.name} for {user_to_ban.id}. Cooling down for 30s...")
                            await asyncio.sleep(30)
                            continue
                        else:
                            logger.error(f"Failed to ban in {target_guild.name} due to rate limit after retries: {e}")
                            return None
                    
                    # 429s are handled by the library mostly, but if we catch one:
                    if e.status == 429:
                         await asyncio.sleep(2)
                         continue
                    
                    # Other HTTP errors (e.g. Missing Permissions) should fail immediately
                    logger.error(f"HTTP Error banning in {target_guild.name}: {e}")
                    return None
                # <<< END OF FIX >>>

                except Exception as e:
                    logger.error(f"Unexpected error banning in {target_guild.name}: {e}", exc_info=True)
                    return None

        except discord.Forbidden:
            logger.warning(f"Missing permissions to ban in {target_guild.name}.")
            return None
        
    return None

async def process_federated_ban(bot: 'AntiScamBot', origin_guild: discord.Guild, user_to_ban: discord.User, moderator: discord.User, reason: str, detailed_reason_field: dict, is_proactive_command: bool = False):
    """
    The single source of truth for processing, counting, and propagating a federated ban.
    OPTIMIZED: Uses asyncio.gather for concurrent execution.
    """
    stats = await data_manager.load_fed_stats()
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")

    # 1. Update the master ban list (Same as before)
    await data_manager.db_add_ban(
        user_id=user_to_ban.id,
        username=user_to_ban.name,
        reason=reason,
        origin_id=origin_guild.id,
        origin_name=origin_guild.name,
        mod_id=moderator.id,
        timestamp=datetime.now(timezone.utc).isoformat()
        # bio defaults to None, which is correct for a fresh ban
    )
    logger.info(f"Added {user_to_ban.name} to master ban list (DB) from {origin_guild.name}.")

    # 2. Update stats for the origin server (Same as before)
    origin_guild_id_str = str(origin_guild.id)
    guild_stats = stats.setdefault(origin_guild_id_str, {})
    guild_stats["bans_initiated_lifetime"] = guild_stats.get("bans_initiated_lifetime", 0) + 1
    monthly_initiated = guild_stats.setdefault("monthly_initiated", {})
    monthly_initiated[current_month_key] = monthly_initiated.get(current_month_key, 0) + 1
    
    global_stats = stats.setdefault("global", {})
    global_stats["total_federated_actions_lifetime"] = global_stats.get("total_federated_actions_lifetime", 0) + 1

    # 3. Send confirmation message to the ORIGIN server (Same as before)
    if not is_proactive_command:
        origin_mod_channel_id = bot.config.get("federation_notice_channels", {}).get(str(origin_guild.id))
        if origin_mod_channel_id and (origin_mod_channel := bot.get_channel(origin_mod_channel_id)):
            embed_desc = (
                f"The manual for **{user_to_ban.name}** (`{user_to_ban.id}`) has been broadcast to all federated servers.\n\n"
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

    # 4. Propagate the ban to other federated servers CONCURRENTLY
    semaphore = asyncio.Semaphore(10) 

    # Create a list of tasks
    tasks = []
    for guild_id in bot.config.get("federated_guild_ids", []):
        # Note: We are NOT filtering out the origin_guild here anymore, per the previous bug fix.
        # The helper handles the logic.
        task = _ban_single_guild(
            bot, guild_id, user_to_ban, origin_guild, reason, detailed_reason_field, 
            stats, current_month_key, is_proactive_command, moderator,
            semaphore
        )
        tasks.append(task)

    # Run all tasks at once
    results = await asyncio.gather(*tasks)

    # 5. Process results to update stats
    # results contains a list of guild IDs where a ban was successfully applied
    for guild_id_banned in results:
        if guild_id_banned:
            target_guild_id_str = str(guild_id_banned)
            target_stats = stats.setdefault(target_guild_id_str, {})
            target_stats["bans_received_lifetime"] = target_stats.get("bans_received_lifetime", 0) + 1
            monthly_received = target_stats.setdefault("monthly_received", {})
            monthly_received[current_month_key] = monthly_received.get(current_month_key, 0) + 1

    await data_manager.save_fed_stats(stats)


async def process_federated_unban(bot: 'AntiScamBot', origin_guild: discord.Guild, user_to_unban: discord.User, moderator: discord.User, reason: str, is_proactive_command: bool = False):
    """
    The single source of truth for processing, counting, and propagating a federated unban.
    """
    stats = await data_manager.load_fed_stats()
    current_month_key = datetime.now(timezone.utc).strftime("%Y-%m")

    # Check if they exist first (to maintain the logic of "don't unban if not on list")
    existing_ban = await data_manager.db_get_ban(user_to_unban.id)
    
    if existing_ban:
        await data_manager.db_remove_ban(user_to_unban.id)
        logger.info(f"Removed {user_to_unban.name} from master ban list (DB) by {moderator.name}.")
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

    if not is_proactive_command:
        origin_mod_channel_id = bot.config.get("federation_notice_channels", {}).get(str(origin_guild.id))
        if origin_mod_channel_id and (origin_mod_channel := bot.get_channel(origin_mod_channel_id)):
            embed_desc = (
                f"The manual unban by {moderator.mention} for **{user_to_unban.name}** (`{user_to_unban.id}`) has been broadcast to all federated servers.\n\n"
                f"**Reason:**\n```{reason[:1000]}```"
            )
            origin_alert_embed = discord.Embed(
                title="✅ Manual Unban Propagated",
                description=embed_desc,
                color=discord.Color.light_grey(),
                timestamp=datetime.now(timezone.utc)
            )
            try:
                await origin_mod_channel.send(embed=origin_alert_embed)
            except Exception as e:
                logger.error(f"Failed to send manual unban confirmation to {origin_guild.name}: {e}")
                
    # Propagate the unban
    for guild_id in bot.config.get("federated_guild_ids", []):        
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
