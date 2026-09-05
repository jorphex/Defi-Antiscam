# Shared federation actions and outcomes.
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import discord
import data_manager
from config import logger
from utils.helpers import get_delete_days_for_guild
from utils.checks import federated_ban_block_reason

if TYPE_CHECKING:
    from antiscam import AntiScamBot


@dataclass
class BanResult:
    applied: set[int] = field(default_factory=set)
    already_banned: set[int] = field(default_factory=set)
    failed: dict[int, str] = field(default_factory=dict)
    skipped: str | None = None
    warning: str | None = None

    def banned_in(self, guild_id: int) -> bool:
        return guild_id in self.applied or guild_id in self.already_banned

    @property
    def complete(self) -> bool:
        return not self.skipped and not self.failed and bool(self.applied or self.already_banned)

    def summary(self) -> str:
        if self.skipped:
            return f"Skipped: {self.skipped}"
        text = (f"Bans applied: {len(self.applied)}; already banned: {len(self.already_banned)}; "
                f"failed: {len(self.failed)}.")
        if self.failed:
            details = "; ".join(f"{guild_id}: {reason}" for guild_id, reason in self.failed.items())
            text += f"\nFailed servers: {details}"
        if self.warning:
            text += f"\n{self.warning}"
        return text[:1000]


async def _ban_single_guild(bot, guild_id, user, origin, reason, detail, proactive, moderator):
    async with bot.federation_semaphore:
        guild = bot.get_guild(guild_id)
        if guild is None:
            return guild_id, "failed", "Bot is not in this server"
        try:
            await guild.fetch_ban(user)
            return guild_id, "already_banned", None
        except discord.NotFound:
            pass
        except Exception as exc:
            logger.warning("Could not check ban in %s: %s", guild_id, exc)
            return guild_id, "failed", "Could not check ban status"

        for attempt in range(3):
            try:
                prefix = (f"Proactive ban initiated by {moderator.name}" if proactive and guild.id == origin.id
                          else f"Federated ban from {origin.name}")
                await guild.ban(user, reason=f"{prefix}. Reason: {reason}"[:512],
                                delete_message_seconds=get_delete_days_for_guild(bot, guild) * 86400)
                break
            except discord.HTTPException as exc:
                retryable = exc.status >= 500 or exc.status == 429 or exc.code == 30035
                if retryable and attempt < 2:
                    await asyncio.sleep(30 if exc.code == 30035 else 1 + attempt)
                    continue
                logger.warning("Ban failed in %s: %s", guild_id, exc)
                return guild_id, "failed", f"Discord error {exc.code} (HTTP {exc.status})"
            except Exception as exc:
                logger.exception("Ban failed in %s", guild_id)
                return guild_id, "failed", type(exc).__name__

        # Notification failures must never retry or misreport a successful ban.
        channel_id = bot.config.get("federation_notice_channels", {}).get(str(guild_id))
        channel = bot.get_channel(channel_id) if channel_id else None
        if channel:
            try:
                from ui.views import FederatedAlertView
                embed = discord.Embed(title="🛡️ Federated Ban Received",
                    description=f"**User:** {user.name} (`{user.id}`)\n**Origin:** {origin.name}\n**Action:** Banned.",
                    color=discord.Color.dark_red(), timestamp=datetime.now(timezone.utc))
                embed.add_field(name=detail["name"], value=detail["value"][:1024], inline=False)
                embed.set_footer(text=f"User ID: {user.id}")
                await channel.send(embed=embed, view=FederatedAlertView(banned_user_id=user.id),
                                   allowed_mentions=discord.AllowedMentions.none())
            except Exception:
                logger.exception("Ban succeeded in %s but notification failed", guild_id)
        return guild_id, "applied", None


async def process_federated_ban(bot: 'AntiScamBot', origin_guild: discord.Guild,
        user_to_ban: discord.User, moderator: discord.User, reason: str,
        detailed_reason_field: dict, is_proactive_command: bool = False) -> BanResult:
    result = BanResult()
    result.skipped = await federated_ban_block_reason(bot, user_to_ban, moderator)
    if result.skipped:
        return result

    await data_manager.db_add_ban(user_to_ban.id, user_to_ban.name, reason,
        origin_guild.id, origin_guild.name, moderator.id, datetime.now(timezone.utc).isoformat())
    outcomes = await asyncio.gather(*(
        _ban_single_guild(bot, guild_id, user_to_ban, origin_guild, reason,
                          detailed_reason_field, is_proactive_command, moderator)
        for guild_id in dict.fromkeys(bot.config.get("federated_guild_ids", []))
    ))
    for guild_id, status, error in outcomes:
        if status == "failed":
            result.failed[guild_id] = error
        else:
            getattr(result, status).add(guild_id)

    try:
        # Read/modify/save only after network work, avoiding stale snapshots during propagation.
        stats = await data_manager.load_fed_stats()
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        origin_stats = stats.setdefault(str(origin_guild.id), {})
        origin_stats["bans_initiated_lifetime"] = origin_stats.get("bans_initiated_lifetime", 0) + 1
        monthly = origin_stats.setdefault("monthly_initiated", {})
        monthly[month] = monthly.get(month, 0) + 1
        global_stats = stats.setdefault("global", {})
        global_stats["total_federated_actions_lifetime"] = global_stats.get("total_federated_actions_lifetime", 0) + 1
        for guild_id in result.applied:
            guild_stats = stats.setdefault(str(guild_id), {})
            guild_stats["bans_received_lifetime"] = guild_stats.get("bans_received_lifetime", 0) + 1
            monthly = guild_stats.setdefault("monthly_received", {})
            monthly[month] = monthly.get(month, 0) + 1
        await data_manager.save_fed_stats(stats)
    except Exception:
        # Enforcement has already happened. Preserve its result even if metrics cannot be saved.
        result.warning = "Ban outcomes are shown above; statistics could not be saved."
        logger.exception("Could not save federation statistics")

    if not is_proactive_command:
        channel_id = bot.config.get("federation_notice_channels", {}).get(str(origin_guild.id))
        channel = bot.get_channel(channel_id) if channel_id else None
        if channel:
            try:
                embed = discord.Embed(title="Federated Ban Result",
                    description=f"**User:** {user_to_ban.name} (`{user_to_ban.id}`)\n{result.summary()}",
                    color=discord.Color.green() if result.complete else discord.Color.orange())
                await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
            except Exception:
                logger.exception("Failed to send federation result to %s", origin_guild.id)
    return result


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
