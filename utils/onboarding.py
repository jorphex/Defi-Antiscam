"""Checkpointed historical ban application. No live federation policy changes."""
import asyncio

import aiosqlite
import discord

import data_manager
from config import logger
from utils.helpers import get_delete_days_for_guild

CHUNK_SIZE = 100
UNRESOLVED = ("pending", "failed", "applying", "review")


async def run_onboarding(bot, guild, report):
    guild_id = str(guild.id)
    async with aiosqlite.connect(data_manager.DB_FILE) as db:
        # Import retained unbans before starting/resuming. Failure aborts safely;
        # existing synchronized guilds remain protected by the legacy command guard.
        async for entry in guild.audit_logs(action=discord.AuditLogAction.unban, limit=None):
            await db.execute("INSERT OR IGNORE INTO onboarding_exclusions VALUES (?, ?)",
                             (guild_id, str(entry.target.id)))
            await db.commit()

        async with db.execute("SELECT 1 FROM onboarding_runs WHERE guild_id = ?", (guild_id,)) as cursor:
            exists = await cursor.fetchone()
        if not exists:
            await db.execute("INSERT INTO onboarding_runs VALUES (?)", (guild_id,))
            # Snapshot IDs in SQLite, rather than materializing the full ban database in Python.
            await db.execute("INSERT INTO onboarding_items (guild_id, user_id) SELECT ?, user_id FROM bans",
                             (guild_id,))
        # Retry failed work once per invocation. Never reapply successfully processed bans.
        await db.execute("UPDATE onboarding_items SET status = 'pending' WHERE guild_id = ? AND status = 'failed'",
                         (guild_id,))
        await db.commit()

        async def finish(user_id, status):
            await db.execute("UPDATE onboarding_items SET status = ? WHERE guild_id = ? AND user_id = ?",
                             (status, guild_id, user_id))
            await db.commit()

        last_id = ""
        while True:
            async with db.execute("""
                SELECT user_id, status FROM onboarding_items
                WHERE guild_id = ? AND user_id > ? AND status IN ('pending', 'applying', 'review')
                ORDER BY user_id LIMIT ?
            """, (guild_id, last_id, CHUNK_SIZE)) as cursor:
                chunk = await cursor.fetchall()
            if not chunk:
                break
            for user_id, prior_status in chunk:
                last_id = user_id
                async with db.execute("""
                    SELECT reason FROM bans WHERE user_id = ? AND NOT EXISTS (
                        SELECT 1 FROM onboarding_exclusions WHERE guild_id = ? AND user_id = ?
                    )
                """, (user_id, guild_id, user_id)) as cursor:
                    record = await cursor.fetchone()
                if not record or data_manager.is_user_whitelisted(user_id, bot.config):
                    await finish(user_id, "skipped")
                    continue

                user = discord.Object(id=int(user_id))
                try:
                    await guild.fetch_ban(user)
                except discord.NotFound:
                    if prior_status in ("applying", "review"):
                        # A crash may have occurred after Discord applied the ban, followed by
                        # a local unban while offline. Never guess and re-ban this account.
                        await finish(user_id, "review")
                        continue
                except Exception:
                    logger.exception("Onboarding could not check user %s in guild %s", user_id, guild_id)
                    await finish(user_id, "review" if prior_status in ("applying", "review") else "failed")
                    continue
                else:
                    await finish(user_id, "already_banned")
                    continue

                async with data_manager.ban_action_lock(user_id):
                    # Serialize the final recheck and Discord action with master removal/local overrides.
                    async with db.execute("""
                        SELECT reason FROM bans WHERE user_id = ? AND NOT EXISTS (
                            SELECT 1 FROM onboarding_exclusions WHERE guild_id = ? AND user_id = ?
                        )
                    """, (user_id, guild_id, user_id)) as cursor:
                        record = await cursor.fetchone()
                    if not record or data_manager.is_user_whitelisted(user_id, bot.config):
                        await finish(user_id, "skipped")
                        continue
                    await finish(user_id, "applying")
                    try:
                        await guild.ban(user, reason=f"Federated ban sync. Original reason: {record[0] or 'N/A'}"[:512],
                                        delete_message_seconds=get_delete_days_for_guild(bot, guild) * 86400)
                    except discord.HTTPException as exc:
                        # 5xx responses can be ambiguous: verify on resume instead of replaying.
                        await finish(user_id, "review" if exc.status >= 500 else "failed")
                        logger.warning("Onboarding ban failed for %s in %s: %s", user_id, guild_id, exc)
                        if exc.status == 429 or exc.code == 30035:
                            # Stop this pass on throttling. The remaining queue stays pending.
                            await report(await data_manager.get_onboarding_counts(guild.id))
                            return await data_manager.get_onboarding_counts(guild.id)
                    except asyncio.CancelledError:
                        raise  # 'applying' remains durable for conservative recovery.
                    except Exception:
                        await finish(user_id, "review")
                        logger.exception("Onboarding action outcome unknown for %s in %s", user_id, guild_id)
                    else:
                        await finish(user_id, "applied")

            await report(await data_manager.get_onboarding_counts(guild.id))

        counts = await data_manager.get_onboarding_counts(guild.id)
        if not any(counts.get(status, 0) for status in UNRESOLVED):
            await data_manager.mark_onboarding_complete(guild.id)
        return counts
