"""Offline regressions: Discord/Gemini are mocked; storage lives in temporary directories."""
import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, patch

import discord

import data_manager as dm
import llm_handler as llm
import screening_handler as screening
from ui.views import ScreeningView, FederatedAlertView, FederatedUnbanAlertView
from utils.checks import check_moderator
from utils.federation_handler import BanResult, process_federated_ban
from utils.onboarding import run_onboarding


def http_error(status=404, code=10026):
    cls = discord.NotFound if status == 404 else discord.HTTPException
    return cls(NS(status=status, reason="test"), {"message": "test failure", "code": code})


def member(uid=111):
    return NS(id=uid, name=f"user{uid}", nick=None, global_name=None, roles=[],
              guild=NS(id=1, name="Test"), created_at=datetime.now(timezone.utc),
              mention=f"<@{uid}>", display_avatar=NS(url="https://example.com/avatar"))


def interaction(uid=111):
    embed = discord.Embed(title="Flagged User", description="Flagged")
    embed.set_footer(text=f"User ID: {uid}")
    embed.add_field(name="Trigger", value="test rule")
    embed.add_field(name="Status", value="Awaiting review")
    bot = NS(config={"federated_guild_ids": [1], "moderator_roles_per_guild": {"1": [9]}},
             pending_ai_actions={}, get_user=lambda value: member(value))
    guild = NS(id=1, name="Test", get_member=lambda value: member(value),
               fetch_ban=AsyncMock(), unban=AsyncMock(), ban=AsyncMock())
    return NS(client=bot, guild=guild, user=NS(id=5, name="Mod", mention="<@5>", roles=[NS(id=9)]),
              message=NS(id=uid+1000, embeds=[embed], edit=AsyncMock(), delete=AsyncMock()),
              response=NS(defer=AsyncMock(), send_message=AsyncMock(), is_done=lambda: True),
              followup=NS(send=AsyncMock(), edit_message=AsyncMock()), edit_original_response=AsyncMock())


class AlertTests(unittest.IsolatedAsyncioTestCase):
    async def test_authorization_shared_by_all_persistent_views(self):
        for view_type in (ScreeningView, FederatedAlertView, FederatedUnbanAlertView):
            view = view_type()
            event = interaction()
            self.assertTrue(await view.interaction_check(event))
            event.user.roles = []
            self.assertFalse(await view.interaction_check(event))
            event.response.send_message.assert_awaited_once()
            event.client.config["bot_owner_id"] = event.user.id
            self.assertTrue(await view.interaction_check(event))
        event = interaction()
        event.guild.id = 2
        self.assertFalse(await check_moderator(event))

    async def test_restored_screening_view_resolves_each_alert(self):
        view = ScreeningView()
        for uid in (111, 222, 333):
            user, _ = await view.get_user_and_member(interaction(uid))
            self.assertEqual(user.id, uid)
        self.assertIsNone(view.flagged_member_id)
        invalid = interaction()
        invalid.message.embeds[0].set_footer(text="Invalid")
        self.assertEqual(await view.get_user_and_member(invalid), (None, None))

    async def test_restored_local_actions_use_each_alert_and_keep_view_independent(self):
        for view_type, button_name, action in (
            (FederatedAlertView, "unban_button", "unban"),
            (FederatedUnbanAlertView, "reban_button", "ban"),
        ):
            view = view_type()
            with patch.object(dm, "record_onboarding_unban", new_callable=AsyncMock) as exclusion:
                for uid in (111, 222):
                    event = interaction(uid)
                    await getattr(view, button_name).callback(event)
                    self.assertEqual(getattr(event.guild, action).await_args.args[0].id, uid)
                    self.assertFalse(getattr(view, button_name).disabled)
                if action == "unban":
                    self.assertEqual(exclusion.await_count, 2)

    async def test_screening_ban_uses_federation_and_reports_partial_failure(self):
        event = interaction()
        result = BanResult(applied={1}, failed={2: "Missing permissions"})
        with patch("ui.views.process_federated_ban", new_callable=AsyncMock, return_value=result) as handler:
            await ScreeningView().ban_button.callback(event)
        handler.assert_awaited_once()
        event.guild.ban.assert_not_awaited()
        self.assertEqual(handler.await_args.args[3], event.user)
        update = event.followup.edit_message.await_args.kwargs
        self.assertIn("failed: 1", next(f.value for f in update["embed"].fields if f.name == "Status"))
        self.assertTrue(update["view"].ban_button.disabled)

    async def test_whitelisted_skip_does_not_show_success(self):
        event = interaction()
        with patch("ui.views.process_federated_ban", new_callable=AsyncMock,
                   return_value=BanResult(skipped="User is on the global whitelist.")):
            await ScreeningView().ban_button.callback(event)
        update = event.followup.edit_message.await_args.kwargs
        self.assertFalse(update["view"].ban_button.disabled)
        self.assertIn("Skipped", next(f.value for f in update["embed"].fields if f.name == "Status"))


class FederationTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_outcomes_and_notification_failure(self):
        guilds = {i: NS(id=i, name=str(i), fetch_ban=AsyncMock(side_effect=http_error()), ban=AsyncMock())
                  for i in (1, 2, 3)}
        guilds[2].fetch_ban = AsyncMock(return_value=NS(reason="existing"))
        guilds[3].ban.side_effect = http_error(403, 50013)
        channel = NS(send=AsyncMock(side_effect=http_error(403, 50013)))
        bot = NS(config={"federated_guild_ids": [1, 2, 3, 4], "federation_notice_channels": {"1": 10}},
                 federation_semaphore=asyncio.Semaphore(2), get_guild=guilds.get, get_channel=lambda _: channel)
        with patch.object(dm, "db_add_ban", new_callable=AsyncMock), \
             patch.object(dm, "load_fed_stats", new_callable=AsyncMock, return_value={}), \
             patch.object(dm, "save_fed_stats", new_callable=AsyncMock) as save:
            result = await process_federated_ban(bot, guilds[1], member(), member(5), "test",
                                                {"name": "Reason", "value": "test"}, True)
        self.assertEqual(result.applied, {1})
        self.assertEqual(result.already_banned, {2})
        self.assertEqual(set(result.failed), {3, 4})
        guilds[1].ban.assert_awaited_once()  # Alert failure did not retry enforcement.
        self.assertFalse(result.complete)
        self.assertEqual(save.await_args.args[0]["1"]["bans_received_lifetime"], 1)

    async def test_whitelist_blocks_storage_and_propagation(self):
        bot = NS(config={"whitelisted_user_ids": [111]})
        with patch.object(dm, "db_add_ban", new_callable=AsyncMock) as add:
            result = await process_federated_ban(bot, NS(id=1), member(), member(5), "test", {})
        add.assert_not_awaited()
        self.assertIsNotNone(result.skipped)

    async def test_statistics_failure_does_not_hide_successful_bans(self):
        guild = NS(id=1, name="Test", fetch_ban=AsyncMock(side_effect=http_error()), ban=AsyncMock())
        bot = NS(config={"federated_guild_ids": [1]}, federation_semaphore=asyncio.Semaphore(1),
                 get_guild=lambda _: guild)
        with patch.object(dm, "db_add_ban", new_callable=AsyncMock), \
             patch.object(dm, "load_fed_stats", new_callable=AsyncMock, return_value={}), \
             patch.object(dm, "save_fed_stats", new_callable=AsyncMock, side_effect=OSError("test disk failure")):
            result = await process_federated_ban(bot, guild, member(), member(5), "test", {}, True)
        self.assertEqual(result.applied, {1})
        self.assertIn("statistics could not be saved", result.summary())

    async def test_protected_targets_rechecked_at_execution(self):
        guild = NS(id=1, name="Test", fetch_member=AsyncMock(return_value=NS(roles=[NS(id=9)])))
        bot = NS(config={"federated_guild_ids": [1], "moderator_roles_per_guild": {"1": [9]}, "bot_owner_id": 111},
                 get_guild=lambda _: guild)
        with patch.object(dm, "db_add_ban", new_callable=AsyncMock) as add:
            owner = await process_federated_ban(bot, guild, member(111), member(5), "test", {})
            self.assertIn("owner", owner.skipped)
            protected = await process_federated_ban(bot, guild, member(222), member(5), "test", {})
            self.assertIn("moderators", protected.skipped)
            bot_account = member(333)
            bot_account.bot = True
            result = await process_federated_ban(bot, guild, bot_account, member(5), "test", {})
            self.assertIn("Bot accounts", result.skipped)
            add.assert_not_awaited()

    async def test_failed_protected_target_lookup_cannot_authorize_ban(self):
        guild = NS(id=1, name="Test", fetch_member=AsyncMock(side_effect=http_error(403, 50013)))
        bot = NS(config={"federated_guild_ids": [1], "moderator_roles_per_guild": {"1": [9]}},
                 get_guild=lambda _: guild)
        with patch.object(dm, "db_add_ban", new_callable=AsyncMock) as add:
            result = await process_federated_ban(bot, guild, member(), member(5), "test", {})
        self.assertIn("Could not verify", result.skipped)
        add.assert_not_awaited()

    async def test_old_alert_cannot_ban_a_newly_protected_moderator(self):
        event = interaction()
        event.guild.fetch_member = AsyncMock(return_value=NS(roles=[NS(id=9)]))
        event.client.get_guild = lambda _: event.guild
        with patch.object(dm, "db_add_ban", new_callable=AsyncMock) as add:
            await ScreeningView().ban_button.callback(event)
        add.assert_not_awaited()
        event.guild.ban.assert_not_awaited()
        embed = event.followup.edit_message.await_args.kwargs["embed"]
        self.assertIn("moderators", next(field.value for field in embed.fields if field.name == "Status"))


class AiTests(unittest.IsolatedAsyncioTestCase):
    def bot(self):
        return NS(llm_client=None, llm_semaphore=asyncio.Semaphore(2), system_prompt="test",
                  pending_ai_actions={}, config={"llm_settings": {"defaults": {"automation_mode": "full", "automation_delay_seconds": 0}}})

    async def test_client_reused(self):
        bot = self.bot()
        with patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"}), patch.object(llm.genai, "Client") as client:
            self.assertTrue(llm.initialize_gemini(bot))
            self.assertTrue(llm.initialize_gemini(bot))
        client.assert_called_once()

    async def test_async_requests_are_bounded_and_allow_other_tasks(self):
        bot = self.bot()
        current = peak = 0
        verdict = llm.AnalysisResult(verdict=llm.Verdict.SAFE, reason="test")
        async def generate(**kwargs):
            nonlocal current, peak
            current += 1
            peak = max(peak, current)
            await asyncio.sleep(0.01)
            current -= 1
            return NS(parsed=verdict)
        bot.llm_client = NS(aio=NS(models=NS(generate_content=generate)))
        results = await asyncio.gather(*(llm.get_llm_verdict(bot, member(), "Message", "test", "test") for _ in range(6)))
        self.assertEqual(peak, 2)
        self.assertEqual(results, [verdict]*6)

    async def test_deadline_includes_queue_time(self):
        bot = self.bot()
        bot.llm_semaphore = asyncio.Semaphore(0)
        generate = AsyncMock()
        bot.llm_client = NS(aio=NS(models=NS(generate_content=generate)))
        with patch.object(llm, "LLM_TIMEOUT_SECONDS", 0.01):
            self.assertIsNone(await llm.get_llm_verdict(bot, member(), "Message", "test", "test"))
        generate.assert_not_awaited()

    async def test_alert_before_inference_and_moderator_cancels_inference(self):
        bot = self.bot()
        alert = NS(id=10, edit=AsyncMock())
        channel = NS(send=AsyncMock(return_value=alert), fetch_message=AsyncMock(), guild=NS(id=1))
        started = asyncio.Event()
        async def verdict(*args):
            channel.send.assert_awaited_once()
            started.set()
            await asyncio.Event().wait()
        with patch.object(llm, "get_llm_verdict", side_effect=verdict), \
             patch.object(llm, "perform_automated_action", new_callable=AsyncMock) as action:
            task = asyncio.create_task(llm.start_llm_analysis_task(bot, channel, discord.Embed(), ScreeningView(), member(), "Message", "test", "test"))
            await started.wait()
            await ScreeningView().cancel_pending_ai_action(NS(client=bot, message=alert, user=member(5)))
            await asyncio.gather(task, return_exceptions=True)
            action.assert_not_awaited()
        alert.edit.assert_not_awaited()
        self.assertEqual(bot.pending_ai_actions, {})

    async def test_failed_inference_keeps_single_manual_alert(self):
        bot = self.bot()
        channel = NS(send=AsyncMock(return_value=NS(id=10)), guild=NS(id=1))
        with patch.object(llm, "get_llm_verdict", new_callable=AsyncMock, return_value=None):
            await llm.start_llm_analysis_task(bot, channel, discord.Embed(), ScreeningView(), member(), "Message", "test", "test")
        channel.send.assert_awaited_once()
        self.assertEqual(bot.pending_ai_actions, {})

    async def test_verdict_updates_same_alert_then_automates(self):
        bot = self.bot()
        alert = NS(id=10, edit=AsyncMock())
        alert.edit.return_value = alert
        channel = NS(send=AsyncMock(return_value=alert), fetch_message=AsyncMock(), guild=NS(id=1))
        verdict = llm.AnalysisResult(verdict=llm.Verdict.SAFE, reason="test")
        with patch.object(llm, "get_llm_verdict", new_callable=AsyncMock, return_value=verdict), \
             patch.object(llm, "perform_automated_action", new_callable=AsyncMock) as action:
            await llm.start_llm_analysis_task(bot, channel, discord.Embed(), ScreeningView(), member(), "Message", "test", "test")
        channel.send.assert_awaited_once()
        alert.edit.assert_awaited_once()
        action.assert_awaited_once()
        self.assertEqual(bot.pending_ai_actions, {})


class ScreeningTests(unittest.IsolatedAsyncioTestCase):
    def test_trusted_url_only_is_exempt(self):
        rules = {"simple_keywords": ["malicious"], "regex_patterns": [r"https?://"],
                 "whitelisted_domains_regex": [r"trusted\.example"]}
        for text in ("https://trusted.example/path", "www.trusted.example/path", "trusted.example", "<https://trusted.example>"):
            with self.subTest(text=text):
                self.assertEqual(screening.check_text_for_keywords(text, rules), [])
        for text in ("malicious https://trusted.example", "https://trusted.example https://evil.example", "https://trusted.example.evil/path", "https://trusted.example@evil.example", "https://evil.example/trusted.example", "[safe](https://trusted.example)[bad](https://evil.example)"):
            with self.subTest(text=text):
                self.assertTrue(screening.check_text_for_keywords(text, rules))
        self.assertIn("malicious", screening.check_text_for_keywords("[malicious](https://trusted.example/path)", rules))

    def test_cache_cleanup_keeps_recent_activity(self):
        now = datetime.now(timezone.utc)
        old = now - timedelta(minutes=10)
        bot = NS(config={}, bio_check_cache={(1, 1): old, (1, 2): now},
                 message_history={1: {1: deque([(old, 1)]), 2: deque([(old, 1), (now, 2)])}, 2: {3: deque()}})
        screening.prune_screening_caches(bot)
        self.assertEqual(set(bot.bio_check_cache), {(1, 2)})
        self.assertEqual(set(bot.message_history), {1})
        self.assertEqual(list(bot.message_history[1][2]), [(now, 2)])

    async def test_partner_checks_share_concurrency_limit(self):
        current = peak = 0
        async def fetch(_):
            nonlocal current, peak
            current += 1
            peak = max(peak, current)
            await asyncio.sleep(0.005)
            current -= 1
            return NS(reason="partner ban")
        guilds = {i: NS(id=i, name=str(i), fetch_ban=fetch) for i in range(2, 7)}
        bot = NS(config={"federated_guild_ids": list(range(1, 7))}, get_guild=guilds.get,
                 screening_semaphore=asyncio.Semaphore(2))
        with patch.object(dm, "db_get_ban", new_callable=AsyncMock, return_value=None):
            results = await asyncio.gather(*(screening.screen_member(bot, member(uid), {}) for uid in (111, 222)))
        self.assertEqual(peak, 2)
        self.assertTrue(all(result["flagged"] for result in results))


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.patches = []
        for name, value in {"DB_FILE": str(self.root/"test.db"), "DATA_DIR": str(self.root),
                            "SERVERS_CONFIG_DIR": str(self.root/"servers"), "GLOBAL_CONFIG_FILE": str(self.root/"global.yaml"),
                            "SYNC_STATUS_FILE": str(self.root/"sync.json"), "_config_cache": None, "_keywords_cache": None,
                            "_config_cache_mtime": None, "_keywords_cache_mtime": None}.items():
            patcher = patch.object(dm, name, value)
            patcher.start()
            self.patches.append(patcher)
        await dm.init_db()
        self.guild = NS(id=1, fetch_ban=AsyncMock(side_effect=http_error()), ban=AsyncMock())
        async def audit_logs(**kwargs):
            for uid in getattr(self, "unbans", []):
                yield NS(target=NS(id=uid))
        self.guild.audit_logs = audit_logs
        self.bot = NS(config={})

    async def asyncTearDown(self):
        for patcher in reversed(self.patches):
            patcher.stop()
        self.temp.cleanup()

    async def add_bans(self, *ids):
        await dm.db_bulk_import_bans([(str(uid), f"user{uid}", "test", 2, "origin", 5, "2026-01-01", None) for uid in ids])

    async def test_cached_reads_do_no_filesystem_work_and_refresh_sees_changes(self):
        Path(dm.GLOBAL_CONFIG_FILE).write_text("federated_guild_ids: [1]\n")
        first = dm.load_federation_config()
        keywords = await dm.load_keywords()
        with patch.object(dm, "_compute_yaml_mtime", side_effect=AssertionError("hot path I/O")), \
             patch.object(dm, "ensure_runtime_dirs", side_effect=AssertionError("hot path I/O")):
            self.assertIs(dm.load_federation_config(), first)
            self.assertIs(await dm.load_keywords(), keywords)
        Path(dm.GLOBAL_CONFIG_FILE).write_text("federated_guild_ids: [2]\n")
        self.assertEqual(dm.load_federation_config()["federated_guild_ids"], [1])
        self.assertEqual(dm.load_federation_config(refresh=True)["federated_guild_ids"], [2])

    async def test_file_removal_changes_config_revision(self):
        Path(dm.GLOBAL_CONFIG_FILE).write_text("{}")
        server = Path(dm.SERVERS_CONFIG_DIR)/"1.yaml"
        server.write_text("guild_id: 1")
        os.utime(server, (1, 1))
        before = dm._compute_yaml_mtime()
        server.unlink()
        self.assertNotEqual(before, dm._compute_yaml_mtime())

    async def test_onboarding_retries_only_failed_and_preserves_local_unbans(self):
        await self.add_bans(111, 222, 333)
        self.unbans = [333]
        async def ban(user, **kwargs):
            if user.id == 222:
                raise http_error(403, 50013)
        self.guild.ban.side_effect = ban
        counts = await run_onboarding(self.bot, self.guild, AsyncMock())
        self.assertEqual(counts, {"applied": 1, "failed": 1, "skipped": 1})
        self.assertEqual((await dm.load_sync_status())["synced_guild_ids"], [])
        # User 111 was successfully banned then locally unbanned: never replay it.
        self.guild.ban = AsyncMock()
        counts = await run_onboarding(self.bot, self.guild, AsyncMock())
        self.assertEqual(counts, {"applied": 2, "skipped": 1})
        self.assertEqual([call.args[0].id for call in self.guild.ban.await_args_list], [222])
        self.assertEqual((await dm.load_sync_status())["synced_guild_ids"], [1])

    async def test_onboarding_chunks_and_skips_whitelist_and_removed_records(self):
        await self.add_bans(111, 222, 333, 444)
        self.bot.config["whitelisted_user_ids"] = [222]
        async def report(counts):
            await dm.db_remove_ban(444)
        with patch("utils.onboarding.CHUNK_SIZE", 1):
            counts = await run_onboarding(self.bot, self.guild, report)
        self.assertEqual(counts, {"applied": 2, "skipped": 2})
        self.assertEqual([call.args[0].id for call in self.guild.ban.await_args_list], [111, 333])

    async def test_interrupted_ban_is_verified_without_blind_replay(self):
        await self.add_bans(111, 222)
        self.guild.ban.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await run_onboarding(self.bot, self.guild, AsyncMock())
        self.assertEqual(await dm.get_onboarding_counts(1), {"applying": 1, "pending": 1})
        self.guild.ban = AsyncMock()
        counts = await run_onboarding(self.bot, self.guild, AsyncMock())
        self.assertEqual(counts, {"review": 1, "applied": 1})
        self.assertEqual([call.args[0].id for call in self.guild.ban.await_args_list], [222])
        self.assertEqual(await dm.get_onboarding_review_ids(1), ["111"])
        # Existing local-unban tool can resolve an ambiguous action safely.
        await dm.record_onboarding_unban(1, 111)
        counts = await run_onboarding(self.bot, self.guild, AsyncMock())
        self.assertEqual(counts, {"applied": 1, "skipped": 1})
        self.assertEqual((await dm.load_sync_status())["synced_guild_ids"], [1])

    async def test_throttling_pauses_without_marking_complete(self):
        await self.add_bans(111, 222)
        self.guild.ban.side_effect = http_error(429, 30035)
        counts = await run_onboarding(self.bot, self.guild, AsyncMock())
        self.assertEqual(counts, {"failed": 1, "pending": 1})
        self.assertEqual((await dm.load_sync_status())["synced_guild_ids"], [])

    async def test_audit_failure_stops_before_any_ban(self):
        await self.add_bans(111)
        async def audit_logs(**kwargs):
            raise http_error(403, 50013)
            yield
        self.guild.audit_logs = audit_logs
        with self.assertRaises(discord.HTTPException):
            await run_onboarding(self.bot, self.guild, AsyncMock())
        self.guild.ban.assert_not_awaited()

    async def test_existing_database_migration_preserves_bans(self):
        await self.add_bans(111)
        await dm.init_db()
        self.assertEqual((await dm.db_get_ban(111))["username"], "user111")

    async def test_legacy_completed_server_cannot_be_replayed(self):
        from cogs.mod_commands import ModCommands
        await dm.mark_onboarding_complete(1)
        event = interaction()
        with patch.object(dm, "get_onboarding_counts", new_callable=AsyncMock) as counts:
            await ModCommands.onboard_server.callback(ModCommands(event.client), event)
        counts.assert_not_awaited()
        self.assertIn("already been onboarded", event.followup.send.await_args.args[0])

    async def test_master_alert_preserves_original_record_and_displays_reason(self):
        await dm.db_add_ban(111, "original name", "Original scam evidence", 2, "Original guild", 77,
                            "2025-01-01", bio="Imported bio evidence")
        original = await dm.db_get_ban(111)
        event = interaction()
        event.message.embeds[0].title = "Flagged User (Master Ban List)"
        event.message.embeds[0].clear_fields()
        event.message.embeds[0].add_field(name="Original Ban Reason", value="```Original scam evidence```")
        event.message.embeds[0].add_field(name="Status", value="Awaiting review")
        event.guild.fetch_member = AsyncMock(side_effect=http_error())
        event.guild.fetch_ban = AsyncMock(side_effect=http_error())
        event.client.federation_semaphore = asyncio.Semaphore(1)
        event.client.get_guild = lambda _: event.guild
        with patch.object(dm, "load_fed_stats", new_callable=AsyncMock, return_value={}), \
             patch.object(dm, "save_fed_stats", new_callable=AsyncMock):
            await ScreeningView().ban_button.callback(event)
        self.assertEqual(await dm.db_get_ban(111), original)
        self.assertIn("Original scam evidence", event.guild.ban.await_args.kwargs["reason"])
        self.assertNotIn("Reason not parsed", event.guild.ban.await_args.kwargs["reason"])

    async def test_new_ban_still_records_provenance(self):
        await dm.db_add_ban(111, "name", "evidence", 2, "origin", 77, "2026-01-01", "bio")
        record = await dm.db_get_ban(111)
        self.assertEqual((record["reason"], record["origin_guild_id"], record["moderator_id"], record["bio_at_import"]),
                         ("evidence", 2, 77, "bio"))

    async def test_removal_during_onboarding_lookup_prevents_ban(self):
        await self.add_bans(111)
        async def fetch(user):
            await dm.db_remove_ban(user.id)
            raise http_error()
        self.guild.fetch_ban.side_effect = fetch
        counts = await run_onboarding(self.bot, self.guild, AsyncMock())
        self.assertEqual(counts, {"skipped": 1})
        self.guild.ban.assert_not_awaited()
        self.assertIsNone(await dm.db_get_ban(111))

    async def test_global_unban_waits_for_inflight_onboarding_then_wins(self):
        from utils.federation_handler import process_federated_unban
        await self.add_bans(111)
        started, release, removing = asyncio.Event(), asyncio.Event(), asyncio.Event()
        banned = False
        async def ban(user, **kwargs):
            nonlocal banned
            started.set()
            await release.wait()
            banned = True
        async def fetch(user):
            if not banned:
                raise http_error()
            return NS(reason="test")
        async def unban(user, **kwargs):
            nonlocal banned
            banned = False
        self.guild.name = "Test"
        self.guild.ban.side_effect = ban
        self.guild.fetch_ban.side_effect = fetch
        self.guild.unban = AsyncMock(side_effect=unban)
        self.bot.config["federated_guild_ids"] = [1]
        self.bot.get_guild = lambda _: self.guild
        original_remove = dm.db_remove_ban
        async def remove(uid):
            removing.set()
            await original_remove(uid)
        with patch.object(dm, "load_fed_stats", new_callable=AsyncMock, return_value={}), \
             patch.object(dm, "save_fed_stats", new_callable=AsyncMock), \
             patch.object(dm, "db_remove_ban", side_effect=remove):
            onboarding = asyncio.create_task(run_onboarding(self.bot, self.guild, AsyncMock()))
            await asyncio.wait_for(started.wait(), 2)
            reversal = asyncio.create_task(process_federated_unban(self.bot, self.guild, member(), member(5), "test", True))
            await asyncio.wait_for(removing.wait(), 2)
            self.assertFalse(reversal.done())
            self.assertIsNotNone(await dm.db_get_ban(111))
            release.set()
            await asyncio.wait_for(asyncio.gather(onboarding, reversal), 5)
        self.assertFalse(banned)
        self.assertIsNone(await dm.db_get_ban(111))
        self.guild.unban.assert_awaited_once()


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_cogs_load_offline_and_shutdown_cancels_unsent_analysis(self):
        from antiscam import AntiScamBot
        with patch.object(dm, "load_federation_config", return_value={}), \
             patch.object(dm, "load_system_prompt", return_value="test"), \
             patch.object(dm, "load_scam_servers", return_value=[]), \
             patch.object(dm, "init_db", new_callable=AsyncMock):
            bot = AntiScamBot(intents=discord.Intents.none())
            async with bot:
                with patch.object(bot.tree, "sync", new_callable=AsyncMock, return_value=[]):
                    await bot.setup_hook()
                self.assertEqual(set(bot.cogs), {"ModCommands", "OwnerCommands", "BackgroundTasks", "EventListeners"})
                started = asyncio.Event()
                async def send(**kwargs):
                    started.set()
                    await asyncio.Event().wait()
                task = llm.schedule_analysis(bot=bot, alert_channel=NS(send=send), embed=discord.Embed(),
                                             view=ScreeningView(), flagged_member=member(), content_type="Message", content="test", trigger="test")
                await started.wait()
                client = NS(aio=NS(aclose=AsyncMock()))
                bot.llm_client = client
            self.assertTrue(task.cancelled())
            client.aio.aclose.assert_awaited_once()
            self.assertEqual(bot.llm_tasks, set())


if __name__ == "__main__":
    unittest.main()
