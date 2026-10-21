"""External source failures must preserve bans and the daily import task."""
import asyncio
import json
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, Mock, patch

import aiohttp

from cogs import background_tasks as bg


class Response:
    def __init__(self, status=200, data=None, error=None):
        self.status, self.data, self.error = status, data, error

    async def __aenter__(self):
        if self.error:
            raise self.error
        return self

    async def __aexit__(self, *args):
        pass

    async def json(self):
        return self.data

    async def text(self):
        return self.data


class ExternalSyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.report = NS(send=AsyncMock())
        self.notice = NS(send=AsyncMock())
        self.bot = NS(config={"log_channel_id": 1, "whitelisted_user_ids": [222]},
                      get_channel=lambda cid: self.report if cid == 1 else self.notice,
                      wait_until_ready=AsyncMock())
        self.cog = bg.BackgroundTasks(self.bot)
        self.session = NS(get=None)
        self.client = AsyncMock()
        self.client.__aenter__.return_value = self.session
        self.importer = AsyncMock(return_value=1)
        for patcher in (
            patch.object(bg, "GITHUB_TOKEN", "test"),
            patch.object(bg.aiohttp, "ClientSession", return_value=self.client),
            patch.object(bg.data_manager, "db_get_ban_count", AsyncMock(return_value=10)),
            patch.object(bg.data_manager, "db_bulk_import_bans", self.importer),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def responses(self, *items):
        self.session.get = Mock(side_effect=items)

    def listing(self, count=1):
        return Response(data=[{"name": f"master_part_{i}.jsonl", "path": f"master_part_{i}.jsonl"}
                              for i in range(count)])

    def assert_report(self, title):
        self.report.send.assert_awaited_once()
        self.notice.send.assert_awaited_once()
        embed = self.report.send.await_args.kwargs["embed"]
        self.assertIn(title, embed.title)
        self.assertIn(title, self.notice.send.await_args.args[0])
        self.assertIn("Existing bans retained", embed.description)
        self.assertNotIn("No new users were found", self.notice.send.await_args.args[0])

    async def test_unavailable_source_and_empty_listing_do_not_import_or_report_success(self):
        for response in (Response(403), Response(404), Response(451),
                         Response(error=asyncio.TimeoutError()), self.listing(0)):
            with self.subTest(status=response.status, error=response.error):
                self.report.send.reset_mock()
                self.notice.send.reset_mock()
                self.responses(response)
                await self.cog.sync_external_bans.coro(self.cog)
                self.importer.assert_not_awaited()
                self.assert_report("Failed")
        self.assertEqual(bg.aiohttp.ClientSession.call_args.kwargs["timeout"].total, 30)

    async def test_partial_downloads_import_available_records_and_report_partial(self):
        records = '\n'.join(json.dumps(p) for p in [
            {"user_id": "111", "username": "scammer"}, {"user_id": "111"},
            {"user_id": "222"}, {"user_id": "333", "bot": True},
        ])
        self.responses(self.listing(3), Response(404),
                       Response(error=aiohttp.ClientConnectionError()), Response(data=records))
        await self.cog.sync_external_bans.coro(self.cog)
        self.assertEqual([row[0] for row in self.importer.await_args.args[0]], ["111"])
        self.assert_report("Partial")
        self.assertIn("**Failed Downloads:** `2`", self.report.send.await_args.kwargs["embed"].description)

    async def test_all_downloads_failed_do_not_import(self):
        self.responses(self.listing(2), Response(404), Response(error=asyncio.TimeoutError()))
        await self.cog.sync_external_bans.coro(self.cog)
        self.importer.assert_not_awaited()
        self.assert_report("Failed")

    async def test_notification_failures_do_not_stop_scheduled_recovery(self):
        self.report.send.side_effect = RuntimeError("report unavailable")
        self.notice.send.side_effect = RuntimeError("notice unavailable")
        self.responses(Response(404), self.listing(), Response(data='{"user_id":"111"}'))
        imported = asyncio.Event()

        async def import_records(rows):
            imported.set()
            return 1

        self.importer.side_effect = import_records
        loop = self.cog.sync_external_bans
        loop.change_interval(seconds=0.01)
        task = loop.start()
        try:
            await asyncio.wait_for(imported.wait(), 2)
            self.assertFalse(loop.failed())
            self.assertEqual(self.report.send.await_count, 2)
            self.assertEqual(self.notice.send.await_count, 2)
            self.importer.assert_awaited_once()
        finally:
            loop.cancel()
            await asyncio.gather(task, return_exceptions=True)
