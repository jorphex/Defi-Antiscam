# Resilient GitHub ban sync

Goal: handle an unavailable GitHub source without misleading reports or stopping scheduled imports.

Acceptance: unavailable/empty sources report failure, partial downloads report partial sync, notification failures do not stop future runs, requests have a 30-second total timeout, existing bans remain intact, and daily retries continue.

- [x] Trace source fetch, database writes, notification paths, and scheduler behavior.
- [x] Implement bounded requests, accurate outcomes, and independent notification error handling.
- [x] Add focused failure/recovery regressions; run checks and review diff.
- [x] Restart existing bot and verify unavailable-source reporting and continued operation.

Constraints: preserve existing dirty changes/configuration and daily schedule; no source changes or ban removals. Native update_plan is unavailable.

Verification: all 37 tests pass in a network-disabled read-only container, including actual scheduled recovery after both notification channels fail. Full configured Ruff and diff checks pass. Source/account and runtime configuration unchanged.

Deployment: bot restarted successfully, loaded all four cogs, synced 40 commands, connected to 10 guilds and initialized Gemini. Live GitHub 404 now reports source unavailable, existing bans retained, next attempt in 24 hours; no notification failures observed.

Review and commit:
- [x] Review importer, failure outcomes, scheduler recovery and test assertions for scope/correctness.
- [x] Run final checks and commit only the sync fix, its regressions and this plan.

Review correction: reset report mocks between failure cases and assert exactly one report per channel, preventing a previous case from hiding a missing notification. Removed a stale comment and simplified a mock import. Earlier URL-whitelist support and test-pruning changes remain separate.

Final validation: 37 tests passed in the isolated production image; configured Ruff and diff checks passed. No behavior changes were needed during review.
