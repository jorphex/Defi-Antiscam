# Six moderation reliability and efficiency fixes

Goal: implement the six agreed fixes without adding the four deferred features or deploying changes.

Acceptance criteria:
- Alert buttons enforce the existing moderator policy and always act on the clicked alert's user, including restored views.
- Intended global bans use one handler; callers distinguish applied, already banned, skipped, and failed guilds. Alert delivery failures do not turn successful bans into failures.
- Gemini uses one async client, bounded concurrency and deadlines; alerts appear before inference and moderator actions cancel pending work.
- Partner checks use shared bounded concurrency; inactive screening caches expire; normal cached keyword/config reads do no filesystem work.
- Trusted URLs alone are excluded; accompanying text and untrusted URLs remain screened.
- Historical onboarding reads chunks, persists progress, resumes/retries incomplete work, preserves local unbans, and marks completion only when all work is resolved.
- Focused offline regression checks and configured Ruff checks pass; final diff contains only in-scope changes.

Steps:
- [x] Inspect current code, callers, guidance, and verification environment.
- [x] Fix alert authorization/target recovery and federated action outcomes.
- [x] Fix async AI lifecycle, screening concurrency/caches, and trusted URL handling.
- [x] Implement checkpointed onboarding and exception preservation.
- [x] Add and run focused offline regressions; apply safe relevant Ruff fixes and rerun checks.
- [x] Resolve review findings: protected-target checks, onboarding/removal coordination, and original ban provenance.
- [x] Add regression cases for all three findings; run offline checks and review the final diff.

Constraints: preserve runtime data and existing changes; no live moderation, deployment, external messages, or deferred feature work. Native `update_plan` is unavailable in this session.

Prior implementation verification: 27 offline regressions passed in a network-disabled, read-only container using the production Python/dependency image. All four cogs load; shutdown cancels pending inference and closes its client. Safe relevant Ruff fixes, the non-mutating Ruff check, and final diff review pass. Runtime data and the running bot were not modified.

Operational note: onboarding creates its checkpoint tables at the next normal startup. Resume via `/onboard-server`; ambiguous interrupted actions are reported for review, and previously completed servers remain protected from replay. No deferred features were added.

Review corrections verified: protected targets are checked at command entry and federation execution; failed moderator lookups block the action. Onboarding revalidates the master record under a per-user lock shared with removals and local overrides. Existing ban evidence/provenance is preserved atomically, and master-alert reasons are extracted correctly. All 34 offline regressions pass in the isolated production image, including both removal/ban orderings. Ruff and final diff checks pass; no deployment or runtime data changes.

Deployment authorized by user: commit and push the verified changes, restart the existing bind-mounted container without changing dependencies or mounts, and verify startup plus checkpoint schema.

- [x] Commit and push changes to main (`cf8cfe2`).
- [x] Back up the database, redeploy, and verify runtime health.

Deployment result (2026-09-05): source commit `cf8cfe2` pushed to main; existing bind-mounted container restarted. All four cogs loaded, 40 commands synced, Discord connected to 9 guilds, Gemini initialized, checkpoint schema verified, and initial external sync finished successfully with no new users. Container running with zero automatic restarts/OOM and no startup errors. Database backup retained in private runtime storage.
