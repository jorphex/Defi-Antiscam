# Discord Defi Antiscam Bot

A federated antiscam bot to proactively protect DeFi and crypto communities. Inspired by the work of [danijelthales](https://github.com/danijelthales).

## Core Features

🛡️ **Federated Network**: A ban in one server is instantly propagated to all others. New servers can be onboarded to receive the entire historical collective ban list, providing immediate protection.

🤖 **Automatic Screening**:
- **On Join**: Scans usernames, bios, and cross-references against the global federated ban list.
- **On Message**: Scans message content for malicious links and keywords. Also performs periodic, cached checks of user bios to catch dormant threats.
- **On Edit**: Scans message content after being edited, handling scammers who send harmless messages first then editing to show the scam payload.

🔎 **Proactive & Reactive**:
- **Real-time Actions**: Instantly deletes scam messages and times out offenders.
- **Proactive Bans**: Moderators can issue a global ban by User ID *before* a known scammer joins any server.
- **Retroactive Scanning**: A slash command to find threats already inside your server.
- **Full Auto**: Reduce workload even more by opting in full auto mode in which the bot fully automates all actions.
- **Mass Kick & Ban**: Handle raids and big groups of scammers and spammers

⚙️ **Advanced Toolkit**:
- **Interactive Alerts**: Alerts with buttons (Ban, Kick, Unban, Ignore) for quick response.
- **Local Control**: Moderators can locally override a federated ban for their specific community.
- **Keyword & Regex Management**: Add and remove keywords and complex regex patterns with slash commands.
- **Federation Stats**: Track bans initiated and received across the network, with federation total.
- **Maintainer Contact**: A slash command for a direct feedback channel to the bot owner.

## Join the Federation

Protect your community by leveraging the collective experience of trusted DeFi projects. By joining, you reduce workload, automate threat removal, and stop scammers before they can act.

Find me on the [Yearn Discord](https://discord.gg/yearn) or on [X](https://x.com/JxyHelper) to get your server onboarded.

## Required Bot Permissions

The bot needs these permissions in any member server to function correctly:

- `View Channels` (to read/send in configured channels)
- `Send Messages`
- `Embed Links` (for alerts and reports)
- `Read Message History` (for context and moderation flows)
- `Manage Messages` (to delete scam messages)
- `Moderate Members` (to timeout flagged users)
- `Ban Members` and `Unban Members`
- `Kick Members` (used by moderator tools)
- `View Audit Log` (to detect manual bans/unbans for federation)

### Current Federated Servers
- Yearn
- BMX
- Alchemix
- Threshold Network
- Icarus Finance
- Ethereal
- GridPlus
- DataDash
- Brilla Finance


## Historical onboarding and development checks

`/onboard-server` saves progress and can resume incomplete runs. Completed entries,
recorded local unbans, and retained unban audit entries are skipped. Interrupted
ban attempts with an uncertain outcome are reported for review rather than blindly
reapplied. Previously completed servers remain protected from a full replay.

Run offline regressions after installing `requirements.txt`:

```bash
python -B -m unittest discover -s tests -v
ruff check antiscam.py config.py data_manager.py llm_handler.py screening_handler.py cogs ui utils tests
```

Tests mock Discord/Gemini and use temporary databases; no bot login is needed.
