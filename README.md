# Discord Defi Antiscam Bot

A federated antiscam bot to proactively protect DeFi and crypto communities. Inspired by the work of [danijelthales](https://github.com/danijelthales).

## Core Features

🛡️ **Federated Network**: A ban in one server is instantly propagated to all others. New servers can be onboarded to receive the entire historical collective ban list, providing immediate protection.

🤖 **Automatic Screening**:
- **On Join**: Scans usernames, bios, and cross-references against the global federated ban list.
- **On Message**: Scans message content for malicious links and keywords. Also performs periodic, cached checks of user bios to catch dormant threats.

🔎 **Proactive & Reactive**:
- **Real-time Actions**: Instantly deletes scam messages and times out offenders.
- **Proactive Bans**: Moderators can issue a global ban by User ID *before* a known scammer joins any server.
- **Retroactive Scanning**: A slash command to find threats already inside your server.

⚙️ **Advanced Toolkit**:
- **Interactive Alerts**: Alerts with buttons (Ban, Kick, Unban, Ignore) for quick response.
- **Local Control**: Moderators can locally override a federated ban for their specific community.
- **Keyword & Regex Management**: Add and remove keywords and complex regex patterns with slash commands.
- **Federation Stats**: Track bans initiated and received across the network, with federation total.
- **Maintainer Contact**: A slash command for a direct feedback channel to the bot owner.

## Join the Federation

Protect your community by leveraging the collective experience of trusted DeFi projects. By joining, you reduce workload, automate threat removal, and stop scammers before they can act.

Find me on the [Yearn Discord](https://discord.gg/yearn) or on [X](https://x.com/JxyHelper) to get your server onboarded.

### Current Federated Servers
- Yearn
- BMX
- Alchemix
