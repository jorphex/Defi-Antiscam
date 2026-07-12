# Privacy Policy

Last updated: July 12, 2026

This Privacy Policy applies to the Discord DeFi Antiscam Bot.

## What the Bot Does

The bot helps participating Discord servers detect and respond to scam, phishing, impersonation, spam, and raid activity. It provides join screening, message screening, moderator alerts, federated bans and unbans, retroactive scans, and related moderation tools.

## Data We Process

The bot may process Discord data needed for moderation and security, including:

- Discord user IDs, usernames, display names, avatars, account age, roles, and server membership events.
- Guild, channel, message, moderator, ban, unban, timeout, and audit-log metadata.
- Usernames, nicknames, bios/profile text, message content, edited message content, attachments or links, and configured keyword/regex matches when needed for scam detection.
- Moderator-provided ban reasons, unban reasons, whitelist entries, configuration values, and federation records.

## Data We Store

The bot stores limited moderation records outside Discord, such as user IDs, usernames, ban reasons, origin guild IDs/names, moderator IDs, timestamps, and sometimes bio/profile text associated with a moderation record.

The bot does not store general message history outside Discord. Message content is processed for real-time moderation. If a message is flagged, an excerpt may appear in a private moderator alert inside Discord so moderators can review the action.

## How We Use Data

We use data only to operate the bot, protect participating communities, detect known scam accounts, enforce moderator actions, synchronize federated bans/unbans, investigate false positives, maintain allowlists, and respond to deletion or correction requests.

We do not sell data. We do not use data for advertising. Message content is not used to train machine learning or AI models.

## AI and Service Providers

When AI analysis is enabled for a participating server, flagged content may be sent to Google Gemini for inference only, to help classify a moderation alert as safe, suspicious, or malicious. This is not used by this bot to train or fine-tune AI models.

The project may also use hosting, logging, database, GitHub, and Discord infrastructure needed to run and maintain the bot.

## Data Sharing

Federated moderation actions may be shared with other participating servers through Discord alerts and automated ban/unban actions. Shared records are limited to moderation-relevant information such as user IDs, usernames, reasons, timestamps, origin server information, and action status.

## Retention

Federated moderation records are kept while needed to protect participating servers, audit moderation decisions, or maintain the shared threat list. Records are removed when a global unban is performed, when a record is found to be incorrect, or when a valid deletion request is approved.

Transient message-processing data is not retained as general message history by the bot.

## Deletion and Correction Requests

Users may request deletion or correction of stored moderation records by contacting the bot maintainer or a participating server's moderators. Requests should include the relevant Discord user ID and enough context to locate the record.

Contact options:

- Use the bot's maintainer contact command in a participating server.
- Contact the maintainer on X: https://x.com/JxyHelper
- Contact moderators in any participating server where the bot is installed.

## Security

Stored bot data is kept in private runtime storage, is not committed to the public repository, and is accessible only to the bot operator and infrastructure needed to run the bot. Access is limited to moderation and maintenance purposes.

## Changes

This policy may be updated as the bot changes. The latest public version is maintained in this repository.
