# /antiscam/antiscam.py

import discord
import os
import asyncio
import config
import data_manager

# --- BOT CLASS DEFINITION ---
class AntiScamBot(discord.Client):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)

        self.config = data_manager.load_federation_config()
        self.system_prompt = data_manager.load_system_prompt()
        self.scam_server_ids = data_manager.load_scam_servers()
        
        self.pending_ai_actions = {}
        self.active_scans = {}
        self.bio_check_cache = {}
        self._has_synced_once = False
        self.suspicious_identity_tags = []

    async def setup_hook(self):
        for filename in os.listdir('./cogs'):
            if filename.endswith('.py'):
                try:
                    await self.load_extension(f'cogs.{filename[:-3]}')
                    config.logger.info(f"Loaded cog: {filename}")
                except Exception as e:
                    config.logger.error(f"Failed to load cog {filename}: {e}", exc_info=True)
        
        try:
            synced = await self.tree.sync()
            config.logger.info(f"Synced {len(synced)} application command(s).")
        except Exception as e:
            config.logger.error(f"Failed to sync application commands: {e}")


# --- MAIN SCRIPT EXECUTION ---
if __name__ == "__main__":
    intents = discord.Intents.default()
    intents.guilds = True
    intents.members = True
    intents.moderation = True
    intents.message_content = True

    bot = AntiScamBot(intents=intents)

    if not config.BOT_TOKEN:
        config.logger.critical("FATAL ERROR: ANTISCAM_BOT_TOKEN environment variable not set.")
    else:
        try:
            config.logger.info("Environment variables found. Starting AntiScam Bot...")
            bot.run(config.BOT_TOKEN, log_handler=None)
        except discord.LoginFailure:
            config.logger.critical("FATAL ERROR: Invalid Discord bot token.")
        except Exception as e:
            config.logger.critical(f"An unexpected error occurred at the top level: {e}", exc_info=True)
