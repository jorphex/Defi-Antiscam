# /antiscam/antiscam.py

import discord
from discord.ext import commands
import os
import config
import data_manager

# --- BOT CLASS ---
class AntiScamBot(commands.Bot):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(command_prefix="~`!", intents=intents)

        self.config = data_manager.load_federation_config()
        self.system_prompt = data_manager.load_system_prompt()
        self.scam_server_ids = data_manager.load_scam_servers()
        
        self.pending_ai_actions = {}
        self.active_scans = {}
        self.bio_check_cache = {}
        self.suspicious_identity_tags = []
        self.message_history = {}
        self.last_config_reload_at = None
        self.last_keywords_reload_at = None

    async def setup_hook(self):
        """This is called once when the bot is setting up, before it logs in."""
        
        # <<< START OF FIX: Initialize the Database >>>
        await data_manager.init_db()
        config.logger.info("Database initialized successfully.")
        # <<< END OF FIX >>>

        for filename in os.listdir('./cogs'):
            if filename.endswith('.py'):
                try:
                    # The [:-3] removes the '.py' from the file name
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
