import discord
import re
from typing import Optional, TYPE_CHECKING
from datetime import datetime, timezone
from unidecode import unidecode
from config import logger
import data_manager
from utils.federation_handler import process_federated_ban, process_federated_unban
from utils.command_helpers import update_onboard_command_visibility
from utils.helpers import get_delete_days_for_guild
from screening_handler import test_text_against_regex

if TYPE_CHECKING:
    from antiscam import AntiScamBot

# --- MODALS ---
class RegexTestModal(discord.ui.Modal, title="Regex Test"):
    def __init__(self, pattern: str, compiled_regex: re.Pattern):
        super().__init__()
        self.pattern = pattern
        self.compiled_regex = compiled_regex

    sample_text = discord.ui.TextInput(
        label="Sample Text", style=discord.TextStyle.paragraph,
        placeholder="Paste the raw, multi-line sample text here...",
        required=True, max_length=1000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        text_to_test = self.sample_text.value
        
        normalized_text_to_test = unidecode(text_to_test).lower()
        
        match = self.compiled_regex.search(normalized_text_to_test) # Test against the normalized version
        
        if match:
            embed = discord.Embed(title="✅ Regex Test: Match Found", color=discord.Color.green())
            embed.add_field(name="Matched Text", value=f"`{match.group(0)}`", inline=False)
        else:
            embed = discord.Embed(title="❌ Regex Test: No Match", color=discord.Color.orange())
        
        embed.add_field(name="Pattern", value=f"`{self.pattern}`", inline=False)
        # Show the original text so the user sees what they pasted
        embed.add_field(name="Original Sample Text", value=f"```{text_to_test}```", inline=False)
        # Also show the normalized version so they understand what the bot *actually* sees
        if text_to_test != normalized_text_to_test:
            embed.add_field(name="Normalized Text (What the bot tested)", value=f"```{normalized_text_to_test}```", inline=False)
            
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        logger.error(f"Error in RegexTestModal: {error}", exc_info=True)
        await interaction.response.send_message("An unexpected error occurred. Please check the logs.", ephemeral=True)

class TestCurrentRegexModal(discord.ui.Modal, title="Test Against Current Regex"):
    def __init__(self):
        super().__init__()

    sample_text = discord.ui.TextInput(
        label="Sample Text to Test",
        style=discord.TextStyle.paragraph,
        placeholder="Paste the raw, multi-line sample text here...",
        required=True,
        max_length=2000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        text_to_test = self.sample_text.value
        
        normalized_text_to_test = unidecode(text_to_test).lower()

        keywords_data = await data_manager.load_keywords()
        guild_id_str = str(interaction.guild.id)

        # Gather all applicable regex patterns (local and global)
        local_patterns = keywords_data.get("per_server_keywords", {}).get(guild_id_str, {}).get("bio_and_message_keywords", {}).get("regex_patterns", [])
        global_patterns = keywords_data.get("global_keywords", {}).get("bio_and_message_keywords", {}).get("regex_patterns", [])
        all_patterns = list(set(local_patterns + global_patterns))

        if not all_patterns:
            await interaction.followup.send("There are no regex patterns configured for this server or globally.", ephemeral=True)
            return

        matched_patterns = test_text_against_regex(normalized_text_to_test, all_patterns)

        if matched_patterns:
            embed = discord.Embed(title="✅ Regex Test: Match Found", color=discord.Color.green())
            matched_list = "\n".join([f"- `{p}`" for p in matched_patterns])
            embed.add_field(name="Matched Patterns", value=matched_list, inline=False)
        else:
            embed = discord.Embed(title="❌ Regex Test: No Match", color=discord.Color.orange())
            embed.add_field(name="Result", value="The provided text did not match any of the current local or global regex patterns.", inline=False)

        # Show the original text
        embed.add_field(name="Original Sample Text", value=f"```{text_to_test}```", inline=False)
        # Also show the normalized version for clarity
        if text_to_test != normalized_text_to_test:
            embed.add_field(name="Normalized Text (What the bot tested)", value=f"```{normalized_text_to_test}```", inline=False)
            
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        logger.error(f"Error in TestCurrentRegexModal: {error}", exc_info=True)
        # Check if response has been sent, as on_submit might have failed after deferring
        if not interaction.response.is_done():
            await interaction.response.send_message("An unexpected error occurred. Please check the logs.", ephemeral=True)
        else:
            await interaction.followup.send("An unexpected error occurred. Please check the logs.", ephemeral=True)

# --- VIEWS ---
class ScreeningView(discord.ui.View):
    def __init__(self, flagged_member_id: Optional[int] = None):
        super().__init__(timeout=None)
        self.flagged_member_id = flagged_member_id

    async def cancel_pending_ai_action(self, interaction: discord.Interaction):
        bot: 'AntiScamBot' = interaction.client
        if interaction.message.id in bot.pending_ai_actions:
            try:
                bot.pending_ai_actions[interaction.message.id].cancel()
                del bot.pending_ai_actions[interaction.message.id]
                logger.info(f"Moderator {interaction.user.name} cancelled pending AI task for alert {interaction.message.id}.")
            except Exception as e:
                logger.error(f"Error cancelling pending AI task: {e}")

    def update_buttons_for_state(self, state: str):
        """Updates button disabled status based on the user's state.
        States: 'initial', 'banned', 'kicked'
        """
        if state == 'initial':
            # User is in the server (likely timed out), awaiting action
            self.ban_button.disabled = False
            self.kick_button.disabled = False
            self.ignore_button.disabled = False
            self.unban_button.disabled = True
        elif state == 'banned':
            # User has been banned from the server
            self.ban_button.disabled = True
            self.kick_button.disabled = True # Can't kick a banned user
            self.ignore_button.disabled = True
            self.unban_button.disabled = False
        elif state == 'kicked':
            # User has been kicked, a final action for this alert
            self.ban_button.disabled = True
            self.kick_button.disabled = True
            self.ignore_button.disabled = True
            self.unban_button.disabled = True

    async def get_user_and_member(self, interaction: discord.Interaction) -> tuple[Optional[discord.User], Optional[discord.Member]]:
        """
        Fetches the User object and, if possible, the Member object.
        The User object is almost always available, while the Member is only if they are in the server.
        """
        bot: 'AntiScamBot' = interaction.client
        if not self.flagged_member_id:
            try:
                embed_footer = interaction.message.embeds[0].footer.text
                match = re.search(r'User ID: (\d+)', embed_footer)
                if match:
                    self.flagged_member_id = int(match.group(1))
                else:
                    await interaction.followup.send("❌ Could not find user ID in the alert footer.", ephemeral=True)
                    return None, None
            except (IndexError, TypeError, ValueError, AttributeError):
                await interaction.followup.send("❌ Could not parse user ID from the alert.", ephemeral=True)
                return None, None

        user = bot.get_user(self.flagged_member_id)
        if not user:
            try:
                user = await bot.fetch_user(self.flagged_member_id)
            except discord.NotFound:
                await interaction.followup.send("❌ User ID is invalid or the user account was deleted.", ephemeral=True)
                return None, None
        
        member = interaction.guild.get_member(self.flagged_member_id)
        
        return user, member
                
    async def update_embed(self, interaction: discord.Interaction, status: str, color: discord.Color):
        embed = interaction.message.embeds[0]
        embed.color = color
        for i, field in enumerate(embed.fields):
            if field.name == "Status":
                embed.set_field_at(i, name="Status", value=status, inline=True)
                break
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)

    @discord.ui.button(label="Ban", style=discord.ButtonStyle.red, custom_id="screening_ban")
    async def ban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot: 'AntiScamBot' = interaction.client
        await self.cancel_pending_ai_action(interaction)
        await interaction.response.defer()
        result = await self.get_user_and_member(interaction)
        if result == (None, None):
            return
        user, member = result

        try:
            original_embed = interaction.message.embeds[0]
            descriptive_reason = "Reason not parsed from alert." # Fallback

            if "User Banned Elsewhere" in original_embed.title:
                for field in original_embed.fields:
                    if "Banned In" in field.name:
                        descriptive_reason = f"User already banned in {field.name.split(': ')[1]}."
                        break
            elif "Flagged User" in original_embed.title:
                for field in original_embed.fields:
                    if "Trigger" in field.name:
                        descriptive_reason = f"Flagged by keyword screening. Trigger: {field.value.strip('`')}."
                        break
            elif "Flagged Message" in original_embed.title:
                for field in original_embed.fields:
                    if "Trigger" in field.name:
                        descriptive_reason = f"Flagged for a message. Trigger: {field.value.strip('`')}."
                        break
            
            reason_text = f"[Federated Action] {descriptive_reason} | AlertID:{interaction.message.id}"
            
            delete_days = get_delete_days_for_guild(bot, interaction.guild)
            delete_seconds = delete_days * 86400
            
            await interaction.guild.ban(user, reason=reason_text, delete_message_seconds=delete_seconds)
            
            self.update_buttons_for_state('banned')
            
            status_text = "✅ Banned"
            if not member:
                status_text += " (User had left)"
                
            await self.update_embed(interaction, status_text, discord.Color.red())
        except Exception as e:
            logger.error(f"Failed to ban user {self.flagged_member_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error banning: {e}", ephemeral=True)

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.primary, custom_id="screening_kick")
    async def kick_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cancel_pending_ai_action(interaction)
        await interaction.response.defer()
        result = await self.get_user_and_member(interaction)
        if result == (None, None):
            return
        _, member = result
        
        if not member:
            await self.update_embed(interaction, "❌ Kick Failed (User left)", discord.Color.greyple())
            self.kick_button.disabled = True
            await interaction.followup.edit_message(message_id=interaction.message.id, view=self)
            await interaction.followup.send("❌ Cannot kick a user who is not in the server.", ephemeral=True)
            return

        try:
            reason_text = "Kicked by Moderator via screening alert."
            await member.kick(reason=reason_text)
            self.update_buttons_for_state('kicked')
            await self.update_embed(interaction, "👢 Kicked", discord.Color.blue())
        except Exception as e:
            logger.error(f"Failed to kick member {self.flagged_member_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An error occurred while kicking: {e}", ephemeral=True)

    @discord.ui.button(label="Unban", style=discord.ButtonStyle.grey, custom_id="screening_unban", disabled=True)
    async def unban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        user_to_unban = discord.Object(id=self.flagged_member_id)
        try:
            reason_text = "[Federated Action] Unbanned by Moderator via screening alert."
            await interaction.guild.unban(user_to_unban, reason=reason_text)
            self.update_buttons_for_state('initial')
            await self.update_embed(interaction, "🟡 Unbanned", discord.Color.gold())
        except Exception as e:
            logger.error(f"Failed to unban member {self.flagged_member_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error unbanning: {e}", ephemeral=True)

    @discord.ui.button(label="Ignore", style=discord.ButtonStyle.grey, custom_id="screening_ignore")
    async def ignore_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cancel_pending_ai_action(interaction)
        await interaction.response.defer()
        result = await self.get_user_and_member(interaction)
        if result == (None, None):
            return
        _, member = result
        
        if member:
            try:
                await member.timeout(None, reason="Flag marked as safe by Moderator.")
                logger.info(f"Removed timeout for {member.name} after ignore.")
            except Exception as e:
                logger.warning(f"Could not remove timeout for {member.name} on ignore: {e}")
        
        try:
            await interaction.message.delete()
            await interaction.followup.send("✅ Alert dismissed.", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to delete screening message: {e}", exc_info=True)
            if not interaction.is_done():
                 await interaction.followup.send("❌ An error occurred during cleanup.", ephemeral=True)

class FederatedAlertView(discord.ui.View):
    def __init__(self, banned_user_id: Optional[int] = None):
        super().__init__(timeout=None)
        self.banned_user_id = banned_user_id

    @discord.ui.button(label="Unban Locally", style=discord.ButtonStyle.secondary, custom_id="fed_alert_unban")
    async def unban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.banned_user_id:
            try:
                embed_footer = interaction.message.embeds[0].footer.text
                match = re.search(r'User ID: (\d+)', embed_footer)
                if match:
                    self.banned_user_id = int(match.group(1))
                    logger.info(f"Recovered user ID {self.banned_user_id} from footer for persistent view.")
                else:
                    await interaction.response.send_message("❌ Could not find a valid user ID in the alert footer.", ephemeral=True)
                    return
            except (IndexError, AttributeError, TypeError):
                await interaction.response.send_message("❌ Could not parse user ID from the alert footer.", ephemeral=True)
                return
            
        await interaction.response.defer()
        user_to_unban = discord.Object(id=self.banned_user_id)
        try:
            await interaction.guild.fetch_ban(user_to_unban)
        except discord.NotFound:
            button.disabled = True
            await interaction.followup.send("This user is not currently banned in this server.", ephemeral=True)
            try:
                embed = interaction.message.embeds[0]
                if "UPDATE:" not in embed.description:
                    embed.description += f"\n\n**UPDATE:** Action attempted by {interaction.user.mention}, but user was already unbanned."
                await interaction.edit_original_response(embed=embed, view=self)
            except Exception:
                pass
            return
            
        try:
            reason_text = "[Local Action] Federated ban reversed by local Moderator."
            await interaction.guild.unban(user_to_unban, reason=reason_text)
            
            embed = interaction.message.embeds[0]
            embed.color = discord.Color.green()
            embed.description += f"\n\n**UPDATE:** User was unbanned from this server by {interaction.user.mention}."
            button.disabled = True
            
            await interaction.edit_original_response(embed=embed, view=self)
            
            logger.info(f"Federated ban for {self.banned_user_id} was reversed in {interaction.guild.name} by {interaction.user.name}.")
        except Exception as e:
            logger.error(f"Failed to reverse federated ban for {self.banned_user_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An unexpected error occurred while unbanning: {e}", ephemeral=True)

class FederatedUnbanAlertView(discord.ui.View):
    def __init__(self, unbanned_user_id: Optional[int] = None):
        super().__init__(timeout=None)
        self.unbanned_user_id = unbanned_user_id

    @discord.ui.button(label="Re-Ban Locally", style=discord.ButtonStyle.danger, custom_id="fed_alert_reban")
    async def reban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        bot: 'AntiScamBot' = interaction.client

        if not self.unbanned_user_id:
            # Fallback to get ID from footer if needed
            try:
                embed_footer = interaction.message.embeds[0].footer.text
                match = re.search(r'User ID: (\d+)', embed_footer)
                if match:
                    self.unbanned_user_id = int(match.group(1))
                else:
                    await interaction.followup.send("❌ Could not find user ID in the alert footer.", ephemeral=True)
                    return
            except (IndexError, AttributeError):
                await interaction.followup.send("❌ Could not parse user ID from the alert.", ephemeral=True)
                return

        user_to_reban = discord.Object(id=self.unbanned_user_id)
        try:
            reason_text = "[Local Action] Federated unban reversed by local Moderator."
            delete_days = get_delete_days_for_guild(bot, interaction.guild)
            await interaction.guild.ban(user_to_reban, reason=reason_text, delete_message_seconds=delete_days * 86400)
            
            embed = interaction.message.embeds[0]
            embed.color = discord.Color.orange()
            embed.description += f"\n\n**UPDATE:** User was re-banned in this server by {interaction.user.mention}."
            button.disabled = True
            await interaction.message.edit(embed=embed, view=self)
            logger.info(f"Federated unban for {self.unbanned_user_id} was reversed in {interaction.guild.name} by {interaction.user.name}.")
        except Exception as e:
            logger.error(f"Failed to reverse federated unban for {self.unbanned_user_id}: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error re-banning: {e}", ephemeral=True)

class ConfirmGlobalBanView(discord.ui.View):
    def __init__(self, bot: 'AntiScamBot', author: discord.User, user_to_ban: discord.User, reason: str):
        super().__init__(timeout=60.0)
        self.bot = bot
        self.author = author
        self.user_to_ban = user_to_ban
        self.reason = reason

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You cannot interact with this confirmation.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Global Ban", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="✅ **Confirmation received. Propagating global ban...**", embed=None, view=self)

        try:
            config = self.bot.config
            origin_mod_channel_id = config.get("federation_notice_channels", {}).get(str(interaction.guild.id))
            if origin_mod_channel_id:
                origin_mod_channel = self.bot.get_channel(origin_mod_channel_id) or await self.bot.fetch_channel(origin_mod_channel_id)
                if origin_mod_channel:
                    confirm_embed = discord.Embed(
                        title="✅ Proactive Global Ban Initiated",
                        description=f"Ban was initiated from this server and has been broadcast to all federated servers.",
                        color=discord.Color.blue(),
                        timestamp=datetime.now(timezone.utc)
                    )
                    confirm_embed.set_author(name=f"{self.user_to_ban.name} (`{self.user_to_ban.id}`)", icon_url=self.user_to_ban.display_avatar.url)
                    confirm_embed.add_field(name="Reason", value=f"```{self.reason}```", inline=False)
                    await origin_mod_channel.send(embed=confirm_embed)
            detailed_reason_field = {"name": "Ban Reason", "value": f"```{self.reason}```"}

            await process_federated_ban(
                self.bot,
                origin_guild=interaction.guild,
                user_to_ban=self.user_to_ban,
                moderator=interaction.user,
                reason=self.reason,
                detailed_reason_field=detailed_reason_field,
                is_proactive_command=True
            )
            
            await interaction.followup.send(f"✅ **Success!** The global ban for **{self.user_to_ban.name}** has been initiated and propagated.", ephemeral=True)
            logger.info(f"Moderator {interaction.user.name} initiated a proactive global ban for {self.user_to_ban.name} from {interaction.guild.name}.")

        except Exception as e:
            await interaction.followup.send(f"❌ **Error:** An unexpected error occurred during propagation. Please check the logs.", ephemeral=True)
            logger.error(f"Error during proactive global ban propagation: {e}", exc_info=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Global ban cancelled.", embed=None, view=self)

class ConfirmGlobalUnbanView(discord.ui.View):
    def __init__(self, bot: 'AntiScamBot', author: discord.User, user_to_unban: discord.User, reason: str):
        super().__init__(timeout=60.0)
        self.bot = bot
        self.author = author
        self.user_to_unban = user_to_unban
        self.reason = reason

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You cannot interact with this confirmation.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Global Unban", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="✅ **Confirmation received. Propagating global unban...**", embed=None, view=self)

        try:
            config = self.bot.config
            origin_mod_channel_id = config.get("federation_notice_channels", {}).get(str(interaction.guild.id))
            if origin_mod_channel_id and (origin_mod_channel := self.bot.get_channel(origin_mod_channel_id)):
                confirm_embed = discord.Embed(
                    title="✅ Proactive Global Unban Initiated",
                    description=f"Unban was initiated from this server and has been broadcast to all federated servers.",
                    color=discord.Color.green(),
                    timestamp=datetime.now(timezone.utc)
                )
                if hasattr(self.user_to_unban, 'display_avatar') and self.user_to_unban.display_avatar:
                    confirm_embed.set_author(name=f"{self.user_to_unban.name} (`{self.user_to_unban.id}`)", icon_url=self.user_to_unban.display_avatar.url)
                else:
                    confirm_embed.set_author(name=f"{self.user_to_unban.name} (`{self.user_to_unban.id}`)")
                confirm_embed.add_field(name="Reason", value=f"```{self.reason}```", inline=False)
                await origin_mod_channel.send(embed=confirm_embed)

            await process_federated_unban(
                self.bot,
                origin_guild=interaction.guild,
                user_to_unban=self.user_to_unban,
                moderator=interaction.user,
                reason=self.reason,
                is_proactive_command=True
            )
            await interaction.followup.send(f"✅ **Success!** The global unban for **{self.user_to_unban.name}** has been initiated.", ephemeral=True)
            logger.info(f"Moderator {interaction.user.name} initiated a global unban for {self.user_to_unban.name} from {interaction.guild.name}.")
        except Exception as e:
            await interaction.followup.send(f"❌ **Error:** An unexpected error occurred during propagation. Please check the logs.", ephemeral=True)
            logger.error(f"Error during global unban propagation: {e}", exc_info=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Global unban cancelled.", embed=None, view=self)
        
class ConfirmScanView(discord.ui.View):
    def __init__(self, author: discord.User):
        super().__init__(timeout=60.0)
        self.author = author
        self.value = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You cannot interact with this confirmation.", ephemeral=True)
            return False
        return True
    @discord.ui.button(label="Confirm Scan", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        self.stop()
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(content="✅ **Confirmation received. Starting scan...** See below for progress updates.", view=self)
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        self.stop()
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(content="Scan cancelled.", view=self)

class OnboardView(discord.ui.View):
    def __init__(self, bot: 'AntiScamBot', author: discord.User, fed_bans: dict):
        super().__init__(timeout=300.0)
        self.bot = bot
        self.author = author
        self.fed_bans = fed_bans

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You are not the one who initiated this command.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Begin Onboarding", style=discord.ButtonStyle.danger)
    async def begin_onboarding(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        progress_embed = discord.Embed(
            title="⏳ Onboarding in Progress...",
            description="Applying historical bans. This may take some time.",
            color=discord.Color.orange()
        )
        progress_embed.add_field(name="Checked", value="`0`", inline=True)
        progress_embed.add_field(name="Applied", value="`0`", inline=True)
        progress_embed.add_field(name="Failed", value="`0`", inline=True)
        
        progress_message = await interaction.followup.send(embed=progress_embed, wait=True)

        target_guild = interaction.guild
        total_bans = len(self.fed_bans)
        applied_count = 0
        already_banned_count = 0
        failed_count = 0
        update_interval = 25

        for i, (user_id_str, ban_data) in enumerate(self.fed_bans.items()):
            user_id = int(user_id_str)
            user_obj = discord.Object(id=user_id)

            try:
                await target_guild.fetch_ban(user_obj)
                already_banned_count += 1
                continue
            except discord.NotFound:
                try:
                    reason = f"Federated ban sync. Original reason: {ban_data.get('reason', 'N/A')}"
                    delete_days = get_delete_days_for_guild(self.bot, target_guild)
                    delete_seconds = delete_days * 86400
                    await target_guild.ban(user_obj, reason=reason[:512], delete_message_seconds=delete_seconds)
                    applied_count += 1
                except Exception as e:
                    logger.warning(f"Failed to onboard-ban user {user_id} in {target_guild.name}: {e}")
                    failed_count += 1
            
            if (i + 1) % update_interval == 0 or (i + 1) == total_bans:
                progress_embed.set_field_at(0, name="Checked", value=f"`{i+1} / {total_bans}`", inline=True)
                progress_embed.set_field_at(1, name="Applied", value=f"`{applied_count}`", inline=True)
                progress_embed.set_field_at(2, name="Failed", value=f"`{failed_count}`", inline=True)
                await progress_message.edit(embed=progress_embed)

        completion_embed = discord.Embed(
            title="✅ Onboarding Complete",
            description=f"The server is now up to date with the federated ban list.",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        )
        completion_embed.add_field(name="Bans Applied", value=f"`{applied_count}`", inline=True)
        completion_embed.add_field(name="Already Banned", value=f"`{already_banned_count}`", inline=True)
        completion_embed.add_field(name="Failed", value=f"`{failed_count}`", inline=True)
        
        await progress_message.edit(content=None, embed=completion_embed)

        sync_status = await data_manager.load_sync_status()
        if target_guild.id not in sync_status["synced_guild_ids"]:
            sync_status["synced_guild_ids"].append(target_guild.id)
            await data_manager.save_sync_status(sync_status)
        
        await update_onboard_command_visibility(self.bot, interaction.guild)
        logger.info(f"Server {interaction.guild.name} has been successfully onboarded and permissions updated.")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Onboarding cancelled.", view=self, embed=None)

class LookupPaginatorView(discord.ui.View):
    def __init__(self, author: discord.User, query: str, results: list):
        super().__init__(timeout=300.0)
        self.author = author
        self.query = query
        self.results = results
        
        self.current_page = 0
        self.items_per_page = 5
        self.total_pages = (len(self.results) - 1) // self.items_per_page + 1

        mentioned_users = [discord.Object(id=int(user_id)) for user_id, data in self.results]
        self.allowed = discord.AllowedMentions(users=mentioned_users)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("You are not the one who initiated this command.", ephemeral=True)
            return False
        return True

    def create_embed(self) -> discord.Embed:
        """Creates the embed for the current page."""
        start_index = self.current_page * self.items_per_page
        end_index = start_index + self.items_per_page
        page_results = self.results[start_index:end_index]

        embed = discord.Embed(
            title=f"Ban List Search Results for \"{self.query}\"",
            description=f"Found **{len(self.results)}** matching record(s).",
            color=discord.Color.blue()
        )

        page_content = []
        for user_id, data in page_results:
            entry = (
                f"**Username:** {data.get('username_at_ban', 'N/A')}\n"
                f"**User ID:** `{user_id}`\n"
                f"**Origin:** {data.get('origin_guild_name', 'N/A')}\n"
                f"**Reason:** {data.get('reason', 'N/A')}"
            )
            page_content.append(entry)
        
        embed.description += "\n\n" + "\n--------------------\n".join(page_content)
        embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")
        
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1
        
        return embed

    @discord.ui.button(label="◄ Previous", style=discord.ButtonStyle.secondary, custom_id="lookup_prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            await interaction.response.edit_message(embed=self.create_embed(), view=self, allowed_mentions=self.allowed)

    @discord.ui.button(label="Next ►", style=discord.ButtonStyle.secondary, custom_id="lookup_next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            await interaction.response.edit_message(embed=self.create_embed(), view=self, allowed_mentions=self.allowed)

class AnnouncementModal(discord.ui.Modal, title="New System Announcement"):
    def __init__(self, bot: 'AntiScamBot'):
        super().__init__(timeout=None)
        self.bot = bot

    announcement_text = discord.ui.TextInput(
        label="Announcement Message",
        style=discord.TextStyle.paragraph,
        placeholder="Type your announcement here. This will be sent to all federated servers.",
        required=True,
        max_length=1500
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        # 1. Prepare the embed that will be sent to everyone
        announcement_embed = discord.Embed(
            title="📢 System Announcement",
            description=self.announcement_text.value,
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        announcement_embed.set_footer(text=f"A message from the {self.bot.user.name} maintainer.")

        # 2. Iterate and send
        success_guilds = []
        failed_guilds = []
        
        federated_guild_ids = self.bot.config.get("federated_guild_ids", [])
        for guild_id in federated_guild_ids:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                failed_guilds.append(f"`{guild_id}` (Not Found)")
                continue

            # Get a unique set of channel IDs to avoid double-sending
            channel_ids = set()
            alert_channel_id = self.bot.config.get("action_alert_channels", {}).get(str(guild.id))
            notice_channel_id = self.bot.config.get("federation_notice_channels", {}).get(str(guild.id))
            if alert_channel_id: channel_ids.add(alert_channel_id)
            if notice_channel_id: channel_ids.add(notice_channel_id)

            if not channel_ids:
                failed_guilds.append(f"{guild.name} (No channels configured)")
                continue

            sent_successfully = False
            for channel_id in channel_ids:
                channel = guild.get_channel(channel_id)
                if not channel:
                    logger.warning(f"Announcement: Could not find channel {channel_id} in {guild.name}.")
                    continue
                
                try:
                    await channel.send(embed=announcement_embed)
                    sent_successfully = True
                except discord.Forbidden:
                    logger.error(f"Announcement: Missing permissions to send to #{channel.name} in {guild.name}.")
                except Exception as e:
                    logger.error(f"Announcement: Failed to send to #{channel.name} in {guild.name}: {e}")
            
            if sent_successfully:
                success_guilds.append(guild.name)
            else:
                failed_guilds.append(f"{guild.name} (All sends failed)")

        # 3. Send the final report back to the owner
        report_embed = discord.Embed(
            title="Announcement Broadcast Report",
            color=discord.Color.green() if not failed_guilds else discord.Color.orange()
        )
        if success_guilds:
            report_embed.add_field(name="✅ Successfully Sent To", value="\n".join(success_guilds), inline=False)
        if failed_guilds:
            report_embed.add_field(name="❌ Failed or Partially Failed For", value="\n".join(failed_guilds), inline=False)
        
        await interaction.followup.send(embed=report_embed, ephemeral=True)
