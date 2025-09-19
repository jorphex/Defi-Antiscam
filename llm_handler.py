# llm_handler.py
import google.genai as genai
from google.genai import types
import os
import logging
import enum
from pydantic import BaseModel
import discord
import asyncio
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING # <--- CHANGE 1: Import TYPE_CHECKING

# vvv CHANGE 2: Add the TYPE_CHECKING block for the IDE vvv
if TYPE_CHECKING:
    # This import is only seen by the type checker, not at runtime.
    # It allows the IDE to understand what 'AntiScamBot' is.
    from antiscam import AntiScamBot

logger = logging.getLogger('discord')




# --- Pydantic Models for Structured Output ---
class Verdict(str, enum.Enum):
    """Enumeration for the classification of the content."""
    MALICIOUS = "MALICIOUS"
    SUSPICIOUS = "SUSPICIOUS"
    SAFE = "SAFE"

class AnalysisResult(BaseModel):
    """Structured response from the LLM analysis."""
    verdict: Verdict
    reason: str

# --- Gemini Client Initialization ---
def initialize_gemini():
    """Initializes the Gemini client using the API key from environment variables."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY not found in environment variables. LLM features will be disabled.")
        return False
    # The client automatically picks up the key from the environment variable
    # No need to pass it explicitly if GEMINI_API_KEY is set
    return True

async def get_llm_verdict(bot: 'AntiScamBot', member: discord.Member, content_type: str, content: str, trigger: str) -> Optional[AnalysisResult]:
    """
    Analyzes content using the Gemini API with a separate system prompt
    and returns a structured verdict.
    """
    try:
        client = genai.Client()
        
        user_prompt = (
            "--- START DATA PACKET ---\n"
            f"Server Name: \"{member.guild.name}\"\n"
            "SERVER CONTEXT: Official support is handled via a ticket system only. Staff will never DM first or direct users to their bio for help.\n"
            f"Username: \"{member.name}\"\n"
            f"Account Age: Created <t:{int(member.created_at.timestamp())}:R>\n"
            f"Analysis Trigger: Flagged for '{trigger}'\n"
            f"Content Type: {content_type}\n"
            "Content to Analyze:\n"
            f"```\n{content}\n```\n"
            "--- END DATA PACKET ---"
        )

        response = client.models.generate_content(
            model="gemini-2.0-flash-lite",
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=bot.system_prompt,
                response_mime_type="application/json",
                response_schema=AnalysisResult,
                temperature=0.0
            )
        )

        if response.parsed:
            logger.info(f"Gemini analysis for {member.name} completed. Verdict: {response.parsed.verdict}")
            return response.parsed
        else:
            logger.error(f"Gemini analysis for {member.name} failed to parse. Raw text: {response.text}")
            return None

    except Exception as e:
        logger.error(f"An error occurred during Gemini API call for {member.name}: {e}", exc_info=True)
        return None

# vvv CHANGE 1: USE STRING TYPE HINTS TO AVOID CIRCULAR IMPORT vvv
async def perform_automated_action(bot: 'AntiScamBot', alert_message: discord.Message, flagged_member_id: int, verdict_result: AnalysisResult, llm_config: dict):
    """
    Performs the automated action (ban or ignore) after the delay.
    Includes safety checks and robust embed/view updating.
    """
    # vvv CHANGE 2: USE A LOCAL IMPORT, WHICH IS SAFE AT RUNTIME vvv
    # The local import for runtime logic is still necessary and correct.
    from antiscam import ScreeningView

    guild = alert_message.guild
    try:
        await alert_message.channel.fetch_message(alert_message.id)
    except discord.NotFound:
        logger.info(f"Automated action for {flagged_member_id} cancelled: Alert message was deleted by a moderator.")
        return

    member = guild.get_member(flagged_member_id)
    if not member:
        logger.info(f"Automated action for {flagged_member_id} cancelled: User has left the server.")
        embed = alert_message.embeds[0]
        for i, field in enumerate(embed.fields):
            if field.name == "Status":
                embed.set_field_at(i, name="Status", value="⚪ User Left Server", inline=True)
                break
        await alert_message.edit(embed=embed, view=None)
        return

    if member.timed_out_until is None:
        logger.info(f"Automated action for {flagged_member_id} cancelled: User is no longer timed out.")
        return

    if verdict_result.verdict == Verdict.MALICIOUS:
        try:
            reason = (
                f"[Automated Action] Banned based on AI analysis. "
                f"Reason: {verdict_result.reason} | AlertID:{alert_message.id}"
            )
            delete_days = bot.config.get("delete_messages_on_ban_days_default", 1)
            await guild.ban(member, reason=reason, delete_message_seconds=delete_days * 86400)
            logger.info(f"AUTOMATED BAN of {member.name} in {guild.name}.")
            
            embed = alert_message.embeds[0]
            embed.color = discord.Color.dark_red()
            for i, field in enumerate(embed.fields):
                if field.name == "Status":
                    embed.set_field_at(i, name="Status", value="🔴 Banned (Automated)", inline=True)
                    break
            
            view = ScreeningView(flagged_member_id=flagged_member_id)
            view.update_buttons_for_state('banned')
            await alert_message.edit(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Failed to execute automated ban for {member.name}: {e}")

    elif verdict_result.verdict == Verdict.SAFE:
        try:
            await member.timeout(None, reason="[Automated Action] Flag resolved as safe by AI analysis.")
            
            if llm_config.get("assign_role_on_safe") and llm_config.get("safe_role_id"):
                role_id = llm_config.get("safe_role_id")
                role = guild.get_role(role_id)
                if role:
                    await member.add_roles(role, reason="[Automated Action] User verified as safe.")
                else:
                    logger.warning(f"Could not find safe_role_id {role_id} in {guild.name}.")

            logger.info(f"AUTOMATED IGNORE for {member.name} in {guild.name}.")
            await alert_message.delete()
        except Exception as e:
            logger.error(f"Failed to execute automated ignore for {member.name}: {e}")

# vvv CHANGE 1 (AGAIN): USE STRING TYPE HINTS HERE TOO vvv
async def delayed_action_wrapper(delay: int, bot: 'AntiScamBot', alert_message: discord.Message, flagged_member_id: int, verdict_result: AnalysisResult, llm_config: dict):
    """
    A wrapper coroutine that waits for a delay, then performs the automated action.
    """
    try:
        await asyncio.sleep(delay)
        logger.info(f"Delay complete for {flagged_member_id}. Performing automated action: {verdict_result.verdict.value}")
        await perform_automated_action(bot, alert_message, flagged_member_id, verdict_result, llm_config)
    except asyncio.CancelledError:
        logger.info(f"Delayed action for {flagged_member_id} was cancelled by a moderator.")
    finally:
        if alert_message.id in bot.pending_ai_actions:
            del bot.pending_ai_actions[alert_message.id]
            
async def start_llm_analysis_task(bot: 'AntiScamBot', alert_channel, embed, view, flagged_member, content_type, content, trigger):
    """
    Orchestrates an AI-powered alert with a fallback to manual mode on API failure.
    """
    verdict_result = await get_llm_verdict(bot, flagged_member, content_type, content, trigger)

    if verdict_result is None:
        logger.warning(f"Gemini analysis failed for {flagged_member.name}. Falling back to manual alert.")
        allowed_mentions = discord.AllowedMentions(users=[flagged_member])
        await alert_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)
        return

    verdict_colors = {Verdict.MALICIOUS: "🔴", Verdict.SUSPICIOUS: "🟡", Verdict.SAFE: "🟢"}
    verdict_text = f"{verdict_colors[verdict_result.verdict]} **{verdict_result.verdict.value}**\n*Reason: {verdict_result.reason}*"
    embed.add_field(name="🤖 AI Analysis", value=verdict_text, inline=False)
    
    alert_message = await alert_channel.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions(users=[flagged_member]))

    guild_id_str = str(alert_channel.guild.id)
    llm_defaults = bot.config.get("llm_settings", {}).get("defaults", {})
    llm_config = bot.config.get("llm_settings", {}).get("per_guild_settings", {}).get(guild_id_str, llm_defaults)
    
    # --- THIS BLOCK IS NOW CORRECTED ---
    if llm_config.get("automation_mode") == "full" and verdict_result.verdict in [Verdict.MALICIOUS, Verdict.SAFE]:
        delay = llm_config.get("automation_delay_seconds", 180)
        logger.info(f"Scheduling automated action ({verdict_result.verdict.value}) for {flagged_member.name} in {delay} seconds.")
        
        # Create a task for the *entire* delayed operation using the new wrapper.
        task = bot.loop.create_task(
            delayed_action_wrapper(
                delay, bot, alert_message, flagged_member.id, verdict_result, llm_config
            )
        )
        
        # Store this new, correct task so it can be cancelled by a button press.
        bot.pending_ai_actions[alert_message.id] = task