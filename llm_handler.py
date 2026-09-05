# /antiscam/llm_handler.py

import google.genai as genai
from google.genai import types
import os
import enum
from pydantic import BaseModel
import discord
import asyncio
from typing import Optional, TYPE_CHECKING

from config import logger

if TYPE_CHECKING:
    from antiscam import AntiScamBot

# --- Pydantic Models for Structured Output ---
class Verdict(str, enum.Enum):
    MALICIOUS = "MALICIOUS"
    SUSPICIOUS = "SUSPICIOUS"
    SAFE = "SAFE"

class AnalysisResult(BaseModel):
    verdict: Verdict
    reason: str

# --- Gemini Client ---
LLM_TIMEOUT_SECONDS = 30

def initialize_gemini(bot):
    """Initializes the Gemini client using the API key from environment variables."""
    if bot.llm_client is not None:
        return True
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY not found in environment variables. LLM features will be disabled.")
        return False
    try:
        bot.llm_client = genai.Client(api_key=api_key, http_options=types.HttpOptions(timeout=25000))
        return True
    except Exception:
        logger.exception("Could not initialize Gemini; manual review remains available")
        return False

async def get_llm_verdict(bot: 'AntiScamBot', member: discord.Member, content_type: str, content: str, trigger: str) -> Optional[AnalysisResult]:
    """
    Analyzes content using the Gemini API and returns a structured verdict.
    """
    try:
        if bot.llm_client is None:
            return None
        
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

        async def request():
            async with bot.llm_semaphore:
                return await bot.llm_client.aio.models.generate_content(
                    model="gemini-3.5-flash-lite",
                    contents=user_prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=bot.system_prompt,
                        response_mime_type="application/json",
                        response_schema=AnalysisResult,
                        temperature=0.0
                    )
                )

        # Includes queue time: overloaded requests fall back to the already-visible manual alert.
        response = await asyncio.wait_for(request(), timeout=LLM_TIMEOUT_SECONDS)

        if response.parsed:
            logger.info(f"Gemini analysis for {member.name} completed. Verdict: {response.parsed.verdict}")
            return response.parsed
        else:
            logger.error(f"Gemini analysis for {member.name} failed to parse. Raw text: {response.text}")
            return None

    except Exception as e:
        logger.error(f"An error occurred during Gemini API call for {member.name}: {e}", exc_info=True)
        return None

async def perform_automated_action(bot: 'AntiScamBot', alert_message: discord.Message, flagged_member_id: int, verdict_result: AnalysisResult, llm_config: dict):
    """
    Performs the automated action (ban or ignore) after the delay.
    """
    from ui.views import ScreeningView
    from utils.federation_handler import process_federated_ban
    
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
            moderator = bot.user 
            public_reason = f"Banned based on AI analysis. Reason: {verdict_result.reason}"
            audit_log_reason = f"[Automated Action] {public_reason} | AlertID:{alert_message.id}"
            detailed_reason_field = {"name": "AI Analysis Result", "value": f"```{public_reason}```"}
            logger.info(f"AI verdict is MALICIOUS. Calling the global ban handler for {member.name}.")
            
            result = await process_federated_ban(
                bot=bot,
                origin_guild=guild,
                user_to_ban=member,
                moderator=moderator,
                reason=audit_log_reason,
                detailed_reason_field=detailed_reason_field
            )
            
            logger.info(f"AUTOMATED GLOBAL BAN of {member.name} from {guild.name} has been processed.")
            
            embed = alert_message.embeds[0]
            embed.color = discord.Color.dark_red()
            for i, field in enumerate(embed.fields):
                if field.name == "Status":
                    embed.set_field_at(i, name="Status", value=result.summary(), inline=True)
                    break
            
            view = ScreeningView(flagged_member_id=flagged_member_id)
            view.update_buttons_for_state('banned' if result.banned_in(guild.id) else 'initial')
            await alert_message.edit(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Failed to execute automated global ban for {member.name}: {e}", exc_info=True)

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
        if bot.pending_ai_actions.get(alert_message.id) is asyncio.current_task():
            del bot.pending_ai_actions[alert_message.id]
            
def schedule_analysis(**kwargs):
    """Track tasks even while their first alert is being posted, for clean shutdown."""
    bot = kwargs["bot"]
    task = asyncio.create_task(start_llm_analysis_task(**kwargs))
    bot.llm_tasks.add(task)
    task.add_done_callback(bot.llm_tasks.discard)
    return task


async def start_llm_analysis_task(bot: 'AntiScamBot', alert_channel: discord.TextChannel,
        embed: discord.Embed, view: discord.ui.View, flagged_member: discord.Member,
        content_type: str, content: str, trigger: str):
    """Post first; analyze/update the same alert while allowing moderator cancellation."""
    alert_message = None
    try:
        alert_message = await alert_channel.send(embed=embed, view=view,
            allowed_mentions=discord.AllowedMentions(users=[flagged_member]))
        bot.pending_ai_actions[alert_message.id] = asyncio.current_task()
        verdict = await get_llm_verdict(bot, flagged_member, content_type, content, trigger)
        if verdict is None:
            return

        # The moderator may have deleted the alert while inference was running.
        await alert_channel.fetch_message(alert_message.id)
        colors = {Verdict.MALICIOUS: "🔴", Verdict.SUSPICIOUS: "🟡", Verdict.SAFE: "🟢"}
        embed.add_field(name=f"🤖 {colors[verdict.verdict]} **{verdict.verdict.value}**",
                        value=verdict.reason[:1024], inline=False)
        alert_message = await alert_message.edit(embed=embed, view=view)
        defaults = bot.config.get("llm_settings", {}).get("defaults", {})
        settings = bot.config.get("llm_settings", {}).get("per_guild_settings", {}).get(str(alert_channel.guild.id), defaults)
        if settings.get("automation_mode") == "full" and verdict.verdict in (Verdict.MALICIOUS, Verdict.SAFE):
            await delayed_action_wrapper(settings.get("automation_delay_seconds", 180),
                bot, alert_message, flagged_member.id, verdict, settings)
    except asyncio.CancelledError:
        logger.info("AI analysis/action cancelled for user %s", flagged_member.id)
        raise
    except discord.NotFound:
        logger.info("AI alert was removed for user %s", flagged_member.id)
    except Exception:
        logger.exception("AI alert processing failed for user %s", flagged_member.id)
    finally:
        if alert_message and bot.pending_ai_actions.get(alert_message.id) is asyncio.current_task():
            del bot.pending_ai_actions[alert_message.id]
