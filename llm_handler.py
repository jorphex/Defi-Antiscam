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
def initialize_gemini():
    """Initializes the Gemini client using the API key from environment variables."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY not found in environment variables. LLM features will be disabled.")
        return False
    return True

async def get_llm_verdict(bot: 'AntiScamBot', member: discord.Member, content_type: str, content: str, trigger: str) -> Optional[AnalysisResult]:
    """
    Analyzes content using the Gemini API and returns a structured verdict.
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
            model="gemini-2.5-flash-lite",
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
            
            await process_federated_ban(
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
                    embed.set_field_at(i, name="Status", value="🔴 Auto banned, malicious intent", inline=True)
                    break
            
            view = ScreeningView(flagged_member_id=flagged_member_id)
            view.update_buttons_for_state('banned')
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
        if alert_message.id in bot.pending_ai_actions:
            del bot.pending_ai_actions[alert_message.id]
            
async def start_llm_analysis_task(bot: 'AntiScamBot', alert_channel: discord.TextChannel, embed: discord.Embed, view: discord.ui.View, flagged_member: discord.Member, content_type: str, content: str, trigger: str):
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
    verdict_name = f"🤖 {verdict_colors[verdict_result.verdict]} **{verdict_result.verdict.value}**"    
    verdict_text = f"*{verdict_result.reason}*"
    embed.add_field(name=verdict_name, value=verdict_text, inline=False)
    
    allowed_mentions = discord.AllowedMentions(users=[flagged_member])
    alert_message = await alert_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)

    guild_id_str = str(alert_channel.guild.id)
    llm_defaults = bot.config.get("llm_settings", {}).get("defaults", {})
    llm_config = bot.config.get("llm_settings", {}).get("per_guild_settings", {}).get(guild_id_str, llm_defaults)
    
    if llm_config.get("automation_mode") == "full" and verdict_result.verdict in [Verdict.MALICIOUS, Verdict.SAFE]:
        delay = llm_config.get("automation_delay_seconds", 180)
        logger.info(f"Scheduling automated action ({verdict_result.verdict.value}) for {flagged_member.name} in {delay} seconds.")
        
        task = bot.loop.create_task(
            delayed_action_wrapper(
                delay, bot, alert_message, flagged_member.id, verdict_result, llm_config
            )
        )
        
        bot.pending_ai_actions[alert_message.id] = task
