# ui.py


import re
import discord
import config



class RegexTestModal(discord.ui.Modal, title="Regex Test"):
    def __init__(self, pattern: str, compiled_regex: re.Pattern):
        super().__init__()
        self.pattern = pattern
        self.compiled_regex = compiled_regex

    sample_text = discord.ui.TextInput(
        label="Sample Text",
        style=discord.TextStyle.paragraph,
        placeholder="Paste the raw, multi-line sample text here...",
        required=True,
        max_length=1000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        text_to_test = self.sample_text.value
        match = self.compiled_regex.search(text_to_test)

        if match:
            embed = discord.Embed(title="✅ Regex Test: Match Found", color=discord.Color.green())
            embed.add_field(name="Pattern", value=f"`{self.pattern}`", inline=False)
            embed.add_field(name="Sample Text", value=f"```{text_to_test}```", inline=False)
            embed.add_field(name="Matched Text", value=f"`{match.group(0)}`", inline=False)
        else:
            embed = discord.Embed(title="❌ Regex Test: No Match", color=discord.Color.orange())
            embed.add_field(name="Pattern", value=f"`{self.pattern}`", inline=False)
            embed.add_field(name="Sample Text", value=f"```{text_to_test}```", inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        config.logger.error(f"Error in RegexTestModal: {error}", exc_info=True)
        await interaction.response.send_message("An unexpected error occurred. Please check the logs.", ephemeral=True)
