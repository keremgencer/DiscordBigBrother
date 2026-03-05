import discord
from discord.ext import commands
from discord import app_commands
import random

class GeneralCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="cf", description="Yazı tura atar.")
    async def cf(self, interaction: discord.Interaction):
        try:
            result = random.choice(["Yazı", "Tura"])
            await interaction.response.send_message(f"🪙 {result}")
        except Exception as e:
            self.bot.logger.log(f"Error in cf command: {e}\n")

async def setup(bot: commands.Bot):
    await bot.add_cog(GeneralCog(bot))
