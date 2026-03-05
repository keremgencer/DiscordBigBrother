import discord
from discord.ext import commands
import asyncio
import os

from Database.SQLiteDatabase import SQLiteDatabase
from Config import Config
from Logger.ConsoleLogger import ConsoleLogger

class MyBot(commands.Bot):
    def __init__(self):
        super().__init__(
            command_prefix="!",
            intents=discord.Intents.all(),
            help_command=None # We handle help in OwnerCog # todo help in OwnerCog is probably just for the owner, there should be another help command for the users
        )
        
        # Attach our core components to the bot instance
        # so Cogs can access them via self.bot.db, etc.
        self.db = SQLiteDatabase(Config.DATABASE_PATH)
        self.db.initialize_schema()
        self.logger = ConsoleLogger()

    async def setup_hook(self):
        # Load Cogs
        await self.load_extension("Cogs.MusicCog")
        await self.load_extension("Cogs.GeneralCog")
        await self.load_extension("Cogs.OwnerCog")
        await self.load_extension("Cogs.EventCog")
        
        # We sync in the on_ready event in EventCog instead, 
        # or we could do it here. EventCog handles it currently.

async def main():
    bot = MyBot()
    try:
        await bot.start(Config.BOT_TOKEN)
    except Exception as e:
        print(f"An error occurred while running the bot: {e}")

if __name__ == "__main__":
    asyncio.run(main())
