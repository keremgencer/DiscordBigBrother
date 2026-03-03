import discord
from discord import app_commands
import random
from Database.IDatabase import IDatabase, DiscordEvent
from Event.EventProcessor import EventProcessor
from Logger.ILogger import ILogger

class EventHandler:
    def __init__(self, client, db: IDatabase, logger: ILogger):
        self.client = client
        self.logger = logger
        self.db = db
        self.tree = app_commands.CommandTree(client)
        self.processor = EventProcessor(self.db, self.logger)

    def handleEvents(self):
        @self.tree.command(name="cf", description="Yazı tura atar.")
        async def cf(interaction: discord.Interaction):
            try:
                result = random.choice(["Yazı", "Tura"])
                await interaction.response.send_message(f"🪙 Coinflip sonucu: **{result}**")
            except Exception as e:
                self.logger.log(f"Error in cf command: {e}\n")

        @self.client.event
        async def on_ready():
            try:
                # Slash komutunu Discord'un genel cache süresine takılmadan anında göstermek için 
                # botun olduğu her sunucuya özel olarak senkronize ediyoruz.
                for guild in self.client.guilds:
                    self.tree.copy_global_to(guild=guild)
                    await self.tree.sync(guild=guild)
                
                # Gelecekte eklenecek sunucular için global senkronizasyon:
                await self.tree.sync()
                
                print(f'Logged in as {self.client.user}')
                print('Started logging activities... (everything will be logged in activity_log.txt and database)')
                print('-' * 50)
                
                await self.processor.process_system_init(self.client)

            except Exception as e:
                self.logger.log(f"Error in on_ready: {e}\n")

        @self.client.event
        async def on_member_join(member):
            await self.processor.process_member_join(member)

        @self.client.event
        async def on_member_remove(member): 
            await self.processor.process_member_remove(member)


        # ---------------------------------------------------------
        # 1. GUILD PROFILE UPDATES (on_member_update)
        # ---------------------------------------------------------
        @self.client.event
        async def on_member_update(before, after):
            await self.processor.process_member_update(before, after)
        
        
        # ---------------------------------------------------------
        # 2. STATUS AND ACTIVITY UPDATES (on_presence_update)
        # ---------------------------------------------------------
        @self.client.event
        async def on_presence_update(before, after):
            await self.processor.process_presence_update(before, after)
        
        
        # ---------------------------------------------------------
        # 3. VOICE CHANNEL UPDATES (on_voice_state_update)
        # ---------------------------------------------------------
        @self.client.event
        async def on_voice_state_update(member, before, after):
            await self.processor.process_voice_state_update(member, before, after)

        @self.client.event
        async def on_user_update(before, after):
            await self.processor.process_user_update(before, after)

#todo database sonradan arayüze de bağlanabilir. (örneğin web tabanlı bir dashboard)