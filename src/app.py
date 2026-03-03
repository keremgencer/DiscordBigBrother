import discord
from Event.EventHandler import EventHandler
from Logger.Logger import Logger
import json 
import os
from Database.SQLiteDatabase import SQLiteDatabase
try:
    intents = discord.Intents.all()

    client = discord.Client(intents=intents)

    
    _basedir = os.path.dirname(os.path.abspath(__file__))
    db = SQLiteDatabase(os.path.join(_basedir, "Database", "database.db"))
    db.initialize_schema()

    logger = Logger()
    eventHandler = EventHandler(client, db, logger)
    eventHandler.handleEvents()

    with open("credentials.json") as f:
        credentials = json.load(f)  

    client.run(credentials["bot_token"])

except Exception as e:
    print(f"An error occurred while running the bot: {e}")

