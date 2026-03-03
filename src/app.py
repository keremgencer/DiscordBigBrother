import discord
import EventHandler
import json 

try:
    intents = discord.Intents.all()

    client = discord.Client(intents=intents)

    from Database.SQLiteDatabase import SQLiteDatabase
    db = SQLiteDatabase("database.db")
    db.initialize_schema()

    eventHandeler = EventHandler.EventHandler(client, db)
    eventHandeler.handleEvents()

    with open("credentials.json") as f:
        credentials = json.load(f)  

    client.run(credentials["bot_token"])

except Exception as e:
    print(f"An error occurred while running the bot: {e}")

