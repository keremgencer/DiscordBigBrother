import discord
import EventHandler
import json 

try:
    # Botun kullanıcıları ve aktiviteleri görebilmesi için gerekli niyetleri (intents) açıyoruz
    intents = discord.Intents.all()
    #intents.presences = True
    #intents.members = True
    #intents.message_content = True

    client = discord.Client(intents=intents)

    eventHandeler = EventHandler.EventHandler(client)
    eventHandeler.handleEvents()

    with open("credentials.json") as f:
        credentials = json.load(f)  

    client.run(credentials["bot_token"])

except Exception as e:
    print(f"An error occurred while running the bot: {e}")

