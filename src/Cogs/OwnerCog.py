import discord
from discord.ext import commands
from Config import Config
import time
import asyncio

class OwnerCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # A cog check to ensure these commands can only be run dynamically by the owner in DMs
    async def cog_check(self, ctx: commands.Context) -> bool:
        if not isinstance(ctx.channel, discord.DMChannel):
            return False
        if ctx.author.id != Config.OWNER_ID:
            return False
        return True

    @commands.command(name="help")
    async def owner_help(self, ctx: commands.Context, *args):
        # Using a distinct name since commands.Bot has a default help command
        help_text = (
            "**🔧 Owner Commands:**\n\n"
            f"`!status <type> <text>` — Change bot activity\n"
            f"`!say <channel_id> <message>` — Send a message as the bot\n"
            f"`!talk` — Play an attached audio file in VC\n"
            f"`!help` — Show this help message\n"
        )
        await ctx.reply(help_text)

    @commands.command(name="status")
    async def status_cmd(self, ctx: commands.Context, status_type: str = None, *, text: str = None):
        if not status_type or status_type.lower() == "help":
            help_text = (
                "**📊 Status Command Usage:**\n\n"
                f"`!status <type> <text>`\n\n"
                "**Types:**\n"
                "• `playing` — Playing <text>\n"
                "• `watching` — Watching <text>\n"
                "• `listening` — Listening to <text>\n"
                "• `competing` — Competing in <text>\n"
                "• `clear` — Remove current status\n\n"
                "**Examples:**\n"
                f"`!status playing Valorant`\n"
                f"`!status watching YouTube`\n"
                f"`!status listening Spotify`\n"
                f"`!status clear`"
            )
            await ctx.reply(help_text)
            return

        status_type = status_type.lower()

        if status_type == "clear":
            await self.bot.change_presence(activity=None)
            await ctx.reply("✅ Status cleared.")
            return

        if not text:
            await ctx.reply("❌ Missing text. Usage: `!status <type> <text>`")
            return

        activity_types = {
            "playing": discord.Game(name=text),
            "watching": discord.Activity(type=discord.ActivityType.watching, name=text),
            "listening": discord.Activity(type=discord.ActivityType.listening, name=text),
            "competing": discord.Activity(type=discord.ActivityType.competing, name=text),
        }

        activity = activity_types.get(status_type)
        if not activity:
            await ctx.reply(f"❌ Unknown type: `{status_type}`\nValid types: `playing`, `watching`, `listening`, `competing`, `clear`")
            return

        try:
            await self.bot.change_presence(activity=activity)
            await ctx.reply(f"✅ Status changed to **{status_type}** `{text}`")
        except Exception as e:
            self.bot.logger.log(f"Error changing status: {e}\n")
            await ctx.reply(f"⚠️ Failed to change status: {e}")

    @commands.command(name="say")
    async def say_cmd(self, ctx: commands.Context, channel_id: int = None, *, message: str = None):
        if not channel_id or not message:
            help_text = (
                "**💬 Say Command Usage:**\n\n"
                f"`!say <channel_id> <message>`\n\n"
                "Sends a message to the specified channel as the bot.\n\n"
                "**Examples:**\n"
                f"`!say 123456789012345678 Hello everyone!`\n"
                f"`!say 123456789012345678 🎉 Welcome!`"
            )
            await ctx.reply(help_text)
            return

        channel = self.bot.get_channel(channel_id)

        if not channel:
            await ctx.reply(f"❌ Channel not found: `{channel_id}`")
            return

        try:
            await channel.send(message)
            await ctx.reply(f"✅ Message sent to **#{channel.name}**")
        except Exception as e:
            self.bot.logger.log(f"Error in say command: {e}\n")
            await ctx.reply(f"⚠️ Failed to send message: {e}")

    @commands.command(name="talk")
    async def talk_cmd(self, ctx: commands.Context):
        if not ctx.message.attachments:
            help_text = (
                "**🔊 Talk Command Usage:**\n\n"
                f"`!talk` + attach an audio file\n\n"
                "Plays the attached audio file through the bot's current voice channel.\n"
                "The bot must be connected to a VC first (use `/connect`).\n\n"
                "**Supported formats:** MP3, WAV, OGG, FLAC, M4A\n\n"
                "**Example:**\n"
                f"Send `!talk` with an audio file attached."
            )
            await ctx.reply(help_text)
            return

        # Check if bot is in a VC
        voice_client = None
        for vc in self.bot.voice_clients:
            voice_client = vc
            break

        if not voice_client or not voice_client.is_connected():
            await ctx.reply("❌ Bot is not connected to a voice channel. Use `/connect` first.")
            return

        attachment = ctx.message.attachments[0]
        audio_extensions = ('.mp3', '.wav', '.ogg', '.flac', '.m4a')
        if not attachment.filename.lower().endswith(audio_extensions):
            await ctx.reply(f"❌ Unsupported format: `{attachment.filename}`\nSupported: {', '.join(audio_extensions)}")
            return

        try:
            # ffmpeg options for streaming
            FFMPEG_OPTIONS = {
                'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5',
                'options': '-vn',
            }
            talk_source = discord.FFmpegPCMAudio(attachment.url, **FFMPEG_OPTIONS)
            talk_source = discord.PCMVolumeTransformer(talk_source, volume=1.0)
            
            if voice_client.source is not None:
                # Music is playing or paused, intercept the audio source natively
                was_paused = voice_client.is_paused()
                original_source = voice_client.source
                
                class InterceptAudioSource(discord.AudioSource):
                    def __init__(self, original, new_source, bot_loop, vc, was_paused):
                        self.original = original
                        self.new_source = new_source
                        self.bot_loop = bot_loop
                        self.vc = vc
                        self.was_paused = was_paused
                        self.new_source_finished = False

                    def read(self) -> bytes:
                        if not self.new_source_finished:
                            ret = self.new_source.read()
                            if ret:
                                return ret
                            self.new_source_finished = True
                            self.new_source.cleanup()
                            # If it was paused before the talk command, re-pause it automatically
                            if self.was_paused:
                                self.bot_loop.call_soon_threadsafe(self.vc.pause)
                            
                        return self.original.read()

                    def is_opus(self) -> bool:
                        return False

                    def cleanup(self):
                        try:
                            self.new_source.cleanup()
                        except:
                            pass
                        try:
                            self.original.cleanup()
                        except:
                            pass

                    @property
                    def volume(self):
                        return getattr(self.original, 'volume', 1.0)
                    
                    @volume.setter
                    def volume(self, value):
                        if hasattr(self.original, 'volume'):
                            self.original.volume = value
                        if hasattr(self.new_source, 'volume'):
                            self.new_source.volume = value

                voice_client.source = InterceptAudioSource(original_source, talk_source, self.bot.loop, voice_client, was_paused)
                
                if was_paused:
                    voice_client.resume()

                await ctx.reply("🔊 Playing audio... (music will resume automatically after)")
            else:
                def after_talk(error):
                    if error:
                        self.bot.logger.log(f"Talk playback error: {error}\n")

                voice_client.play(talk_source, after=after_talk)
                await ctx.reply('🔊 Playing audio...')

        except Exception as e:
            self.bot.logger.log(f"Error in talk command: {e}\n")
            await ctx.reply(f"⚠️ Failed to play audio: {e}")

async def setup(bot: commands.Bot):
    # Removing the built-in help command so it doesn't collide with our owner help, though we renamed ours.
    # It's better to keep it clean anyway if they aren't using prefix commands publicly
    bot.help_command = None
    await bot.add_cog(OwnerCog(bot))
