import discord
import time
import yt_dlp
import asyncio
import random
from discord.ext import commands
from discord import app_commands
from Config import Config

class MusicQueue:
    """Per-guild music queue state manager."""

    def __init__(self):
        self.queue: list[dict] = []       # List of {"url": str, "title": str}
        self.current: dict | None = None  # Currently playing track
        self.loop_mode: str = "off"       # "off", "track", "queue"
        self.volume: float = 0.5          # 0.0 to 1.0

    def add(self, track: dict):
        """Add a track to the end of the queue."""
        self.queue.append(track)

    def add_front(self, track: dict):
        """Add a track to the front of the queue (plays next)."""
        self.queue.insert(0, track)

    def advance(self) -> dict | None:
        """Advance to the next track based on loop mode.
        Returns the next track dict, or None if queue is empty."""
        if self.loop_mode == "track" and self.current:
            return self.current

        if self.loop_mode == "queue" and self.current:
            # Re-add current track to end of queue
            self.queue.append(self.current)

        if not self.queue:
            self.current = None
            return None

        self.current = self.queue.pop(0)
        return self.current

    def skip(self) -> dict | None:
        """Skip current track (ignores track loop). Returns next track or None."""
        if self.loop_mode == "queue" and self.current:
            self.queue.append(self.current)

        if not self.queue:
            self.current = None
            return None

        self.current = self.queue.pop(0)
        return self.current

    def shuffle(self):
        """Shuffle the remaining queue."""
        random.shuffle(self.queue)

    def toggle_loop(self) -> str:
        """Cycle loop mode: off -> track -> queue -> off. Returns new mode."""
        modes = ["off", "track", "queue"]
        idx = modes.index(self.loop_mode)
        self.loop_mode = modes[(idx + 1) % len(modes)]
        return self.loop_mode

    def clear(self):
        """Clear the queue (does not stop current track)."""
        self.queue.clear()

    def set_current(self, track: dict):
        """Set the currently playing track."""
        self.current = track

# ffmpeg options for streaming
FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5',
    'options': '-vn',
}

class MusicCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.queues: dict[int, MusicQueue] = {}  # guild_id -> MusicQueue
        self.skip_advance: set[int] = set()  # guild IDs where after-callback should NOT advance
        # yt-dlp options: extract audio URL without downloading
        self.YTDL_OPTIONS = {
            'format': 'bestaudio/best',
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'default_search': 'ytsearch', # Allow direct search queries
        }

    def get_queue(self, guild_id: int) -> MusicQueue:
        """Get or create a MusicQueue for a guild."""
        if guild_id not in self.queues:
            self.queues[guild_id] = MusicQueue()
        return self.queues[guild_id]

    async def _extract_track_info(self, url: str) -> dict:
        """Helper to extract track info using yt-dlp without downloading."""
        ytdl = yt_dlp.YoutubeDL(self.YTDL_OPTIONS)
        try:
            # Using asyncio.to_thread to prevent yt-dlp from blocking the main event loop
            info = await asyncio.to_thread(ytdl.extract_info, url, download=False)
            
            if 'entries' in info:
                # It's a playlist or search result, take the first entry
                info = info['entries'][0]

            return {
                'url': info['url'],
                'title': info.get('title', 'Unknown Title'),
                'duration': info.get('duration', 0),
                'thumbnail': info.get('thumbnail', None),
                'webpage_url': info.get('webpage_url', url),
                'original_url': url, # Keep the original request URL
            }
        except Exception as e:
            self.bot.logger.log(f"Error extracting info for {url}: {e}\n")
            raise

    def play_track(self, voice_client: discord.VoiceClient, track: dict, guild_id: int):
        """Start playing a track on the voice client."""
        mq = self.get_queue(guild_id)
        source = discord.FFmpegPCMAudio(track['url'], **FFMPEG_OPTIONS)
        source = discord.PCMVolumeTransformer(source, volume=mq.volume)
        mq.set_current(track)
        mq.play_start_time = time.time()

        def after_playback(error):
            if error:
                self.bot.logger.log(f"Playback error: {error}\n")
            # Skip auto-advance if flagged (forceplay/stop set this)
            if guild_id in self.skip_advance:
                self.skip_advance.discard(guild_id)
                return
            # Schedule next track on the event loop
            fut = asyncio.run_coroutine_threadsafe(
                self.play_next(voice_client, guild_id),
                self.bot.loop
            )
            try:
                fut.result()
            except Exception as e:
                self.bot.logger.log(f"Error advancing queue: {e}\n")

        voice_client.play(source, after=after_playback)

    async def play_next(self, voice_client: discord.VoiceClient, guild_id: int):
        """Advance the queue and play the next track."""
        if not voice_client.is_connected():
            return

        mq = self.get_queue(guild_id)
        next_track = mq.advance()

        if not next_track:
            mq.current = None
            return

        try:
            # Re-extract audio URL (stream URLs expire)
            track_info = await self._extract_track_info(next_track['original_url'])
            next_track['url'] = track_info['url']
            self.play_track(voice_client, next_track, guild_id)
        except Exception as e:
            self.bot.logger.log(f"Error playing next track: {e}\n")
            # Try the next one if this one fails
            await self.play_next(voice_client, guild_id)

    async def resume_track(self, voice_client: discord.VoiceClient, guild_id: int, track: dict, seek_seconds: int):
        """Resume a track at a specific position using FFmpeg -ss seek."""
        mq = self.get_queue(guild_id)
        mq.set_current(track)
        mq.play_start_time = time.time() - seek_seconds

        try:
            track_info = await self._extract_track_info(track['original_url'])
            fresh_url = track_info['url']
        except Exception as e:
            self.bot.logger.log(f"Error refreshing URL for resume: {e}\n")
            await self.play_next(voice_client, guild_id)
            return

        seek_opts = FFMPEG_OPTIONS.copy()
        seek_opts['before_options'] = f"-ss {seek_seconds} " + seek_opts.get('before_options', '')

        source = discord.FFmpegPCMAudio(fresh_url, **seek_opts)
        source = discord.PCMVolumeTransformer(source, volume=mq.volume)
        
        def after_playback(error):
            if error:
                self.bot.logger.log(f"Resume Playback error: {error}\n")
            if guild_id in self.skip_advance:
                self.skip_advance.discard(guild_id)
                return
            fut = asyncio.run_coroutine_threadsafe(
                self.play_next(voice_client, guild_id),
                self.bot.loop
            )
            try:
                fut.result()
            except Exception as e:
                self.bot.logger.log(f"Error advancing queue: {e}\n")

        voice_client.play(source, after=after_playback)

    async def ensure_voice_state(self, interaction: discord.Interaction, require_playing: bool = False, require_paused: bool = False, auto_connect: bool = False) -> str | None:
        """
        Validates voice channel state for music commands.
        Returns an error message string if validation fails, or None if successful.
        """
        guild = interaction.guild
        
        # 1. Auto-connect logic
        if auto_connect and not guild.voice_client:
            if not interaction.user.voice or not interaction.user.voice.channel:
                return "❌ You are not in a voice channel. Join one first or use `/connect`."
            await interaction.user.voice.channel.connect()
            
        # 2. Require bot to be in VC
        if not auto_connect and not guild.voice_client:
             return "❌ Bot is not in a voice channel."

        # 3. Require user to be in the SAME VC as the bot
        if interaction.user.voice and interaction.user.voice.channel != guild.voice_client.channel:
            return "❌ You must be in the same voice channel as the bot."
            
        # 4. Require user to be in a VC at all
        if not interaction.user.voice or not interaction.user.voice.channel:
             return "❌ You are not in a voice channel."

        # 5. Playback state requirements
        if require_playing and not guild.voice_client.is_playing() and not guild.voice_client.is_paused():
             return "❌ Nothing is playing right now."
             
        if require_paused and not guild.voice_client.is_paused():
             return "❌ Nothing is paused right now."

        return None

    # ----------------------------------------------------------
    # Music: Play / Queue / Force
    # ----------------------------------------------------------

    @app_commands.command(name="play", description="Play or queue a YouTube URL.")
    @app_commands.describe(url="YouTube video URL")
    async def play(self, interaction: discord.Interaction, url: str):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction, auto_connect=True)
            if error_msg:
                return await interaction.followup.send(error_msg)

            guild = interaction.guild
            track_info = await self._extract_track_info(url)
            mq = self.get_queue(guild.id)

            if not guild.voice_client.is_playing() and not guild.voice_client.is_paused():
                self.play_track(guild.voice_client, track_info, guild.id)
                await interaction.followup.send(f"🎵 Now playing: **{track_info['title']}**")
            else:
                mq.add(track_info)
                position = len(mq.queue)
                await interaction.followup.send(f"📋 Added to queue (#{position}): **{track_info['title']}**")
        except Exception as e:
            self.bot.logger.log(f"Error in play command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to play: {e}")

    @app_commands.command(name="queue", description="Add a YouTube URL to the queue.")
    @app_commands.describe(url="YouTube video URL")
    async def queue(self, interaction: discord.Interaction, url: str):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction, auto_connect=True)
            if error_msg:
                return await interaction.followup.send(error_msg)

            guild = interaction.guild
            track_info = await self._extract_track_info(url)
            mq = self.get_queue(guild.id)

            if not guild.voice_client.is_playing() and not guild.voice_client.is_paused():
                self.play_track(guild.voice_client, track_info, guild.id)
                await interaction.followup.send(f"🎵 Now playing: **{track_info['title']}**")
            else:
                mq.add(track_info)
                position = len(mq.queue)
                await interaction.followup.send(f"📋 Added to queue (#{position}): **{track_info['title']}**")
        except Exception as e:
            self.bot.logger.log(f"Error in queue command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to queue: {e}")

    @app_commands.command(name="forceplay", description="Stop current track and play this immediately.")
    @app_commands.describe(url="YouTube video URL")
    async def forceplay(self, interaction: discord.Interaction, url: str):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction, auto_connect=True)
            if error_msg:
                return await interaction.followup.send(error_msg)

            guild = interaction.guild
            track_info = await self._extract_track_info(url)
            mq = self.get_queue(guild.id)

            if guild.voice_client.is_playing() or guild.voice_client.is_paused():
                if mq.current:
                    mq.add_front(mq.current)
                self.skip_advance.add(guild.id)
                guild.voice_client.stop()
                await asyncio.sleep(0.5)

            self.play_track(guild.voice_client, track_info, guild.id)
            await interaction.followup.send(f"⏭️ Force playing: **{track_info['title']}**")
        except Exception as e:
            self.bot.logger.log(f"Error in forceplay command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to force play: {e}")

    @app_commands.command(name="forcequeue", description="Add a track to the front of the queue (plays next).")
    @app_commands.describe(url="YouTube video URL")
    async def forcequeue(self, interaction: discord.Interaction, url: str):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction, auto_connect=True)
            if error_msg:
                return await interaction.followup.send(error_msg)

            guild = interaction.guild
            track_info = await self._extract_track_info(url)
            mq = self.get_queue(guild.id)

            if not guild.voice_client.is_playing() and not guild.voice_client.is_paused():
                self.play_track(guild.voice_client, track_info, guild.id)
                await interaction.followup.send(f"🎵 Now playing: **{track_info['title']}**")
            else:
                mq.add_front(track_info)
                await interaction.followup.send(f"⏫ Added to front of queue: **{track_info['title']}**")
        except Exception as e:
            self.bot.logger.log(f"Error in forcequeue command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to force queue: {e}")

    # ----------------------------------------------------------
    # Music: Playback Controls
    # ----------------------------------------------------------

    @app_commands.command(name="stop", description="Stop playback and clear the queue.")
    async def stop(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction, require_playing=True)
            if error_msg:
                return await interaction.followup.send(error_msg)

            guild = interaction.guild
            mq = self.get_queue(guild.id)
            mq.clear()
            mq.current = None
            self.skip_advance.add(guild.id)
            guild.voice_client.stop()
            await interaction.followup.send("⏹️ Playback stopped and queue cleared.")
        except Exception as e:
            self.bot.logger.log(f"Error in stop command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to stop: {e}")

    @app_commands.command(name="pause", description="Pause the current playback.")
    async def pause(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction, require_playing=True)
            if error_msg:
                return await interaction.followup.send(error_msg)

            interaction.guild.voice_client.pause()
            await interaction.followup.send("⏸️ Playback paused. Use `/resume` to continue.")
        except Exception as e:
            self.bot.logger.log(f"Error in pause command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to pause: {e}")

    @app_commands.command(name="resume", description="Resume paused playback.")
    async def resume(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction, require_paused=True)
            if error_msg:
                return await interaction.followup.send(error_msg)

            interaction.guild.voice_client.resume()
            await interaction.followup.send("▶️ Playback resumed.")
        except Exception as e:
            self.bot.logger.log(f"Error in resume command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to resume: {e}")

    @app_commands.command(name="skip", description="Skip to the next track in the queue.")
    async def skip(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction, require_playing=True)
            if error_msg:
                return await interaction.followup.send(error_msg)

            guild = interaction.guild
            mq = self.get_queue(guild.id)
            skipped_title = mq.current['title'] if mq.current else 'Unknown'

            guild.voice_client.stop()
            await interaction.followup.send(f"⏭️ Skipped: **{skipped_title}**")
        except Exception as e:
            self.bot.logger.log(f"Error in skip command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to skip: {e}")

    # ----------------------------------------------------------
    # Music: Info & Settings
    # ----------------------------------------------------------

    @app_commands.command(name="np", description="Show the currently playing track.")
    async def np(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction)
            if error_msg:
                return await interaction.followup.send(error_msg)

            guild = interaction.guild
            mq = self.get_queue(guild.id)

            if not mq.current:
                return await interaction.followup.send("❌ Nothing is playing right now.")

            elapsed = int(time.time() - getattr(mq, 'play_start_time', time.time()))
            minutes, seconds = divmod(elapsed, 60)

            status = "⏸️ Paused" if guild.voice_client and guild.voice_client.is_paused() else "▶️ Playing"
            loop_icon = {"off": "", "track": " 🔂", "queue": " 🔁"}.get(mq.loop_mode, "")
            volume_pct = int(mq.volume * 100)

            msg = (
                f"🎵 **Now Playing:**\n"
                f"{status}: **{mq.current['title']}**\n"
                f"⏱️ `{minutes:02d}:{seconds:02d}` | 🔊 {volume_pct}%{loop_icon}"
            )
            await interaction.followup.send(msg)
        except Exception as e:
            self.bot.logger.log(f"Error in np command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to get now playing: {e}")

    @app_commands.command(name="volume", description="Set the playback volume (0-100).")
    @app_commands.describe(level="Volume level (0-100)")
    async def volume(self, interaction: discord.Interaction, level: int):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction)
            if error_msg:
                return await interaction.followup.send(error_msg)

            if level < 0 or level > 100:
                return await interaction.followup.send("❌ Volume must be between 0 and 100.")

            guild = interaction.guild
            mq = self.get_queue(guild.id)
            mq.volume = level / 100.0

            if guild.voice_client and guild.voice_client.source and hasattr(guild.voice_client.source, 'volume'):
                guild.voice_client.source.volume = mq.volume

            await interaction.followup.send(f"🔊 Volume set to **{level}%**")
        except Exception as e:
            self.bot.logger.log(f"Error in volume command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to set volume: {e}")

    @app_commands.command(name="loop", description="Toggle loop mode: off → track → queue.")
    async def loop(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction)
            if error_msg:
                return await interaction.followup.send(error_msg)
                
            guild = interaction.guild
            mq = self.get_queue(guild.id)
            new_mode = mq.toggle_loop()

            icons = {"off": "▶️ Loop disabled", "track": "🔂 Looping current track", "queue": "🔁 Looping entire queue"}
            await interaction.followup.send(icons.get(new_mode, f"Loop mode: {new_mode}"))
        except Exception as e:
            self.bot.logger.log(f"Error in loop command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to toggle loop: {e}")

    @app_commands.command(name="shuffle", description="Shuffle the remaining queue.")
    async def shuffle(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction)
            if error_msg:
                return await interaction.followup.send(error_msg)
                
            guild = interaction.guild
            mq = self.get_queue(guild.id)

            if not mq.queue:
                return await interaction.followup.send("❌ Queue is empty, nothing to shuffle.")

            mq.shuffle()
            await interaction.followup.send(f"🔀 Queue shuffled ({len(mq.queue)} tracks)")
        except Exception as e:
            self.bot.logger.log(f"Error in shuffle command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to shuffle: {e}")

    @app_commands.command(name="showqueue", description="Display the current queue.")
    async def showqueue(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction)
            if error_msg:
                return await interaction.followup.send(error_msg)
                
            guild = interaction.guild
            mq = self.get_queue(guild.id)
            lines = []

            if mq.current:
                status = "⏸️" if guild.voice_client and guild.voice_client.is_paused() else "▶️"
                lines.append(f"{status} **Now:** {mq.current['title']}")
            else:
                lines.append("**Nothing is playing.**")

            if mq.queue:
                lines.append(f"\n**Queue ({len(mq.queue)} tracks):**")
                for i, track in enumerate(mq.queue[:10], 1):
                    lines.append(f"`{i}.` {track['title']}")
                if len(mq.queue) > 10:
                    lines.append(f"*...and {len(mq.queue) - 10} more*")
            else:
                lines.append("\n*Queue is empty.*")

            loop_label = {"off": "Off", "track": "🔂 Track", "queue": "🔁 Queue"}.get(mq.loop_mode, "Off")
            volume_pct = int(mq.volume * 100)
            lines.append(f"\n🔊 {volume_pct}% | Loop: {loop_label}")

            await interaction.followup.send("\n".join(lines))
        except Exception as e:
            self.bot.logger.log(f"Error in showqueue command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to show queue: {e}")

    @app_commands.command(name="clear", description="Clear the queue without stopping the current track.")
    async def clear(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction)
            if error_msg:
                return await interaction.followup.send(error_msg)
                
            guild = interaction.guild
            mq = self.get_queue(guild.id)

            if not mq.queue:
                return await interaction.followup.send("❌ Queue is already empty.")

            count = len(mq.queue)
            mq.clear()
            await interaction.followup.send(f"🗑️ Cleared **{count}** track(s) from the queue.")
        except Exception as e:
            self.bot.logger.log(f"Error in clear command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to clear queue: {e}")

    @app_commands.command(name="remove", description="Remove a track from the queue by position.")
    @app_commands.describe(position="Position number in the queue (use /showqueue to see)")
    async def remove(self, interaction: discord.Interaction, position: int):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction)
            if error_msg:
                return await interaction.followup.send(error_msg)
                
            guild = interaction.guild
            mq = self.get_queue(guild.id)

            if not mq.queue:
                return await interaction.followup.send("❌ Queue is empty.")

            if position < 1 or position > len(mq.queue):
                return await interaction.followup.send(f"❌ Invalid position. Use a number between 1 and {len(mq.queue)}.")

            removed = mq.queue.pop(position - 1)
            await interaction.followup.send(f"🗑️ Removed #{position}: **{removed['title']}**")
        except Exception as e:
            self.bot.logger.log(f"Error in remove command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to remove track: {e}")

    # ----------------------------------------------------------
    # Voice Channel: Connect / Disconnect
    # ----------------------------------------------------------

    @app_commands.command(name="connect", description="Bot joins your voice channel.")
    async def connect(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            if not interaction.user.voice or not interaction.user.voice.channel:
                return await interaction.followup.send("❌ You are not in a voice channel.")
            
            channel = interaction.user.voice.channel
            guild = interaction.guild

            if guild.voice_client:
                return await interaction.followup.send("❌ Bot is already in a voice channel.")
            
            await channel.connect()
            await interaction.followup.send(f"🔊 Connected to **{channel.name}**.")
        except Exception as e:
            self.bot.logger.log(f"Error in connect command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to connect: {e}")

    @app_commands.command(name="disconnect", description="Bot leaves the voice channel.")
    async def disconnect(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            error_msg = await self.ensure_voice_state(interaction)
            if error_msg:
                return await interaction.followup.send(error_msg)
            
            guild = interaction.guild
            bot_channel = guild.voice_client.channel
            
            if guild.voice_client.is_playing() or guild.voice_client.is_paused():
                self.skip_advance.add(guild.id)
                guild.voice_client.stop()

            mq = self.get_queue(guild.id)
            mq.clear()
            mq.current = None

            channel_name = bot_channel.name
            await guild.voice_client.disconnect()
            await interaction.followup.send(f"👋 Disconnected from **{channel_name}**.")
        except Exception as e:
            self.bot.logger.log(f"Error in disconnect command: {e}\n")
            await interaction.followup.send(f"⚠️ Failed to disconnect: {e}")

async def setup(bot: commands.Bot):
    await bot.add_cog(MusicCog(bot))
