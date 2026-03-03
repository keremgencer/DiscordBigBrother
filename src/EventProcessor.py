import discord
from Database.IDatabase import IDatabase, DiscordEvent
import Logger

class EventProcessor:
    def __init__(self, db: IDatabase, logger: Logger.Logger):
        self.db = db
        self.logger = logger

    async def process_system_init(self, client: discord.Client):
        for guild in client.guilds:
            for member in guild.members:
                try:
                    self.db.insert_member_history(member, DiscordEvent.SYSTEM_INIT)
                except Exception as db_e:
                    self.logger.log(f"DB Error tracking init for {member.name}: {db_e}\n")

    async def process_member_join(self, member):
        try:
            self.db.insert_member_history(member, DiscordEvent.MEMBER_JOIN)
            message = ""
            message += f"{member.name} joined the server.\n" #todo member list gibi detaylar eklenebilir
            
            if message.strip():
                self.logger.log(message)
        except Exception as e:
            self.logger.log(f"Error in on_member_join: {e}\n")

    async def process_member_remove(self, member):
        try:
            self.db.insert_member_history(member, DiscordEvent.MEMBER_REMOVE)
            message = ""
            message += f"{member.name} left the server.\n"
            if message.strip():
                self.logger.log(message)
        except Exception as e:
            self.logger.log(f"Error in on_member_remove: {e}\n")

    async def process_member_update(self, before, after):
        try:
            self.db.insert_member_history(after, DiscordEvent.MEMBER_UPDATE)
            message = ""
            member_name = after.name

            if before.nick != after.nick:
                old_nick = before.nick if before.nick else before.name
                new_nick = after.nick if after.nick else after.name
                message += f"🏷️ {member_name} changed their server nickname: '{old_nick}' -> '{new_nick}'.\n"

            if before.roles != after.roles:
                added_roles = [r.name for r in after.roles if r not in before.roles]
                removed_roles = [r.name for r in before.roles if r not in after.roles]
                if added_roles: message += f"➕ Roles added to {member_name}: {', '.join(added_roles)}\n"
                if removed_roles: message += f"➖ Roles removed from {member_name}: {', '.join(removed_roles)}\n"

            if before.timed_out_until != after.timed_out_until:
                if after.timed_out_until:
                    message += f"🔇 {member_name} was timed out (Until: {after.timed_out_until.strftime('%Y-%m-%d %H:%M:%S')}).\n"
                else:
                    message += f"🔊 {member_name}'s timeout ended or was removed.\n"

            if before.premium_since != after.premium_since:
                if after.premium_since: message += f"🚀 {member_name} started boosting the server!\n"
                else: message += f"📉 {member_name} stopped boosting the server.\n"

            if before.guild_avatar != after.guild_avatar:
                message += f"🖼️ {member_name} changed their guild avatar.\n"

            if before.guild_banner != after.guild_banner:
                message += f"🎌 {member_name} changed their guild banner.\n"

            if before.pending != after.pending and not after.pending:
                message += f"✅ {member_name} accepted the server rules and became a full member.\n"

            if message.strip():
                self.logger.log(message)
        except Exception as e:
            self.logger.log(f"Error in on_member_update: {e}\n")

    async def process_presence_update(self, before, after):
        try:
            self.db.insert_member_history(after, DiscordEvent.PRESENCE_UPDATE)
            message = ""
            member_name = after.name

            status_emojis = {
                discord.Status.online: "🟢",
                discord.Status.do_not_disturb: "🔴",
                discord.Status.dnd: "🔴",
                discord.Status.idle: "🟡",
                discord.Status.offline: "🟣",
                discord.Status.invisible: "🟣"
            }

            device_statuses = [
                ("desktop", "💻", before.desktop_status, after.desktop_status),
                ("mobile", "📱", before.mobile_status, after.mobile_status),
                ("web", "🌐", before.web_status, after.web_status)
            ]

            device_changed = False

            for device_name, device_emoji, old_stat, new_stat in device_statuses:
                if old_stat != new_stat:
                    emoji = status_emojis.get(new_stat, "⚪")
                    message += f"{emoji} {device_emoji} {member_name} changed their {device_name} status to '{new_stat}'.\n"
                    device_changed = True

            if not device_changed and before.status != after.status:
                emoji = status_emojis.get(after.status, "⚪")
                message += f"{emoji} 👤 {member_name} changed their overall status to '{after.status}'.\n"

            if before.activities != after.activities:
                if after.activities:
                    message += f"🎮 {member_name} updated their activities:\n"
                    for act in after.activities:
                        if isinstance(act, discord.Spotify):
                            message += f"  🎵 Listening to Spotify:\n"
                            message += f"      - Song: {act.title}\n"
                            message += f"      - Artist(s): {', '.join(act.artists)}\n"
                            message += f"      - Album: {act.album}\n"
                            message += f"      - Track URL: https://open.spotify.com/track/{act.track_id}\n"
                            if act.start and act.end:
                                duration = act.end - act.start
                                minutes, seconds = divmod(int(duration.total_seconds()), 60)
                                message += f"      - Duration: {minutes:02d}:{seconds:02d}\n"
                                message += f"      - Started at: {act.start.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n"
                                message += f"      - Gonna end at: {act.end.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n"
                        elif isinstance(act, discord.CustomActivity):
                            emoji = f"{act.emoji} " if act.emoji else ""
                            text = act.name if act.name else ""
                            message += f"  💬 Custom Status: {emoji}{text}\n"
                        else:
                            act_type = act.type.name.capitalize() if hasattr(act, 'type') else "Doing"
                            message += f"  🔹 {act_type}: {act.name}\n"
                            if getattr(act, 'details', None): message += f"      - Details: {act.details}\n"
                            if getattr(act, 'state', None): message += f"      - State: {act.state}\n"
                            if getattr(act, 'platform', None): message += f"      - Platform: {act.platform}\n"
                            if getattr(act, 'url', None): message += f"      - Stream URL: {act.url}\n"
                            if getattr(act, 'start', None): message += f"      - Started at: {act.start.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n"
                            if getattr(act, 'end', None): message += f"      - Ends at: {act.end.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n"
                            if getattr(act, 'details_url', None): message += f"      - Details URL: {act.details_url}\n"
                            if getattr(act, 'state_url', None): message += f"      - State URL: {act.state_url}\n"
                            
                            large_url = getattr(act, 'large_image_url', None)
                            large_text = getattr(act, 'large_image_text', None)
                            if large_url or large_text:
                                message += f"      - Large Image: [Hover: {large_text or 'None'}] ({large_url or 'No URL'})\n"
                            small_url = getattr(act, 'small_image_url', None)
                            small_text = getattr(act, 'small_image_text', None)
                            if small_url or small_text:
                                message += f"      - Small Image: [Hover: {small_text or 'None'}] ({small_url or 'No URL'})\n"
                else:
                    message += f"🛑 {member_name} closed all activities/games.\n"

            if message.strip():
                self.logger.log(message)
        except Exception as e:
            self.logger.log(f"Error in on_presence_update: {e}\n")

    async def process_voice_state_update(self, member, before, after):
        try:
            self.db.insert_member_history(member, DiscordEvent.VOICE_UPDATE)
            message = ""
            member_name = member.name

            if before.channel != after.channel:
                if before.channel is None:
                    message += f"🎤 {member_name} JOINED the voice channel '{after.channel.name}'.\n"
                elif after.channel is None:
                    message += f"🚪 {member_name} LEFT the voice channel '{before.channel.name}'.\n"
                else:
                    message += f"🔄 {member_name} SWITCHED voice channels: '{before.channel.name}' -> '{after.channel.name}'.\n"

            if before.self_stream != after.self_stream:
                if after.self_stream: message += f"📺 {member_name} STARTED screen sharing (stream).\n"
                else: message += f"📺 {member_name} STOPPED screen sharing.\n"

            if before.self_video != after.self_video:
                if after.self_video: message += f"📷 {member_name} TURNED ON their camera.\n"
                else: message += f"📷 {member_name} TURNED OFF their camera.\n"

            if before.self_mute != after.self_mute:
                if after.self_mute: message += f"🎙️ {member_name} MUTED their microphone.\n"
                else: message += f"🎙️ {member_name} UNMUTED their microphone.\n"

            if before.self_deaf != after.self_deaf:
                if after.self_deaf: message += f"🎧 {member_name} DEAFENED their headset.\n"
                else: message += f"🎧 {member_name} UNDEAFENED their headset.\n"

            if message.strip():
                self.logger.log(message)
        except Exception as e:
            self.logger.log(f"Error in on_voice_state_update: {e}\n")
