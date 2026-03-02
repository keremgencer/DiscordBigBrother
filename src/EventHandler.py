import Logger
class EventHandler:
    def __init__(self, client):
        self.client = client
        self.logger = Logger.Logger()

    def handleEvents(self):
        @self.client.event
        async def on_ready():
            try:
                print(f'Logged in as {self.client.user}')
                print('Started logging activities... (everything will be logged in activity_log.txt)')
                print('-' * 50)
            except Exception as e:
                self.logger.log(f"Error in on_ready: {e}\n")

        @self.client.event
        async def on_member_join(member):
            try:
                message = ""
                message += f"{member.name} joined the server.\n" #todo member list gibi detaylar eklenebilir
                
                if message.strip():  # Eğer mesaj boş değilse logla
                    self.logger.log(message)

            except Exception as e:
                self.logger.log(f"Error in on_member_join: {e}\n")

        @self.client.event
        async def on_member_remove(member): 
            try:
                message = ""
                message += f"{member.name} left the server.\n" #todo member list gibi detaylar eklenebilir
                if message.strip():  # Eğer mesaj boş değilse logla
                    self.logger.log(message)

            except Exception as e:
                self.logger.log(f"Error in on_member_remove: {e}\n")


        # ---------------------------------------------------------
        # 1. GUILD PROFILE UPDATES (on_member_update)
        # ---------------------------------------------------------
        @self.client.event
        async def on_member_update(before, after):
            try:
                message = ""
                member_name = after.name

                # Server Nickname
                if before.nick != after.nick:
                    old_nick = before.nick if before.nick else before.name
                    new_nick = after.nick if after.nick else after.name
                    message += f"🏷️ {member_name} changed their server nickname: '{old_nick}' -> '{new_nick}'.\n"

                # Roles
                if before.roles != after.roles:
                    added_roles = [r.name for r in after.roles if r not in before.roles]
                    removed_roles = [r.name for r in before.roles if r not in after.roles]
                    if added_roles: message += f"➕ Roles added to {member_name}: {', '.join(added_roles)}\n"
                    if removed_roles: message += f"➖ Roles removed from {member_name}: {', '.join(removed_roles)}\n"

                # Timeout (timed_out_until)
                if before.timed_out_until != after.timed_out_until:
                    if after.timed_out_until:
                        message += f"🔇 {member_name} was timed out (Until: {after.timed_out_until.strftime('%Y-%m-%d %H:%M:%S')}).\n"
                    else:
                        message += f"🔊 {member_name}'s timeout ended or was removed.\n"

                # Server Boost (premium_since)
                if before.premium_since != after.premium_since:
                    if after.premium_since: message += f"🚀 {member_name} started boosting the server!\n"
                    else: message += f"📉 {member_name} stopped boosting the server.\n"

                # Guild-Specific Avatar
                if before.guild_avatar != after.guild_avatar:
                    message += f"🖼️ {member_name} changed their guild avatar.\n"

                # Guild-Specific Banner
                if before.guild_banner != after.guild_banner:
                    message += f"🎌 {member_name} changed their guild banner.\n"

                # Pending / Rules Acceptance
                if before.pending != after.pending and not after.pending:
                    message += f"✅ {member_name} accepted the server rules and became a full member.\n"
        

                if message.strip():
                    self.logger.log(message)
            except Exception as e:
                self.logger.log(f"Error in on_member_update: {e}\n")
        
        
        # ---------------------------------------------------------
        # 2. STATUS AND ACTIVITY UPDATES (on_presence_update)
        # ---------------------------------------------------------
        @self.client.event
        async def on_presence_update(before, after):
            try:
                message = ""
                member_name = after.name

                # Emoji mapping for different statuses
                status_emojis = {
                    discord.Status.online: "🟢",
                    discord.Status.do_not_disturb: "🔴",
                    discord.Status.dnd: "🔴",
                    discord.Status.idle: "🟡",
                    discord.Status.offline: "🟣",
                    discord.Status.invisible: "🟣"
                }

                # List of devices to check (name, emoji, before_status, after_status)
                device_statuses = [
                    ("desktop", "💻", before.desktop_status, after.desktop_status),
                    ("mobile", "📱", before.mobile_status, after.mobile_status),
                    ("web", "🌐", before.web_status, after.web_status)
                ]

                device_changed = False

                # 1. Check if any specific device status changed
                for device_name, device_emoji, old_stat, new_stat in device_statuses:
                    if old_stat != new_stat:
                        emoji = status_emojis.get(new_stat, "⚪") # Get the emoji, default to ⚪ if unknown
                        message += f"{emoji} {device_emoji} {member_name} changed their {device_name} status to '{new_stat}'.\n"
                        device_changed = True

                # 2. Fallback: If overall status changed but no specific device was detected
                # (This usually happens when a user goes 'Invisible' globally)
                if not device_changed and before.status != after.status:
                    emoji = status_emojis.get(after.status, "⚪")
                    message += f"{emoji} 👤 {member_name} changed their overall status to '{after.status}'.\n"

                
                # Activities / Games / Spotify / Custom Status
                if before.activities != after.activities:
                    if after.activities:
                        message += f"🎮 {member_name} updated their activities:\n"

                        for act in after.activities:

                            # 1. SPOTIFY ACTIVITY
                            if isinstance(act, discord.Spotify):
                                message += f"  🎵 Listening to Spotify:\n"
                                message += f"      - Song: {act.title}\n"
                                message += f"      - Artist(s): {', '.join(act.artists)}\n"
                                message += f"      - Album: {act.album}\n"
                                message += f"      - Track URL: https://open.spotify.com/track/{act.track_id}\n"

                                if act.start and act.end:
                                    # Toplam şarkı süresini hesaplama
                                    duration = act.end - act.start
                                    # Sadece dakika ve saniye kısmını alıp formatlama
                                    minutes, seconds = divmod(int(duration.total_seconds()), 60)
                                    message += f"      - Duration: {minutes:02d}:{seconds:02d}\n"
                                    message += f"      - Started at: {act.start.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n"
                                    message += f"      - Gonna end at: {act.end.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n"


                            # 2. CUSTOM STATUS
                            elif isinstance(act, discord.CustomActivity):
                                emoji = f"{act.emoji} " if act.emoji else ""
                                text = act.name if act.name else ""
                                message += f"  💬 Custom Status: {emoji}{text}\n"

                            # 3. NORMAL GAMES / RICH PRESENCE
                            else:
                                act_type = act.type.name.capitalize() if hasattr(act, 'type') else "Doing"
                                message += f"  🔹 {act_type}: {act.name}\n"

                                # Basic details
                                if getattr(act, 'details', None): message += f"      - Details: {act.details}\n"
                                if getattr(act, 'state', None): message += f"      - State: {act.state}\n"
                                if getattr(act, 'platform', None): message += f"      - Platform: {act.platform}\n"
                                if getattr(act, 'url', None): message += f"      - Stream URL: {act.url}\n"

                                # Timestamps
                                if getattr(act, 'start', None): message += f"      - Started at: {act.start.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n"
                                if getattr(act, 'end', None): message += f"      - Ends at: {act.end.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n"

                                # Clickable URLs
                                if getattr(act, 'details_url', None): message += f"      - Details URL: {act.details_url}\n"
                                if getattr(act, 'state_url', None): message += f"      - State URL: {act.state_url}\n"

                                # Assets (Images and Hover texts)
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
        
        
        # ---------------------------------------------------------
        # 3. VOICE CHANNEL UPDATES (on_voice_state_update)
        # ---------------------------------------------------------
        @self.client.event
        async def on_voice_state_update(member, before, after):
            try:
                message = ""
                member_name = member.name

                # Joining / Leaving / Switching Channels
                if before.channel != after.channel:
                    if before.channel is None:
                        message += f"🎤 {member_name} JOINED the voice channel '{after.channel.name}'.\n"
                    elif after.channel is None:
                        message += f"🚪 {member_name} LEFT the voice channel '{before.channel.name}'.\n"
                    else:
                        message += f"🔄 {member_name} SWITCHED voice channels: '{before.channel.name}' -> '{after.channel.name}'.\n"

                # Screen Sharing / Streaming
                if before.self_stream != after.self_stream:
                    if after.self_stream: message += f"📺 {member_name} STARTED screen sharing (stream).\n"
                    else: message += f"📺 {member_name} STOPPED screen sharing.\n"

                # Camera (Video)
                if before.self_video != after.self_video:
                    if after.self_video: message += f"📷 {member_name} TURNED ON their camera.\n"
                    else: message += f"📷 {member_name} TURNED OFF their camera.\n"

                # Microphone Mute/Unmute
                if before.self_mute != after.self_mute:
                    if after.self_mute: message += f"🎙️ {member_name} MUTED their microphone.\n"
                    else: message += f"🎙️ {member_name} UNMUTED their microphone.\n"

                # Headset Deafen/Undeafen
                if before.self_deaf != after.self_deaf:
                    if after.self_deaf: message += f"🎧 {member_name} DEAFENED their headset.\n"
                    else: message += f"🎧 {member_name} UNDEAFENED their headset.\n"

                if message.strip():
                    self.logger.log(message)
            except Exception as e:
                self.logger.log(f"Error in on_voice_state_update: {e}\n")

#todo: on_user_update (global profile changes like username, discriminator, avatar) kesin eklenecek.
#todo şu anda dinlenen eventlerin detaylarına inilebilir.
#todo: bir database e bağlayıp sonrasında sql komutları ile filtreleme işi basitleştirilebilir.
#todo database sonradan arayüze de bağlanabilir. (örneğin web tabanlı bir dashboard)