import sqlite3
import json
import discord
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from .IDatabase import IDatabase, DiscordEvent

class SQLiteDatabase(IDatabase):
    def __init__(self, db_path: str = "database.db"):
        self.db_path = db_path

    def _get_connection(self):
        # We use row factory to access columns by name
        conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize_schema(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Enable Foreign Keys
            cursor.execute("PRAGMA foreign_keys = ON;")

            # UserHistory: stores global properties of the user
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS UserHistory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    name TEXT,
                    global_name TEXT,
                    avatar_id INTEGER,
                    banner_id INTEGER,
                    bot BOOLEAN,
                    created_at TIMESTAMP,
                    timestamp TIMESTAMP NOT NULL,
                    FOREIGN KEY(avatar_id) REFERENCES Images(id),
                    FOREIGN KEY(banner_id) REFERENCES Images(id)
                )
            """)

            # GuildList: id, name, bot_joined_at
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS GuildList (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    bot_joined_at TIMESTAMP
                )
            """)

            # Images: storing avatar/banner details
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    hash TEXT UNIQUE NOT NULL,
                    cdn_url TEXT NOT NULL,
                    local_path TEXT
                )
            """)

            # MemberHistory
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS MemberHistory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    guild_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    nick TEXT,
                    guild_avatar_id INTEGER,
                    guild_banner_id INTEGER,
                    joined_at TIMESTAMP,
                    premium_since TIMESTAMP,
                    pending BOOLEAN,
                    timed_out_until TIMESTAMP,
                    raw_status TEXT,
                    mobile_status TEXT,
                    desktop_status TEXT,
                    web_status TEXT,
                    flags INTEGER,
                    left_at TIMESTAMP,
                    FOREIGN KEY(guild_id) REFERENCES GuildList(id),
                    FOREIGN KEY(guild_avatar_id) REFERENCES Images(id),
                    FOREIGN KEY(guild_banner_id) REFERENCES Images(id)
                )
            """)

            # MemberRolesHistory
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS MemberRolesHistory (
                    history_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    role_name TEXT NOT NULL,
                    FOREIGN KEY(history_id) REFERENCES MemberHistory(id) ON DELETE CASCADE
                )
            """)

            # MemberActivityHistory
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS MemberActivityHistory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    history_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    guild_id INTEGER NOT NULL,
                    name TEXT,
                    type TEXT,
                    details TEXT,
                    state TEXT,
                    url TEXT,
                    start TIMESTAMP,
                    end TIMESTAMP,
                    spotify_song_name TEXT,
                    spotify_artists TEXT,
                    spotify_album TEXT,
                    spotify_track_id TEXT,
                    started_at TIMESTAMP NOT NULL,
                    ended_at TIMESTAMP,
                    FOREIGN KEY(history_id) REFERENCES MemberHistory(id) ON DELETE CASCADE
                )
            """)

            # MemberVoiceStateHistory
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS MemberVoiceStateHistory (
                    history_id INTEGER NOT NULL,
                    channel_id INTEGER,
                    self_mute BOOLEAN,
                    self_deaf BOOLEAN,
                    self_stream BOOLEAN,
                    self_video BOOLEAN,
                    FOREIGN KEY(history_id) REFERENCES MemberHistory(id) ON DELETE CASCADE
                )
            """)

            conn.commit()

    def _ensure_user_and_guild(self, cursor, member: discord.Member):
        # the user history is handled separately now; just ensure guild logic remains intact

        
        # Insert guild if not exists. Note: bot_joined_at could be updated if needed, but for now we insert if missing.
        # member.guild.me.joined_at gets when the bot joined the guild.
        bot_member = member.guild.me
        bot_joined_at = bot_member.joined_at if bot_member else None
        
        cursor.execute("""
            INSERT INTO GuildList (id, name, bot_joined_at) 
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET name=excluded.name
        """, (member.guild.id, member.guild.name, bot_joined_at))

    def _get_or_create_image(self, cursor, asset: Optional[discord.Asset]) -> Optional[int]:
        if not asset:
            return None
            
        img_hash = getattr(asset, "key", str(asset)) # e.g., the hash or the full URL string if no hash
        if hasattr(asset, "key"):
            # asset.key is the hash
            img_hash = asset.key
        
        url = asset.url
        
        # Check if exists
        cursor.execute("SELECT id FROM Images WHERE hash = ?", (img_hash,))
        row = cursor.fetchone()
        if row:
            return row["id"]
            
        # Insert new
        cursor.execute("INSERT INTO Images (hash, cdn_url, local_path) VALUES (?, ?, ?)", (img_hash, url, None))
        return cursor.lastrowid

    def get_member_last_instance(self, user_id: int, guild_id: int) -> Optional[dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Fetch the latest MemberHistory row
            cursor.execute("""
                SELECT * FROM MemberHistory 
                WHERE user_id = ? AND guild_id = ? 
                ORDER BY timestamp DESC, id DESC LIMIT 1
            """, (user_id, guild_id))
            
            mh_row = cursor.fetchone()
            if not mh_row:
                return None
                
            history_id = mh_row["id"]
            
            # Fetch Roles
            cursor.execute("SELECT role_id, role_name FROM MemberRolesHistory WHERE history_id = ?", (history_id,))
            roles = [dict(r) for r in cursor.fetchall()]
            
            # Fetch Activities
            # Note: Now fetches from MemberActivityHistory based on active state (ended_at is NULL) or recent ending intersecting history row
            # For exact "last instance" compatibility, just return currently ongoing activities.
            cursor.execute("""
                SELECT * FROM MemberActivityHistory
                WHERE user_id = ? AND guild_id = ? AND ended_at IS NULL
            """, (user_id, guild_id))
            activities = [dict(a) for a in cursor.fetchall()]


            # Fetch Voice State
            cursor.execute("SELECT * FROM MemberVoiceStateHistory WHERE history_id = ?", (history_id,))
            voice_row = cursor.fetchone()
            voice_state = dict(voice_row) if voice_row else None
            
            # Construct the comprehensive dict
            result = dict(mh_row)
            result["roles"] = roles
            result["activities"] = activities
            result["voice_state"] = voice_state
            
            # Helper: get image hashes
            if result.get("guild_avatar_id"):
                cursor.execute("SELECT hash, cdn_url FROM Images WHERE id = ?", (result["guild_avatar_id"],))
                row = cursor.fetchone()
                if row:
                    result["guild_avatar_hash"] = row["hash"]
            if result.get("guild_banner_id"):
                cursor.execute("SELECT hash, cdn_url FROM Images WHERE id = ?", (result["guild_banner_id"],))
                row = cursor.fetchone()
                if row:
                    result["guild_banner_hash"] = row["hash"]
                    
            return result

    def get_member_instance_at(self, user_id: int, guild_id: int, target_time: datetime) -> Optional[dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Fetch the latest MemberHistory row at or before target_time
            cursor.execute("""
                SELECT * FROM MemberHistory 
                WHERE user_id = ? AND guild_id = ? AND timestamp <= ?
                ORDER BY timestamp DESC, id DESC LIMIT 1
            """, (user_id, guild_id, target_time))
            
            mh_row = cursor.fetchone()
            if not mh_row:
                return None
                
            history_id = mh_row["id"]
            
            # Fetch Roles
            cursor.execute("SELECT role_id, role_name FROM MemberRolesHistory WHERE history_id = ?", (history_id,))
            roles = [dict(r) for r in cursor.fetchall()]
            
            # Fetch Activities
            # For point-in-time, we want activities that started before target_time, and either haven't ended or ended after target_time.
            cursor.execute("""
                SELECT * FROM MemberActivityHistory 
                WHERE user_id = ? AND guild_id = ? 
                  AND started_at <= ? 
                  AND (ended_at IS NULL OR ended_at >= ?)
            """, (user_id, guild_id, target_time, target_time))
            activities = [dict(a) for a in cursor.fetchall()]
            
            # Fetch Voice State
            cursor.execute("SELECT * FROM MemberVoiceStateHistory WHERE history_id = ?", (history_id,))
            voice_row = cursor.fetchone()
            voice_state = dict(voice_row) if voice_row else None
            
            # Construct dictionary
            result = dict(mh_row)
            result["roles"] = roles
            result["activities"] = activities
            result["voice_state"] = voice_state
            
            if result.get("guild_avatar_id"):
                cursor.execute("SELECT hash, cdn_url FROM Images WHERE id = ?", (result["guild_avatar_id"],))
                row = cursor.fetchone()
                if row:
                    result["guild_avatar_hash"] = row["hash"]
            if result.get("guild_banner_id"):
                cursor.execute("SELECT hash, cdn_url FROM Images WHERE id = ?", (result["guild_banner_id"],))
                row = cursor.fetchone()
                if row:
                    result["guild_banner_hash"] = row["hash"]
                    
            return result

    def _is_state_different(self, current_data: dict, last_data: dict) -> bool:
        # Check standard scalar properties
        keys_to_compare = [
            "nick", "joined_at", "premium_since", "pending", "timed_out_until",
            "raw_status", "mobile_status", "desktop_status", "web_status", "flags", "left_at"
        ]
        
        for key in keys_to_compare:
            curr_val = current_data.get(key)
            last_val = last_data.get(key)
            
            # Normalize datetime comparison since SQLite returns ISO strings
            if isinstance(curr_val, datetime):
                curr_val = curr_val.strftime('%Y-%m-%d %H:%M:%S')
                if curr_val.endswith(" 00:00:00"): 
                    curr_val = curr_val.replace(" 00:00:00", "") # Normalize Discord Py behaviors
                    
            if isinstance(last_val, str) and len(last_val) > 19:
                # Truncate fractional seconds for safe comparison
                last_val = last_val[:19]
            elif isinstance(last_val, datetime):
                last_val = last_val.strftime('%Y-%m-%d %H:%M:%S')

            # Normalize Booleans to Integers
            if isinstance(curr_val, bool): curr_val = 1 if curr_val else 0
            if isinstance(last_val, bool): last_val = 1 if last_val else 0

            # Treat entirely missing/None as equal
            if curr_val is None and last_val is None:
                continue

            # Final scalar string check
            if str(curr_val) != str(last_val):
                return True
                
        # Compare avatar/banner IDs
        if current_data.get("guild_avatar_id") != last_data.get("guild_avatar_id"):
            return True
        if current_data.get("guild_banner_id") != last_data.get("guild_banner_id"):
            return True

        # Compare Roles (List of dicts: role_id, role_name)
        curr_roles = sorted([r["role_id"] for r in current_data.get("roles", [])])
        last_roles = sorted([r["role_id"] for r in last_data.get("roles", [])])
        if curr_roles != last_roles:
            return True
            
        # Compare Voice State
        curr_voice = current_data.get("voice_state") or {}
        last_voice = last_data.get("voice_state") or {}
        # remove id/history_id for comparison and treat None as equal missing
        def normalize_voice(v_dict):
            cleaned = {}
            for k, v in v_dict.items():
                if k in ["id", "history_id"] or v is None: continue
                cleaned[k] = 1 if v is True else (0 if v is False else v)
            return cleaned
            
        curr_vs = normalize_voice(curr_voice)
        last_vs = normalize_voice(last_voice)
        
        if curr_vs != last_vs:
            return True
            
        return False
        
    def _is_activity_state_different(self, current_activities: list, last_activities: list) -> bool:
        if len(current_activities) != len(last_activities):
            return True
            
        def clean_act(act):
            cleaned = {}
            # ignore IDs, timestamps explicitly so we don't trigger updates for mere time ticks
            for k, v in act.items():
                if k in ["id", "history_id", "start", "end", "started_at", "ended_at", "user_id", "guild_id"]:
                     continue
                
                # Exclude state for rich presence time-remaining ticks
                if k == "state":
                     continue
                     
                if isinstance(v, datetime):
                    cleaned[k] = v.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    cleaned[k] = str(v)
            return cleaned
            
        ca_list = [clean_act(a) for a in current_activities]
        la_list = [clean_act(a) for a in last_activities]
        
        def act_sort_key(act_dict):
            return str(act_dict.get("name", "")) + str(act_dict.get("type", "")) + str(act_dict.get("details", ""))
            
        ca_list.sort(key=act_sort_key)
        la_list.sort(key=act_sort_key)
        
        if ca_list != la_list:
            return True
                
        return False

    def insert_member_history(self, member: discord.Member, event_type: DiscordEvent):
        timestamp = datetime.now(timezone.utc)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            self._ensure_user_and_guild(cursor, member)
        
        # Process Images early
        avatar_id = self._get_or_create_image(cursor, member.guild_avatar)
        banner_id = self._get_or_create_image(cursor, getattr(member, 'guild_banner', None))
        
        # Construct scalar state
        # If the event is MEMBER_REMOVE, member might be populated but the user left.
        left_at = timestamp if event_type == DiscordEvent.MEMBER_REMOVE else None
        
        current_data = {
            "nick": member.nick,
            "guild_avatar_id": avatar_id,
            "guild_banner_id": banner_id,
            "joined_at": member.joined_at,
            "premium_since": member.premium_since,
            "pending": member.pending,
            "timed_out_until": member.timed_out_until,
            "raw_status": str(member.status),
            "mobile_status": str(member.mobile_status),
            "desktop_status": str(member.desktop_status),
            "web_status": str(member.web_status),
            "flags": member.flags.value if member.flags else 0,
            "left_at": left_at,
            "roles": [{"role_id": r.id, "role_name": r.name} for r in member.roles],
            "activities": [],
            "voice_state": None
        }
        
        # Activities
        for act in member.activities:
            act_data = {
                "name": getattr(act, "name", None),
                "type": str(getattr(act, "type", "")) or None,
                "details": getattr(act, "details", None),
                "state": getattr(act, "state", None),
                "url": getattr(act, "url", None),
                "spotify_song_name": None,
                "spotify_artists": None,
                "spotify_album": None,
                "spotify_track_id": None
            }
            if hasattr(act, "start"): act_data["start"] = act.start
            if hasattr(act, "end"): act_data["end"] = act.end
            
            if isinstance(act, discord.Spotify):
                act_data["spotify_song_name"] = act.title
                act_data["spotify_artists"] = ", ".join(act.artists)
                act_data["spotify_album"] = act.album
                act_data["spotify_track_id"] = act.track_id
                
            current_data["activities"].append(act_data)
        
        # Voice State
        if member.voice:
            current_data["voice_state"] = {
                "channel_id": member.voice.channel.id if member.voice.channel else None,
                "self_mute": member.voice.self_mute,
                "self_deaf": member.voice.self_deaf,
                "self_stream": member.voice.self_stream,
                "self_video": getattr(member.voice, "self_video", False)
            }
            
        # Delta Check
        last_data = self.get_member_last_instance(member.id, member.guild.id)
        
        # Decide if we need to insert a MemberHistory profile snapshot
        needs_snapshot = True
        history_id = None
        
        if last_data:
            state_changed = self._is_state_different(current_data, last_data)
            if not state_changed and event_type not in [DiscordEvent.MEMBER_JOIN, DiscordEvent.MEMBER_REMOVE]:
                needs_snapshot = False
                history_id = last_data["id"]
                
        if needs_snapshot:
            # Need to insert
            cursor.execute("""
                INSERT INTO MemberHistory (
                    user_id, guild_id, event_type, timestamp, nick,
                    guild_avatar_id, guild_banner_id, joined_at, premium_since, pending,
                    timed_out_until, raw_status, mobile_status, desktop_status, web_status, flags, left_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                member.id, member.guild.id, event_type.value, timestamp, current_data["nick"],
                current_data["guild_avatar_id"], current_data["guild_banner_id"], current_data["joined_at"],
                current_data["premium_since"], current_data["pending"], current_data["timed_out_until"],
                current_data["raw_status"], current_data["mobile_status"], current_data["desktop_status"],
                current_data["web_status"], current_data["flags"], current_data["left_at"]
            ))
            history_id = cursor.lastrowid
            
            # Only insert roles & voice states if we created a new snapshot
            for role in current_data["roles"]:
                cursor.execute("""
                    INSERT INTO MemberRolesHistory (history_id, role_id, role_name) VALUES (?, ?, ?)
                """, (history_id, role["role_id"], role["role_name"]))
                
            voice = current_data["voice_state"]
            if voice:
                cursor.execute("""
                    INSERT INTO MemberVoiceStateHistory (history_id, channel_id, self_mute, self_deaf, self_stream, self_video)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    history_id, voice.get("channel_id"), voice.get("self_mute"), voice.get("self_deaf"),
                    voice.get("self_stream"), voice.get("self_video")
                ))
        
        # Now process Activities Tracking Independently
        current_acts_cleaned = []
        for act in current_data["activities"]:
            # create a key dictionary to uniquely identify the "running instance" of an act
            # Ensure None values are uniformly represented as empty strings to avoid "None" != "" mismatches
            name_str = str(act.get("name") or "")
            type_str = str(act.get("type") or "")
            details_str = str(act.get("details") or "")
            
            c_dict = {
                "name": act.get("name"),
                "type": act.get("type"),
                "details": act.get("details"),
                "clean_key": name_str + type_str + details_str
            }
            current_acts_cleaned.append(c_dict)

        # Get the ongoing activities directly from DB
        cursor.execute("""
            SELECT id, name, type, details FROM MemberActivityHistory
            WHERE user_id = ? AND guild_id = ? AND ended_at IS NULL
        """, (member.id, member.guild.id))
        
        ongoing_db_acts = []
        for row in cursor.fetchall():
            name_str = str(row["name"] if row["name"] is not None else "")
            type_str = str(row["type"] if row["type"] is not None else "")
            details_str = str(row["details"] if row["details"] is not None else "")
            
            ongoing_db_acts.append({
                "id": row["id"],
                "clean_key": name_str + type_str + details_str
            })
        # List of clean_keys currently active in discord
        current_keys = [c["clean_key"] for c in current_acts_cleaned]
        
        # 1. Any DB activity not in current_keys has ENDED
        ended_ids = []
        for db_act in ongoing_db_acts:
            if db_act["clean_key"] not in current_keys:
                ended_ids.append(db_act["id"])
                
        if ended_ids:
            placeholders = ",".join("?" for _ in ended_ids)
            cursor.execute(f"""
                UPDATE MemberActivityHistory 
                SET ended_at = ?
                WHERE id IN ({placeholders})
            """, [timestamp] + ended_ids)

        # 2. Any current activity not in ongoing_db_acts is NEW
        db_keys = [d["clean_key"] for d in ongoing_db_acts]
        
        new_acts_to_insert = []
        for act in current_data["activities"]:
            name_str = str(act.get("name") or "")
            type_str = str(act.get("type") or "")
            details_str = str(act.get("details") or "")
            clean_key = name_str + type_str + details_str
            
            if clean_key not in db_keys:
                new_acts_to_insert.append(act)

        if new_acts_to_insert:
            for act in new_acts_to_insert:
                cursor.execute("""
                    INSERT INTO MemberActivityHistory (
                        history_id, user_id, guild_id, name, type, details, state, url, start, end, 
                        spotify_song_name, spotify_artists, spotify_album, spotify_track_id, started_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    history_id if history_id else (last_data["id"] if last_data else 0), 
                    member.id, member.guild.id, act.get("name"), act.get("type"), act.get("details"), 
                    act.get("state"), act.get("url"), act.get("start"), act.get("end"),
                    act.get("spotify_song_name"), act.get("spotify_artists"), act.get("spotify_album"), act.get("spotify_track_id"),
                    timestamp
                ))
                
        conn.commit()

    def get_user_last_instance(self, user_id: int) -> Optional[dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM UserHistory 
                WHERE user_id = ?
                ORDER BY timestamp DESC, id DESC LIMIT 1
            """, (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def insert_user_history(self, user: discord.User, event_type: DiscordEvent):
        timestamp = datetime.now(timezone.utc)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            avatar_id = self._get_or_create_image(cursor, user.avatar)
            banner_id = self._get_or_create_image(cursor, getattr(user, 'banner', None))
            
            current_data = {
                "user_id": user.id,
                "name": getattr(user, "name", None),
                "global_name": getattr(user, "global_name", None),
                "avatar_id": avatar_id,
                "banner_id": banner_id,
                "bot": 1 if getattr(user, "bot", False) else 0,
                "created_at": getattr(user, "created_at", None)
            }
            
            last_data = self.get_user_last_instance(user.id)
            
            needs_snapshot = True
            if last_data:
                state_changed = False
                for key in ["name", "global_name", "avatar_id", "banner_id", "bot", "created_at"]:
                    curr_val = current_data.get(key)
                    last_val = last_data.get(key)
                    
                    if isinstance(curr_val, datetime):
                        curr_val = curr_val.strftime('%Y-%m-%d %H:%M:%S')
                        if curr_val.endswith(" 00:00:00"): curr_val = curr_val.replace(" 00:00:00", "")
                    
                    if isinstance(last_val, str) and len(last_val) > 19:
                        last_val = last_val[:19]
                    elif last_val and isinstance(last_val, datetime):
                        last_val = last_val.strftime('%Y-%m-%d %H:%M:%S')
                        
                    if str(curr_val) != str(last_val):
                        state_changed = True
                        break
                        
                if not state_changed:
                    needs_snapshot = False
                    
            if needs_snapshot:
                cursor.execute("""
                    INSERT INTO UserHistory (
                        user_id, name, global_name, avatar_id, banner_id, bot, created_at, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    current_data["user_id"], current_data["name"], current_data["global_name"], 
                    current_data["avatar_id"], current_data["banner_id"], current_data["bot"], 
                    current_data["created_at"], timestamp
                ))
                
            conn.commit()
