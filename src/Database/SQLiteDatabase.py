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

            # UserList: only ID since global properties go to UserHistory later
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS UserList (
                    id INTEGER PRIMARY KEY
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
                    FOREIGN KEY(user_id) REFERENCES UserList(id),
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
                    history_id INTEGER NOT NULL,
                    name TEXT,
                    type TEXT,
                    details TEXT,
                    state TEXT,
                    url TEXT,
                    start TIMESTAMP,
                    end TIMESTAMP,
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
        # Insert user if not exists
        cursor.execute("INSERT OR IGNORE INTO UserList (id) VALUES (?)", (member.id,))
        
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
            cursor.execute("SELECT * FROM MemberActivityHistory WHERE history_id = ?", (history_id,))
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
            cursor.execute("SELECT * FROM MemberActivityHistory WHERE history_id = ?", (history_id,))
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
            
        # Compare Activities
        curr_acts = current_data.get("activities", [])
        last_acts = last_data.get("activities", [])
        if len(curr_acts) != len(last_acts):
            return True
            
        def clean_act(act):
            # ignore IDs and convert datetime to string to match SQLite fetch output
            cleaned = {}
            for k, v in act.items():
                if k in ["id", "history_id"] or v is None: continue
                if isinstance(v, datetime):
                    cleaned[k] = v.strftime('%Y-%m-%d %H:%M:%S') # approximate matching
                else:
                    cleaned[k] = str(v)
            return cleaned
            
        # Check all cleaned activities
        ca_list = [clean_act(a) for a in curr_acts]
        la_list = [clean_act(a) for a in last_acts]
        
        # Sort both lists safely to compare regardless of order
        def act_sort_key(act_dict):
            return str(act_dict.get("name", "")) + str(act_dict.get("type", "")) + str(act_dict.get("state", ""))
            
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
                "url": getattr(act, "url", None)
            }
            # Handles Spotify/custom start end dates
            if hasattr(act, "start"): act_data["start"] = act.start
            if hasattr(act, "end"): act_data["end"] = act.end
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
        if last_data:
            state_changed = self._is_state_different(current_data, last_data)
            # We always log MEMBER_REMOVE and MEMBER_JOIN regardless of delta check
            if not state_changed and event_type not in [DiscordEvent.MEMBER_JOIN, DiscordEvent.MEMBER_REMOVE]:
                return # No need to save a duplicated snapshot
                
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
        
        # Insert Roles
        for role in current_data["roles"]:
            cursor.execute("""
                INSERT INTO MemberRolesHistory (history_id, role_id, role_name) VALUES (?, ?, ?)
            """, (history_id, role["role_id"], role["role_name"]))
            
        # Insert Activities
        for act in current_data["activities"]:
            cursor.execute("""
                INSERT INTO MemberActivityHistory (history_id, name, type, details, state, url, start, end)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                history_id, act.get("name"), act.get("type"), act.get("details"), 
                act.get("state"), act.get("url"), act.get("start"), act.get("end")
            ))
            
        # Insert Voice State
        voice = current_data["voice_state"]
        if voice:
            cursor.execute("""
                INSERT INTO MemberVoiceStateHistory (history_id, channel_id, self_mute, self_deaf, self_stream, self_video)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                history_id, voice.get("channel_id"), voice.get("self_mute"), voice.get("self_deaf"),
                voice.get("self_stream"), voice.get("self_video")
            ))
        
        conn.commit()
