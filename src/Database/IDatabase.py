from abc import ABC, abstractmethod
from enum import Enum
import discord
from datetime import datetime
from typing import Optional

class DiscordEvent(Enum):
    MEMBER_JOIN = "MEMBER_JOIN"
    MEMBER_REMOVE = "MEMBER_REMOVE"
    MEMBER_UPDATE = "MEMBER_UPDATE"
    PRESENCE_UPDATE = "PRESENCE_UPDATE"
    VOICE_UPDATE = "VOICE_UPDATE"
    USER_UPDATE = "USER_UPDATE"
    SYSTEM_INIT = "SYSTEM_INIT" # Used on bot startup for the initial snapshot

class IDatabase(ABC):
    @abstractmethod
    def initialize_schema(self):
        """Initializes the database schema (creates tables if they don't exist)."""
        pass

    @abstractmethod
    def insert_member_history(self, member: discord.Member, event_type: DiscordEvent):
        """
        Inserts a new snapshot into MemberHistory if there are any changes compared 
        to the most recent snapshot in the database.
        """
        pass

    @abstractmethod
    def get_member_last_instance(self, user_id: int, guild_id: int) -> Optional[dict]:
        """
        Retrieves the most recent (latest) historical snapshot for a given user and guild.
        Should return a dictionary representation of the member state, including roles, activities, etc.
        """
        pass

    @abstractmethod
    def get_member_instance_at(self, user_id: int, guild_id: int, target_time: datetime) -> Optional[dict]:
        """
        Retrieves the historical snapshot for a given user and guild that was active at exact `target_time`.
        Should return a dictionary representation of the member state at that time.
        """
        pass
        
    @abstractmethod
    def insert_user_history(self, user: discord.User, event_type: DiscordEvent):
        """
        Inserts a new snapshot into UserHistory if the global user attributes have changed.
        """
        pass
