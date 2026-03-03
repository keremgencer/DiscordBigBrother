import sqlite3
from flask import Flask, render_template, jsonify
import os
import shutil
import time
import threading

app = Flask(__name__)

# The database is located in the parent folder's src/ directory
# DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'src', 'database.db')
DB_PATH = "D:/database.db"

USE_DB_CACHING = True
LOCAL_DB_PATH = os.path.join(os.path.dirname(__file__), "local_cache.db")

# Track remote file state in memory
_last_remote_mtime = 0
_last_remote_size = 0

def sync_db_cache_loop():
    global _last_remote_mtime, _last_remote_size
    while True:
        if USE_DB_CACHING:
            try:
                current_mtime = os.path.getmtime(DB_PATH)
                current_size = os.path.getsize(DB_PATH)
                
                if current_mtime != _last_remote_mtime or current_size != _last_remote_size or not os.path.exists(LOCAL_DB_PATH):
                    print(f"[Cache Daemon] Remote DB changed! Syncing {current_size} bytes...")
                    shutil.copy2(DB_PATH, LOCAL_DB_PATH)
                    _last_remote_mtime = current_mtime
                    _last_remote_size = current_size
                    print("[Cache Daemon] Cache sync complete.")
            except Exception as e:
                print(f"[Cache Daemon] Sync error: {e}")
                
        time.sleep(60)

def get_db_connection():
    if USE_DB_CACHING and os.path.exists(LOCAL_DB_PATH):
        conn = sqlite3.connect(LOCAL_DB_PATH)
    else:
        conn = sqlite3.connect(DB_PATH)
        
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def dashboard():
    """Renders the main dashboard page."""
    return render_template('index.html')

@app.route('/user/<int:user_id>')
def user_profile(user_id):
    """Renders the detailed history page for a specific user."""
    return render_template('user.html', user_id=user_id)

@app.route('/api/users')
def api_get_users():
    """Returns a list of all distinct users tracked in the system along with their latest info."""
    conn = get_db_connection()
    cursor = conn.cursor()
    # Fetch the most recent UserHistory row for each distinct user
    cursor.execute("""
        SELECT uh.*, i.cdn_url as avatar_url, mh.raw_status
        FROM UserHistory uh
        INNER JOIN (
            SELECT user_id, MAX(timestamp) as max_time
            FROM UserHistory
            GROUP BY user_id
        ) latest ON uh.user_id = latest.user_id AND uh.timestamp = latest.max_time
        LEFT JOIN Images i ON uh.avatar_id = i.id
        LEFT JOIN (
            -- Subquery to get the absolute most recent status from MemberHistory
            SELECT user_id, raw_status
            FROM MemberHistory
            WHERE id IN (
                SELECT MAX(id) 
                FROM MemberHistory 
                GROUP BY user_id
            )
        ) mh ON uh.user_id = mh.user_id
        ORDER BY uh.name ASC
    """)
    rows = cursor.fetchall()
    conn.close()
    
    users = []
    for row in rows:
        user_dict = dict(row)
        # JavaScript loses precision on 64-bit Discord IDs. Treat them as strings.
        user_dict['user_id'] = str(user_dict['user_id'])
        users.append(user_dict)
        
    return jsonify(users)

@app.route('/api/stats')
def api_get_stats():
    """Returns basic counting statistics for the dashboard header."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(DISTINCT user_id) FROM UserHistory")
    total_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM MemberActivityHistory")
    total_activities = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM MemberVoiceStateHistory")
    total_voice_sessions = cursor.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        "tracked_users": total_users,
        "logged_activities": total_activities,
        "logged_voice_sessions": total_voice_sessions
    })

@app.route('/api/users/<int:user_id>/history')
def api_get_user_history(user_id):
    """Aggregates all chronological events (Profile updates, Activities, Voice, Roles) for a single user."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Fetch Latest Profile Data
    cursor.execute("""
        SELECT uh.*, i.cdn_url as avatar_url, b.cdn_url as banner_url
        FROM UserHistory uh
        LEFT JOIN Images i ON uh.avatar_id = i.id
        LEFT JOIN Images b ON uh.banner_id = b.id
        WHERE uh.user_id = ?
        ORDER BY uh.timestamp DESC LIMIT 1
    """, (user_id,))
    profile_row = cursor.fetchone()
    
    # 2. If no UserHistory exists, try to build a fallback profile from MemberHistory
    if not profile_row:
        cursor.execute("""
            SELECT m.user_id, m.nick as name, m.nick as global_name, 
                   i.cdn_url as avatar_url, b.cdn_url as banner_url,
                   1 as bot, NULL as premium_since, 0 as pending
            FROM MemberHistory m
            LEFT JOIN Images i ON m.guild_avatar_id = i.id
            LEFT JOIN Images b ON m.guild_banner_id = b.id
            WHERE m.user_id = ?
            ORDER BY m.timestamp DESC LIMIT 1
        """, (user_id,))
        profile_row = cursor.fetchone()
        
    if not profile_row:
        # Ultimate fallback: they literally have no data in the system
        return jsonify({"error": "User not found"}), 404
        
    profile = dict(profile_row)
    
    # 2. Fetch Guild Member Timeline (Snapshots)
    cursor.execute("""
        SELECT m.*, g.name as guild_name 
        FROM MemberHistory m
        LEFT JOIN GuildList g ON m.guild_id = g.id
        WHERE m.user_id = ?
        ORDER BY m.timestamp DESC
    """, (user_id,))
    snapshots = [dict(r) for r in cursor.fetchall()]
    
    # 3. Fetch Activities
    cursor.execute("""
        SELECT * FROM MemberActivityHistory WHERE user_id = ?
        ORDER BY started_at DESC
    """, (user_id,))
    activities = [dict(r) for r in cursor.fetchall()]
    
    # 4. Fetch Voice Sessions
    cursor.execute("""
        SELECT v.*, g.name as guild_name 
        FROM MemberVoiceStateHistory v
        LEFT JOIN GuildList g ON v.guild_id = g.id
        WHERE v.user_id = ?
        ORDER BY v.started_at DESC
    """, (user_id,))
    voice_sessions = [dict(r) for r in cursor.fetchall()]
    
    # 5. Fetch Role Assignments
    cursor.execute("""
        SELECT r.*, g.name as guild_name
        FROM MemberRolesHistory r
        LEFT JOIN GuildList g ON r.guild_id = g.id
        WHERE r.user_id = ?
        ORDER BY r.started_at DESC
    """, (user_id,))
    roles = [dict(r) for r in cursor.fetchall()]

    conn.close()
    
    return jsonify({
        "profile": profile,
        "snapshots": snapshots,
        "activities": activities,
        "voice_sessions": voice_sessions,
        "roles": roles
    })

if __name__ == '__main__':
    # Start the background sync daemon before launching Flask
    sync_thread = threading.Thread(target=sync_db_cache_loop, daemon=True)
    sync_thread.start()
    
    app.run(debug=True, port=3000, use_reloader=False)  # Disabled use_reloader to avoid double-spawning threads
