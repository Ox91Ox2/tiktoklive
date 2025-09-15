import os, json, time
from datetime import datetime, timezone
def to_uint(s):
    try: return int(s)
    except: return 0

def build_user(profile):
    row = [
        str(profile.get("id", "")),
        profile.get("sec_uid", ""),
        profile.get("username", ""),
        profile.get("nick_name", ""),

        (profile.get("avatar_thumb") or {}).get("m_uri", ""),
        (profile.get("avatar_thumb") or {}).get("m_urls", []),

        int((profile.get("follow_info") or {}).get("following_count")or 0),
        int((profile.get("follow_info") or {}).get("follower_count") or 0),

        json.dumps(profile.get("user_honor", {}), ensure_ascii=False),
        json.dumps(profile.get("user_attr", {}), ensure_ascii=False),
        json.dumps(profile, ensure_ascii=False),

        int(time.time()),  # ver
        datetime.now(tz=timezone.utc)  # updated_at
    ]
    return row

def insert_user(client, row):

    client.insert(
        "dim_users",
        [row],
        column_names=[
            "user_id", "sec_uid", "username", "nick_name",
            "avatar_uri", "avatar_urls",
            "following_count", "follower_count",
            "user_honor", "user_attr", "raw",
            "ver", "updated_at"
        ]
    )