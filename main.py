from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, CommentEvent, JoinEvent,DisconnectEvent, LikeEvent , ShareEvent
import requests
import betterproto
import json
import time
from uuid import uuid4
from datetime import datetime, timezone
import clickhouse_connect
from httpx import Proxy
from users.user import insert_user, build_user

client_clickhouse = clickhouse_connect.get_client(host='localhost', username='tiktoklive', password='clickhouseadmin', database='tiktoklive', port=18123)


# Create the client
client: TikTokLiveClient = TikTokLiveClient(unique_id="@christinanguyen9999", web_proxy=Proxy("http://192.168.1.238:4001", auth=("admin", "1234!@#$")))

def create_payload(event_type, comment, user_id, live_id, event, gift=int(0)):
    now = datetime.now(tz=timezone.utc)
    base_ts = now.timestamp()
    payload = {
        "live_id": str(live_id),
        "event_time": now.isoformat(timespec="milliseconds"),  # ClickHouse đọc được ISO
        "event_id": str(uuid4()),
        "user_id": user_id,
        "user_name": event.user.nickname ,
        "event_type": event_type,
        "comment": comment,
        "gift_value": gift,
        "payload_json": "",
    }
    return payload
# Listen to an event with a decorator!
@client.on(ConnectEvent)
async def on_connect(event: ConnectEvent):

    print(f"Connected to @{event.unique_id} (Room ID: {client.room_id}")
@client.on(DisconnectEvent )
async def on_disconnected(event: DisconnectEvent ):
    print(f"Disconnected to @{event} (Room ID: {client.room_id}")
    # client.run()

@client.on(LikeEvent )
async def on_digg(event: LikeEvent ):
    payload = create_payload("LIKE", "", event.user.unique_id, event.base_message.room_id, event)

    client_clickhouse.insert(table="events_raw", data=[list(payload.values())], column_names=list(payload.keys()),database="tiktoklive")
    print(f"LIKE: {event.user.unique_id}:{event.user.nickname}")
@client.on(ShareEvent)
async def on_share(event: ShareEvent ):

    payload = create_payload("SHARE", "", event.user.unique_id, event.base_message.room_id, event)

    client_clickhouse.insert(table="events_raw", data=[list(payload.values())], column_names=list(payload.keys()),database="tiktoklive")
    print(f"SHARE: {event.user.unique_id}:{event.user.nickname} shared live")
#
@client.on(JoinEvent)
async def on_join(event: JoinEvent):
    payload = create_payload("JOINED", "", event.user.unique_id, event.base_message.room_id, event)
    # print(event.user.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=False))
    insert_user(client_clickhouse, build_user(event.user.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=False)))
    client_clickhouse.insert(table="events_raw", data=[list(payload.values())], column_names=list(payload.keys()),database="tiktoklive")
    print(f"JOIN: @{event.user.nickname} joined ")

# Or, add it manually via "client.add_listener()"
async def on_comment(event: CommentEvent) -> None:
    payload = create_payload("COMMENT", event.comment, event.user.unique_id, event.base_message.room_id, event)
    insert_user(client_clickhouse,
                build_user(event.user.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=False)))
    client_clickhouse.insert(table="events_raw", data=[list(payload.values())], column_names=list(payload.keys()),database="tiktoklive")
    print(f"COMMENT {event.user.unique_id}:{event.user.nickname} is commented: {event.comment}")


client.add_listener(CommentEvent, on_comment)

if __name__ == '__main__':
    # Run the client and block the main thread
    # await client.start() to run non-blocking
    while True:
        try:
            client.run()
        finally:
            client.run()