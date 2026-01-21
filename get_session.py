from telethon.sync import TelegramClient
from telethon.sessions import StringSession

API_ID = 34456629        # твой TG_API_ID
API_HASH = "b54959eed146203ec150e86cbe990bdd"  # твой TG_API_HASH

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    print("\n=== TG_SESSION_STRING ===\n")
    print(client.session.save())
    print("\n=========================\n")
