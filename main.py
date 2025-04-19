from telethon.sync import TelegramClient
from telethon.tl.types import PeerChannel
from dotenv import load_dotenv
import asyncio
import os
import sqlite3
import logging

logging.basicConfig(
    level=logging.INFO,
    filename="main.log",
    filemode="a",
    format="%(asctime)s %(levelname)s %(message)s"
)

load_dotenv()

api_id=os.getenv('API_ID')
api_hash=os.getenv('API_HASH')
channel_id=os.getenv('CHANNEL_ID')

# Задержка в секундах перед итерациями обработки
sleep_time = os.getenv('SLEEP_TIME')
# Кол-во последних сообшений для обработки
message_limit = os.getenv('MESSAGE_LIMIT')

if not sleep_time:
    sleep_time = 15

sleep_time=int(sleep_time)

if not message_limit:
    message_limit = 3

message_limit=int(message_limit)

if None in [api_id, api_hash, channel_id]:
    raise Exception(f"Для запуска скрипта задайте переменные окружения API_ID, API_HASH, CHANNEL_ID")

client = TelegramClient(
    session='image_finder',
    api_id=int(api_id),
    api_hash=api_hash
)

img_dir = "img"
os.makedirs(img_dir, exist_ok=True)

conn = sqlite3.connect("processed.db")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS processed_messages (
        id INTEGER PRIMARY KEY
    )
""")
conn.commit()

def is_processed(msg_id: int) -> bool:
    cursor.execute("SELECT 1 FROM processed_messages WHERE id = ?", (msg_id,))
    return cursor.fetchone() is not None


async def save_photo(message):
    path = await client.download_media(message, file=os.path.join(img_dir, f"{message.id}.jpg"))
    logging.info(f"Новая фотография - {path}")
    cursor.execute("INSERT INTO processed_messages (id) VALUES (?)", (message.id,))
    conn.commit()

async def main():
    logging.info(f"Начало работы")
    await client.start()


    while(True):
        entity = await client.get_entity(PeerChannel(int(channel_id)))

        messages = await client.get_messages(entity, limit=3)
        processed_grouped_ids = set()

        for msg in messages:
            if not msg.media or is_processed(msg.id):
                continue

            if msg.grouped_id:
                if msg.grouped_id in processed_grouped_ids:
                    continue

                album_msgs = [m for m in messages if m.grouped_id == msg.grouped_id]
                for m in album_msgs:
                    if m.photo and not is_processed(m.id):
                        await save_photo(m)

                processed_grouped_ids.add(msg.grouped_id)

            elif msg.photo:
                await save_photo(msg)

        await asyncio.sleep(sleep_time)

try:
    asyncio.run(main())
except Exception as ex:
    logging.error(f"Ошибка при работе программы - {ex}", exc_info=True)

conn.close()