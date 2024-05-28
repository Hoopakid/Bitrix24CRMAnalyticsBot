import os

from aiogram import Bot
import asyncio
from aiogram import Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message
from aiogram.filters import CommandStart
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ.get('BOT_TOKEN')

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

MBI_CHAT_ID = os.environ.get('MBI_CHAT_ID')
ISAYEV_CHAT_ID = os.environ.get('ISAYEV_CHAT_ID')
SHER_CHAT_ID = os.environ.get('SHER_CHAT_ID')
HASAN_CHAT_ID = os.environ.get('HASAN_CHAT_ID')

all_chat_ids = [int(MBI_CHAT_ID), int(HASAN_CHAT_ID), int(SHER_CHAT_ID), int(ISAYEV_CHAT_ID)]


@dp.message(CommandStart())
async def start(message: Message):
    message_chat_id = message.from_user.id
    first_name = message.from_user.first_name if message.from_user.first_name is not None else ''
    last_name = message.from_user.last_name if message.from_user.last_name is not None else ''
    if message_chat_id in all_chat_ids:
        await message.answer(f"Assalomu Alaykum {first_name} {last_name} 😊, ushbu bot soat 19:10 da xizmat ko'rsatadi")
    else:
        await message.answer('Ushbu bot faqat belgilangan foydalanuvchilar uchun ishlaydi holos 🤝')


async def main():
    await dp.start_polling(bot, skip_update=True)


if __name__ == '__main__':
    asyncio.run(main())
