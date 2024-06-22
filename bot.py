import asyncio
import logging
import shutil
import traceback

import yaml
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text

import db
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.types import ContentTypes, ParseMode

import time, os
from document import read_file
from tweepy import Twitterbot

chat_id = 'CHAT-ID'
# Configure logging
logging.basicConfig(level=logging.INFO)

with open('config.yaml') as f:
    config = yaml.safe_load(f)

storage = MemoryStorage()
telegram_bot = Bot(token=config["api_token"])
dp = Dispatcher(telegram_bot)

bot = Twitterbot(config["email"], config["password"])


class UserState(StatesGroup):
    users = State()


@dp.message_handler(commands=["start", "help"])
async def send_welcome(message: types.Message):
    try:
        bot.login()
        db.connect_to_db()
    except Exception as ex:
        print("Попробуйте еще раз:", ex)
    await message.reply(text="Добро пожаловать в бота! Загрузите список пользователей")


@dp.message_handler(content_types=ContentTypes.ANY)
async def document_message(message: types.Message, state):
    document = message.document
    if document:
        try:
            path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'users')
            shutil.rmtree(path)
        except Exception as er:
            print("Данные не очищены")
        try:
            db.clean_up_db()
        except Exception as er:
            print("Данные не очищены")
        random = time.time()
        await document.download(destination_file=f'users/{random}-{document.file_name}')
        await message.reply("Пользователи успешно добавлены")
        users = read_file(f'users/{random}-{document.file_name}')
        while os.path.isfile(f'users/{random}-{document.file_name}'):
            await get_info_about_users(users, message.chat.id)
            await asyncio.sleep(config["sleep_time"])
    else:
        bot.logout()
        db.disconnect_from_db()
        await state.finish()
        await message.reply("Напишите /start для начала работы.")


async def send_message(user, friend_name, friend_url, chat_id):
    try:
        await telegram_bot.send_message(chat_id=chat_id,
                                        text=f"Пользователь {user} подписался на {friend_name}: {friend_url}")
    except Exception as err:
        print(err)


async def handle_users(users_chunk_from_twitter, chat_id):
    for user in users_chunk_from_twitter:
        if db.user_not_in_db(user["id"]):
            db.write_new_users_to_db([user])
            db.write_new_friends_to_db(user["id"], user["friends_list"])
        else:
            db_user_friends = db.get_friends_from_db(user["id"])
            new_friends = get_new_friends(db_user_friends, user["friends_list"])
            db.write_new_friends_to_db(user["id"], new_friends)
            for new_friend in new_friends:
                await send_message(user["name"], new_friend["name"], new_friend["url"], chat_id)


async def get_info_about_users(users, chat_id):
    for i in range(0, len(users), 20):
        try:
            chunk = users[i:i + 20]
            await handle_users(bot.get_users(chunk), chat_id)
        except:
            traceback.print_exc()
            print('Что-то пошло не так, пропуск...')


def get_new_friends(friends_from_db, friends_from_twitter):
    new_friends = []
    for friend_from_twitter in friends_from_twitter:
        found = False
        for friend_from_db in friends_from_db:
            if friend_from_db[0] == friend_from_twitter["id"]:
                found = True
                break
        if not found:
            new_friends.append(friend_from_twitter)
    return new_friends


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
