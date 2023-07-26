import asyncio
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Command
from aiogram.dispatcher.filters.state import StatesGroup, State
import asyncpg
import os

from aiogram.utils import exceptions



DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="postgres"
DB_USER="postgres"
DB_PASS="12345678"
BOT_TOKEN="6350199987:AAHr1VO0QLHfBakRtRVA4zhF8Dmml60AOQk"
# Set up the bot and dispatcher

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Connect to the PostgreSQL database
async def create_pool():
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

loop = asyncio.get_event_loop()
pool = loop.run_until_complete(create_pool())

# Define the passwords table schema
CREATE_TABLE_PASSWORDS = """
    CREATE TABLE IF NOT EXISTS passwords (
        id SERIAL PRIMARY KEY,
        password TEXT,
        group_name TEXT
    )
"""

# Define the logged_in_users table schema
CREATE_TABLE_LOGGED_IN_USERS = """
    CREATE TABLE IF NOT EXISTS logged_in_users (
        chat_id INTEGER PRIMARY KEY,
        group_name TEXT
    )
"""

# Create the tables if they don't exist
async def create_tables():
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE_PASSWORDS)
        await conn.execute(CREATE_TABLE_LOGGED_IN_USERS)

loop.run_until_complete(create_tables())

# Define the states for password input
class Login(StatesGroup):
    waiting_for_password = State()

# Handler for the /start command
@dp.message_handler(commands=['start'])
async def start(message: types.Message, state: FSMContext):
    chat_id = message.chat.id

    async with pool.acquire() as conn:
        # Check if the user is already authorized
        user_result = await conn.fetchrow("SELECT * FROM logged_in_users WHERE chat_id = $1", chat_id)

        if user_result:
            group_name = user_result['group_name']
            await message.reply(f"Вы уже авторизованы и подключены к группе '{group_name}'.")
        else:
            await message.reply("Добро пожаловать! Используйте команду /login для авторизации с паролем.")

# Handler for the /login command
@dp.message_handler(commands=['login'])
async def login(message: types.Message):
    await message.reply("Пожалуйста, введите ваш пароль:")
    await Login.waiting_for_password.set()

    chat_id = message.chat.id
    async with pool.acquire() as conn:
        # Remove the user from the old group if they are already authorized
        user_result = await conn.fetchrow("SELECT * FROM logged_in_users WHERE chat_id = $1", chat_id)
        if user_result:
            await conn.execute("DELETE FROM logged_in_users WHERE chat_id = $1", chat_id)

# Handler for the user's password input
@dp.message_handler(state=Login.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    password = message.text

    async with pool.acquire() as conn:
        # Check if the password exists in the database
        result = await conn.fetchrow("SELECT * FROM passwords WHERE password = $1", password)

        if result:
            group_name = result['group_name']

            chat_id = message.chat.id
            # Check if the user is already listed as authorized
            user_result = await conn.fetchrow("SELECT * FROM logged_in_users WHERE chat_id = $1", chat_id)

            if user_result:
                # Remove the user from the old group
                old_group_name = user_result['group_name']
                await conn.execute("DELETE FROM logged_in_users WHERE chat_id = $1", chat_id)
                await message.reply(f"Вы были удалены из предыдущей группы '{old_group_name}'.")

            # Save the user in the database as authorized
            await conn.execute("INSERT INTO logged_in_users (chat_id, group_name) VALUES ($1, $2)", chat_id, group_name)
            await message.reply(f"Авторизация успешна! Вы подключены к группе '{group_name}'.")
        else:
            await message.reply("Неверный пароль. Пожалуйста, попробуйте еще раз команду /login.")

    await state.finish()

# Handler for the /status command
@dp.message_handler(commands=['status'])
async def check_status(message: types.Message):
    chat_id = message.chat.id

    async with pool.acquire() as conn:
        # Check if the user is listed as authorized
        user_result = await conn.fetchrow("SELECT * FROM logged_in_users WHERE chat_id = $1", chat_id)

        if user_result:
            group_name = user_result['group_name']
            await message.reply(f"Вы авторизованы и подключены к группе '{group_name}'.")
        else:
            await message.reply("Вы не авторизованы.")

# Handler for the /generate command
@dp.message_handler(commands=['generate'])
async def generate_password(message: types.Message):
    # Check if the message is from your account (chat_id 866435799)
    if message.chat.id != 866435799:
        await message.reply("Извините, у вас нет прав для использования этой команды.")
        return

    # Extract the password and group name from the command
    command_parts = message.get_args().split()

    if len(command_parts) >= 2:
        password = command_parts[0]
        group_name = " ".join(command_parts[1:])

        async with pool.acquire() as conn:
            # Check if the password already exists
            password_result = await conn.fetchrow("SELECT * FROM passwords WHERE password = $1", password)
            if password_result:
                await message.reply("Пароль уже существует. Пожалуйста, сгенерируйте уникальный пароль.")
                return

            # Check if the group name already exists
            group_result = await conn.fetchrow("SELECT * FROM passwords WHERE group_name = $1", group_name)
            if group_result:
                await message.reply("Название группы уже существует. Пожалуйста, выберите уникальное название группы.")
                return

            # Save the password and group name in the 'passwords' table
            await conn.execute("INSERT INTO passwords (password, group_name) VALUES ($1, $2)", password, group_name)

            await message.reply("Пароль и название группы успешно сохранены!")
    else:
        await message.reply("Пожалуйста, укажите и пароль, и название группы.")

# Handler for the /help command
@dp.message_handler(commands=['help'])
async def help_command(message: types.Message):
    help_text = """
    Бот-авторизация и управление группами.
    
    Список доступных команд:
    /start - Начало работы
    /login - Авторизоваться с паролем
    /status - Проверить статус авторизации
    /generate - Сгенерировать пароль и название группы
    /help - Показать справку
    """
    await message.reply(help_text)

# Function to send special messages to all users
async def send_special_message():
    async with pool.acquire() as conn:
        users = await conn.fetch("SELECT * FROM logged_in_users")

        for user in users:
            chat_id = user['chat_id']
            group_name = user['group_name']
            try:
                await bot.send_message(chat_id, f"Новый тендер для компании {group_name}")
            except exceptions.BotBlocked:
                print(f"Ошибка: Пользователь с ID {chat_id} заблокировал бота.")
            except exceptions.ChatNotFound:
                print(f"Ошибка: Чат с пользователем {chat_id} не найден.")
            except exceptions.RetryAfter as e:
                print(f"Ошибка: Перегрузка API. Повторная отправка через {e.timeout} секунд.")
                await asyncio.sleep(e.timeout)
                await bot.send_message(chat_id, f"Новый тендер для компании {group_name}")
            except exceptions.UserDeactivated:
                print(f"Ошибка: Пользователь с ID {chat_id} деактивировал свою учетную запись.")
            except exceptions.TelegramAPIError:
                print(f"Ошибка: Произошла ошибка Telegram API при отправке сообщения пользователю {chat_id}.")

# Schedule the special message sending task every 10 seconds
async def schedule_special_message():
    while True:
        await send_special_message()
        await asyncio.sleep(10)

# Start the special message scheduling task
async def on_startup(dp):
    asyncio.create_task(schedule_special_message())

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
