import asyncio
import psycopg2
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Command
from aiogram.dispatcher.filters.state import StatesGroup, State
import os
from aiogram.utils import exceptions
import psycopg2
from datetime import datetime
import sys
import io

import time

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "postgres"
BOT_TOKEN = "6350199987:AAHr1VO0QLHfBakRtRVA4zhF8Dmml60AOQk"  # Replace with your actual bot token

# Set up the bot and dispatcher

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Connect to the PostgreSQL database
def create_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def execute_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def create_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()

def update_sended_today():
    try:
        # Replace the connection parameters with your PostgreSQL database details
        connection = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres"
        )

        cursor = connection.cursor()

        # Update the sended_today column to false for all records
        update_query = "UPDATE filters SET sended_today = false;"
        cursor.execute(update_query)

        connection.commit()
        cursor.close()
        connection.close()

        print("Successfully updated sended_today to false for all records.")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL or executing the update query:", error)






# Define the states for password input
class Login(StatesGroup):
    waiting_for_password = State()

# Handler for the /start command
@dp.message_handler(commands=['start'])
async def start(message: types.Message, state: FSMContext):
    chat_id = message.chat.id

    conn = create_connection()
    with conn:
        with conn.cursor() as cur:
            # Check if the user is already authorized
            cur.execute(f"SELECT * FROM filters WHERE telegram_chat_ids LIKE '%{chat_id}%';")
            user_result = cur.fetchone()

            if user_result:
                group_name = user_result[2]
                await message.reply(f"Вы уже авторизованы и подключены к группе '{group_name}'.")
            else:
                await message.reply("Добро пожаловать! Используйте команду /login для авторизации с паролем.")

# Handler for the /login command
@dp.message_handler(commands=['login'])
async def login(message: types.Message):
    await message.reply("Если вы состояли в группе ранее вы были из нее удалены!\nПожалуйста, введите пароль от новой группы:")
    await Login.waiting_for_password.set()

    chat_id = message.chat.id
    conn = create_connection()
    with conn:
        with conn.cursor() as cur:
            # Remove the user from the old group if they are already authorized
            cur.execute(f"SELECT * FROM filters WHERE telegram_chat_ids LIKE '%{chat_id}%';")
            user_result = cur.fetchone()
            if user_result:
                query=f'''UPDATE filters
SET telegram_chat_ids = REPLACE(telegram_chat_ids, '{chat_id}', '')
WHERE telegram_chat_ids LIKE '%{chat_id}%';
UPDATE filters
SET telegram_chat_ids = TRIM(',' FROM telegram_chat_ids)
WHERE telegram_chat_ids LIKE ',%' OR telegram_chat_ids LIKE '%,';
UPDATE filters
SET telegram_chat_ids = REPLACE(telegram_chat_ids, ',,', ',')'''
                cur.execute(query)

# Handler for the user's password input
@dp.message_handler(state=Login.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    password = message.text

    conn = create_connection()
    with conn:
        with conn.cursor() as cur:
            # Check if the password exists in the database
            cur.execute(f"SELECT * FROM filters WHERE password='{password}';")
            result = cur.fetchone()

            if result:
                group_name = result[2]

                chat_id = message.chat.id
                # Check if the user is already listed as authorized
                cur.execute(f"SELECT * FROM filters WHERE telegram_chat_ids LIKE '%{chat_id}%';")
                # user_result = cur.fetchone()

                # if user_result:
                #     # Remove the user from the old group
                #     old_group_name = user_result[1]
                #     cur.execute("DELETE FROM logged_in_users WHERE chat_id = %s", (chat_id,))
                #     await message.reply(f"Вы были удалены из предыдущей группы '{old_group_name}'.")

                # Save the user in the database as authorized
                cur.execute(f"UPDATE filters SET telegram_chat_ids = CONCAT(telegram_chat_ids, ',{chat_id}') WHERE password='{password}';")
                await message.reply(f"Авторизация успешна! Вы подключены к группе '{group_name}'.")
            else:
                await message.reply("Неверный пароль. Пожалуйста, попробуйте еще раз команду /login.")

    await state.finish()

# Handler for the /status command
@dp.message_handler(commands=['status'])
async def check_status(message: types.Message):
    chat_id = message.chat.id

    conn = create_connection()
    with conn:
        with conn.cursor() as cur:
            # Check if the user is listed as authorized
            cur.execute(f"SELECT * FROM filters WHERE telegram_chat_ids LIKE '%{chat_id}%';")
            user_result = cur.fetchone()

            if user_result:
                group_name = user_result[2]
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

        conn = create_connection()
        with conn:
            with conn.cursor() as cur:
                # Check if the password already exists
                cur.execute("SELECT * FROM passwords WHERE password = %s", (password,))
                password_result = cur.fetchone()
                if password_result:
                    await message.reply("Пароль уже существует. Пожалуйста, сгенерируйте уникальный пароль.")
                    return

                # Check if the group name already exists
                cur.execute("SELECT * FROM passwords WHERE group_name = %s", (group_name,))
                group_result = cur.fetchone()
                if group_result:
                    await message.reply("Название группы уже существует. Пожалуйста, выберите уникальное название группы.")
                    return

                # Save the password and group name in the 'passwords' table
                cur.execute("INSERT INTO passwords (password, group_name) VALUES (%s, %s)", (password, group_name))

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
# async def send_special_message():
#     conn = create_connection()
#     with conn:
#         with conn.cursor() as cur:
#             cur.execute("SELECT * FROM logged_in_users")
#             users = cur.fetchall()

#             for user in users:
#                 chat_id = user[0]
#                 group_name = user[1]
#                 try:
#                     await bot.send_message(chat_id, f"Новый тендер для компании {group_name}")
#                 except exceptions.BotBlocked:
#                     print(f"Ошибка: Пользователь с ID {chat_id} заблокировал бота.")
#                 except exceptions.ChatNotFound:
#                     print(f"Ошибка: Чат с пользователем {chat_id} не найден.")
#                 except exceptions.RetryAfter as e:
#                     print(f"Ошибка: Перегрузка API. Повторная отправка через {e.timeout} секунд.")
#                     await asyncio.sleep(e.timeout)
#                     await bot.send_message(chat_id, f"Новый тендер для компании {group_name}")
#                 except exceptions.UserDeactivated:
#                     print(f"Ошибка: Пользователь с ID {chat_id} деактивировал свою учетную запись.")
#                 except exceptions.TelegramAPIError:
#                     print(f"Ошибка: Произошла ошибка Telegram API при отправке сообщения пользователю {chat_id}.")
async def send_special_message():
    try:
        # Подключение к базе данных
        connection = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres"
        )

        # Выбор всех записей из таблицы filters
        select_filters_query = "SELECT * FROM filters"
        filters_data = execute_query(connection, select_filters_query)

        # Получаем текущее время
        current_time = datetime.now().strftime("%H:%M")

        for filter_data in filters_data:
            # Проверяем наличие айди чатов
            telegram_chat_ids = filter_data[8]  # Индекс 7 соответствует telegram_chat_ids
            if not telegram_chat_ids:
                continue

            if filter_data[9]:
                print("уже отправлено")
                continue

            # Проверяем поле "во сколько времени отправлять"
            send_time = filter_data[7]  # Индекс 6 соответствует send_time
            if send_time:
                send_time_obj = datetime.strptime(send_time, "%H:%M")
                current_time_obj = datetime.strptime(current_time, "%H:%M")
                if current_time_obj < send_time_obj:
                    continue

            # Разбиваем строку с KTRU на список
            ktru_list = filter_data[3].split(",")  # Индекс 3 соответствует KTRU
            # Преобразуем start_price и end_price в целочисленные значения
            start_price = int(filter_data[5])  # Индекс 4 соответствует start_price
            end_price = int(filter_data[6])  # Индекс 5 соответствует end_price
            results = []
            # Генерируем запросы и выполняем их
            for ktru in ktru_list:
                ktru = ktru.strip()
                kato = filter_data[4]  # Индекс 3 соответствует KATO
                if kato:
                    query = f"SELECT * FROM tenders WHERE kato='{kato}' AND ktru_code='{ktru}' AND CAST(planned_amount as float)>={start_price} AND CAST(planned_amount as float)<={end_price}"
                else:
                    query = f"SELECT * FROM tenders WHERE ktru_code='{ktru}' AND CAST(planned_amount as float)>={start_price} AND CAST(planned_amount as float)<={end_price}"

                print(query)
                results.append(execute_query(connection, query))

            chat_ids = filter_data[8].split(",")  # Индекс 8 соответствует chat_id
            for chat_id in chat_ids:
                if chat_id == '':
                    continue
                for tenders in results:
                    if tenders:
                        pass
                    else:
                        continue
                    for result in tenders:
                        if result:
                            if chat_id in result[30]:
                                continue
                            try:
                                link = result[7]
                                message_text = f"""
🚀 Новый Лот!
Ссылка: {result[7]}
Наименование лота: {result[0]}
Дополнительная характеристика: {result[16]}
Заказчик: {result[1]}
БИН Заказчика: {result[11]}
Сумма: {result[23]}
КТРУ: {result[13]}
ЕИ: {result[19]}
Начало приема заявок: {result[9]}
Окончание приема заявок: {result[10]}
Статус: {result[6]}
"""
                                keyboard = types.InlineKeyboardMarkup()
                                button = types.InlineKeyboardButton(text="Открыть лот", url=link)
                                keyboard.add(button)
                                await bot.send_message(chat_id=chat_id, text=message_text, reply_markup=keyboard)
                                query = f"UPDATE tenders SET sended_to = CONCAT(sended_to, ',{chat_id}') WHERE lot_number='{result[8]}';"
                                print(query)
                                create_query(connection, query)
                            except exceptions.BotBlocked:
                                print(f"Ошибка: Пользователь с ID {chat_id} заблокировал бота.")
                            except exceptions.ChatNotFound:
                                print(f"Ошибка: Чат с пользователем {chat_id} не найден.")
                            except exceptions.RetryAfter as e:
                                print(f"Ошибка: Перегрузка API. Повторная отправка через {e.timeout} секунд.")
                                await asyncio.sleep(e.timeout)
                                await bot.send_message(chat_id=chat_id, text=f"TENDER!\n{result}")
                            except exceptions.UserDeactivated:
                                print(f"Ошибка: Пользователь с ID {chat_id} деактивировал свою учетную запись.")
                            except exceptions.TelegramAPIError:
                                print(f"Ошибка: Произошла ошибка Telegram API при отправке сообщения пользователю {chat_id}.")
                        else: continue

                if send_time:
                    query_update = f"UPDATE filters SET sended_today = true WHERE id = {filter_data[0]};"
                    print(query_update)
                    create_query(connection, query_update)

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL:", error)

    finally:
        if connection:
            connection.close()

# Schedule the special message sending task every 10 seconds
async def schedule_special_message():
    while True:
        await send_special_message()
        await asyncio.sleep(1)

# Start the special message scheduling task
async def on_startup(dp):
    asyncio.create_task(schedule_special_message())

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=False, on_startup=on_startup)

