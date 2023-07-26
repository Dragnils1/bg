import logging
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage

API_TOKEN = '6350199987:AAHr1VO0QLHfBakRtRVA4zhF8Dmml60AOQk'

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    """
    This handler will be called when user sends /start or /help commands.
    """
    await message.reply("Hi! I'm a bot. Send me a message and I'll provide some information about it.")


@dp.message_handler()
async def echo_message(message: types.Message):
    """
    This handler will be called when user sends any other text message.
    """
    # Get information about the message
    chat_id = message.chat.id
    user_id = message.from_user.id
    message_text = message.text

    # Prepare the response
    response = f"Message info:\nChat ID: {chat_id}\nUser ID: {user_id}\nMessage: {message_text}"

    # Send the response
    await bot.send_message(chat_id, response)


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
