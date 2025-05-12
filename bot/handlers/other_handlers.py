from aiogram import Router
from aiogram.types import Message
# from fluentogram import TranslatorRunner


other_router = Router()


@other_router.message()
async def send_empty_message(message: Message):
    await message.answer('Пожалуйста, выберите действие из меню.')
