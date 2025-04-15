from aiogram import Router
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
# from fluentogram import TranslatorRunner

from filters.filters import IsAdmin


admin_router = Router()


@admin_router.message(Command(commands='getlog'), IsAdmin())
async def admin_get_log_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer_document(FSInputFile('loger/logs.log'))
