from aiogram import Router
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
# from fluentogram import TranslatorRunner
from bot.services.delay_service.publisher import get_promocode_list_publisher

from bot.filters.filters import IsAdmin


admin_router = Router()


@admin_router.message(Command(commands='getlog'), IsAdmin())
async def admin_get_log_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer_document(FSInputFile('loger/logs.log'))


@admin_router.message(Command(commands='getpromocodelist'), IsAdmin())
async def admin_get_promocode_list(
                                    message: Message,
                                    state: FSMContext,
                                    js,
                                    subject_admin_promocode_publisher):
    await state.clear()
    await get_promocode_list_publisher(
                                        js=js,
                                        chat_id=message.chat.id,
                                        subject=subject_admin_promocode_publisher
    )

@admin_router.message(Command(commands='getdklist'), IsAdmin())
async def admin_get_promocode_list(
                                    message: Message,
                                    state: FSMContext,
                                    js,
                                    subject_admin_dk_publisher):
    await state.clear()
    await get_promocode_list_publisher(
                                        js=js,
                                        chat_id=message.chat.id,
                                        subject=subject_admin_dk_publisher
    )


@admin_router.message(Command(commands='news'), IsAdmin())
async def admin_news_command(message: Message):
    await message.answer(text='NEWS')
