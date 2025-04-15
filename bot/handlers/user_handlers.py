import logging
from aiogram import Router
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command, CommandStart
# from fluentogram import TranslatorRunner


logger = logging.getLogger(__name__)

user_router = Router()


@user_router.message(CommandStart())
async def process_start_command(message: Message, state: FSMContext):
    await state.clear()
    logger.info(f'{message.chat.username} ({message.chat.id}) - start bot')
    await message.answer(
        text='Добрый день, меня зовут VK_POSTER!\n\n'
        'Я помогу вам дублировать новости '
        'из вашей группы вк в телеграмм группу.\n\n'
        'Напишите мне /autopost '
        'для начала настройки автопоста.\n\n'
        'Триал версия состовляет 1 месяц, '
        'для просмотра тарифов напишите /tariffs.\n\n'
        'Перед началом работы узнайте id группы вк '
        '(убедитесь что она открыта), '
        'а так же id группы тг.\n'
        'Можете переслать сообщание из группы тг мне, а'
        'я подскажу (копировать id группы нужно без минуса!).'
        )


@user_router.message(Command(commands='help'))
async def process_help_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        text='Напишите мне /autopost '
        'для начала настройки автопоста.\n\n'
        'Триал версия состовляет 1 месяц, '
        'для просмотра тарифов напишите /tariffs.\n\n'
        'Перед началом работы узнайте id группы вк '
        '(убедитесь что она открыта), '
        'а так же id группы тг.\n'
        'Можете переслать сообщание из группы тг мне, а '
        'я подскажу (копировать id группы нужно без минуса!).'
        '\n\nВ дальнейшем наш бот будет развиваться, '
        'будем рады обратной связи.\n\n'
        'Напишите мне /feedback и оставте обратную связь.'
        )


@user_router.message(Command(commands='tariffs'))
async def process_tariffs_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        text='Мы пока работаем над этим, ожидайте.'
        )


@user_router.message(Command(commands='feedback'))
async def process_feedback_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        text='Мы пока работаем над этим, ожидайте.'
        )


@user_router.message(Command(commands='support'))
async def process_support_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        text='Мы пока работаем над этим, ожидайте.'
        )


@user_router.message(Command(commands='info'))
async def process_info_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        text='Добрый день, меня зовут VK_POSTER!\n\n'
        'Я помогу вам дублировать новости '
        'из вашей группы вк в телеграмм группу.\n\n'
        'Напишите мне /autopost '
        'для начала настройки автопоста.\n\n'
        'Триал версия состовляет 1 месяц, '
        'для просмотра тарифов напишите /tariffs.\n\n'
        'Перед началом работы узнайте id группы вк '
        '(убедитесь что она открыта), '
        'а так же id группы тг.\n'
        'Можете переслать сообщание из группы тг мне, а'
        'я подскажу (копировать id группы нужно без минуса!).'
        )
