import logging

from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext

from states.states import NatsFillForm
# from fluentogram import TranslatorRunner


logger = logging.getLogger(__name__)

bot_router = Router()

# Создаем "базу данных" пользователей
user_dict: dict = {}


@bot_router.message(Command(commands='cancel'), StateFilter(NatsFillForm))
async def process_cancel_command_state(message: Message, state: FSMContext):
    await message.answer(
        text='Вы вышли из настройки автопоста.\n\n'
             'Чтобы снова перейти к настройке втопоста - '
             'отправьте команду /autopost.'
    )
    # Сбрасываем состояние и очищаем данные, полученные внутри состояний
    await state.set_state()


@bot_router.message(Command(commands='cancel'))
async def process_cancel_command(message: Message, state: FSMContext):
    await message.answer(
        text='Отменять нечего. Вы не настраиваете автопост.\n\n'
        'Чтобы перейти к настройке автопоста - '
        'отправьте команду /autopost.'
        )


@bot_router.message(Command(commands='autopost'))
async def process_autopost_command(message: Message, state: FSMContext):
    logger.info(f'{message.chat.username} ({message.chat.id}) '
                '- start fill autopost')
    await message.answer(text='Пожалуйста, введите id группы vk.')
    await state.set_state(NatsFillForm.fill_id_vk_group)


@bot_router.message(StateFilter(NatsFillForm.fill_id_vk_group),
                    lambda x: x.text.isdigit())
async def process_autopost_id_vk_group(message: Message, state: FSMContext):
    await state.update_data(id_vk_group=int(message.text))
    await message.answer(
        text='Спасибо!\n\nА теперь введите id группы tg.'
    )
    await state.set_state(NatsFillForm.fill_id_tg_group)


@bot_router.message(StateFilter(NatsFillForm.fill_id_vk_group))
async def warning_not_id_vk_group(message: Message):
    await message.answer(
        text='id группы vk должен быть числом.'
    )


@bot_router.message(StateFilter(NatsFillForm.fill_id_tg_group),
                    lambda x: x.text.isdigit())
async def process_autopost_id_tg_group(message: Message, state: FSMContext):
    await state.update_data(id_tg_group=int(message.text))

    user_dict[message.from_user.id] = await state.get_data()

    await state.clear()
    print(user_dict)

    await message.answer(
        text='Ваш бот запущен, наслаждайтесь!'
    )


@bot_router.message(StateFilter(NatsFillForm.fill_id_tg_group))
async def warning_not_id_tg_group(message: Message):
    await message.answer(
        text='id группы tg должен быть числом.'
    )


# в дальнейшем перенести в userhandlers
@bot_router.message(Command(commands='showautopost'))
async def process_showautopost_command(message: Message):
    if message.from_user.id in user_dict:
        await message.answer(
            text=f'id vk - {user_dict[message.from_user.id]['id_vk_group']}'
            f'\nid tg - {user_dict[message.from_user.id]['id_tg_group']}'
        )
    else:
        await message.answer(
            text='Вы еще не настраивали бота,'
            'для настройки напишите мне /autopost'
        )
