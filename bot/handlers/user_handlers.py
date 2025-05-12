import logging

from aiogram import Router
from aiogram.types import Message, ChatMemberUpdated
from aiogram.fsm.context import FSMContext
from aiogram.filters import (
                            Command,
                            CommandStart,
                            StateFilter,
                            ChatMemberUpdatedFilter,
                            KICKED)

from bot.states.states import PromocodeFillForm, GetDKInfoFillForm
# from fluentogram import TranslatorRunner

from bot.services.delay_service.publisher import (
    get_dk_info_publisher,
    get_promocode_publisher,
    user_active,
    user_deactive)


logger = logging.getLogger(__name__)

user_router = Router()


@user_router.message(Command(commands='cancel'), StateFilter(PromocodeFillForm))
async def process_cancel_command_state(message: Message, state: FSMContext):
    await message.answer(
        text='Выберете действие.'
    )
    # Сбрасываем состояние и очищаем данные, полученные внутри состояний
    await state.set_state()


@user_router.message(Command(commands='cancel'), StateFilter(GetDKInfoFillForm))
async def process_cancel_command_state(message: Message, state: FSMContext):
    await message.answer(
        text='Выберете действие.'
    )
    # Сбрасываем состояние и очищаем данные, полученные внутри состояний
    await state.set_state()



@user_router.message(CommandStart())
async def process_start_command(
                                message: Message,
                                state: FSMContext,
                                js,
                                subject_user_active_publisher: str):
    await state.clear()
    logger.info(f'{message.chat.username} ({message.chat.id}) - start bot')
    await user_active(
        js=js,
        user_id=message.chat.id,
        user_name=message.chat.username,
        subject=subject_user_active_publisher
    )

@user_router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=KICKED))
async def process_kicked_user(
                                event: ChatMemberUpdated,
                                state: FSMContext,
                                js,
                                subject_user_inactive_publisher: str):
    await state.clear()
    logger.info(f'{event.from_user.username} ({event.from_user.id}) - kicked bot')
    await user_deactive(
        js=js,
        user_id=event.from_user.id,
        user_name=event.from_user.username,
        subject=subject_user_inactive_publisher
    )


@user_router.message(Command(commands='help'))
async def process_help_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(text='HELP!')


@user_router.message(Command(commands='support'))
async def process_support_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(text='SUPPORT!')


@user_router.message(Command(commands='info'))
async def process_info_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(text='INFO!')


# Форма для уточнения скидки по карте покупателя
# --------------------------------------------------------------------
@user_router.message(Command(commands='dkinfo'))
async def process_dkinfo_command(message: Message, state: FSMContext):
    logger.info(f'{message.chat.username} ({message.chat.id}) '
                '- start fill dkinfo')
    await message.answer(text='Пожалуйста, введите номер дисконтной карты (без DK)')
    await state.set_state(GetDKInfoFillForm.fill_dk)


@user_router.message(StateFilter(GetDKInfoFillForm.fill_dk),
                     lambda x: x.text.isdigit() and len(x.text) == 9)
async def process_dkinfo_dk(message: Message, state: FSMContext):
    await state.update_data(dk=int(message.text))
    await message.answer(
        text='Введите Фамилию владельца карты.'
    )
    await state.set_state(GetDKInfoFillForm.fill_dk_owner)


@user_router.message(StateFilter(GetDKInfoFillForm.fill_dk))
async def dk_info_wrong_dk(message: Message):
    await message.answer(
        text='Номер карты должен состоять из цифр (вводите без DK) длиной 9 символов'
    )


@user_router.message(StateFilter(GetDKInfoFillForm.fill_dk_owner),
                     lambda x: x.text.isalpha())
async def process_dk_info_dk_owner(
                                    message: Message,
                                    state: FSMContext,
                                    js,
                                    subject_user_dk_publisher):
    await state.update_data(dk_owner=message.text)

    dk_info = await state.get_data()

    dk, dk_owner = dk_info['dk'], dk_info['dk_owner']

    await get_dk_info_publisher(
                                js=js,
                                chat_id=message.chat.id,
                                dk=dk,
                                dk_owner=dk_owner,
                                subject=subject_user_dk_publisher
    )
    await state.set_state()


@user_router.message(StateFilter(GetDKInfoFillForm.fill_dk_owner))
async def dk_info_wrong_dk_owner(message: Message):
    await message.answer(
        text='Введите корректную фамилию владельца.'
    )
# --------------------------------------------------------------------


# Форма получения промокода
# --------------------------------------------------------------------
@user_router.message(Command(commands='promocode'))
async def process_promocode_command(message: Message, state: FSMContext):
    logger.info(f'{message.chat.username} ({message.chat.id}) '
                '- start fill promocode')
    await message.answer(text='Пожалуйста, введите номер дисконтной карты (без DK)')
    await state.set_state(PromocodeFillForm.fill_dk)


@user_router.message(StateFilter(PromocodeFillForm.fill_dk),
                     lambda x: x.text.isdigit() and len(x.text) == 9)
async def process_promocode_dk(message: Message, state: FSMContext):
    await state.update_data(dk=int(message.text))
    await message.answer(
        text='Введите Фамилию владельца карты.'
    )
    await state.set_state(PromocodeFillForm.fill_dk_owner)


@user_router.message(StateFilter(PromocodeFillForm.fill_dk))
async def promocode_wrong_dk(message: Message):
    await message.answer(
        text='Номер карты должен состоять из цифр (вводите без DK) длиной 9 символов'
    )


@user_router.message(StateFilter(PromocodeFillForm.fill_dk_owner),
                     lambda x: x.text.isalpha())
async def process_promocode_dk_owner(
                                    message: Message,
                                    state: FSMContext,
                                    js,
                                    subject_user_promocode_publisher):
    await state.update_data(dk_owner=message.text)

    dk_info = await state.get_data()

    dk, dk_owner = dk_info['dk'], dk_info['dk_owner']

    await get_promocode_publisher(
                                js=js,
                                chat_id=message.chat.id,
                                dk=dk,
                                dk_owner=dk_owner,
                                subject=subject_user_promocode_publisher
    )
    await state.set_state()


@user_router.message(StateFilter(PromocodeFillForm.fill_dk_owner))
async def promocode_wrong_dk_owner(message: Message):
    await message.answer(
        text='Введите корректную фамилию владельца.'
    )
# --------------------------------------------------------------------
