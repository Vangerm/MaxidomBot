from aiogram.fsm.state import State, StatesGroup


class PromocodeFillForm(StatesGroup):
    fill_dk = State()
    fill_dk_owner = State()


class GetDKInfoFillForm(StatesGroup):
    fill_dk = State()
    fill_dk_owner = State()
