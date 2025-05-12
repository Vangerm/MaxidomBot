import json
import logging
from contextlib import suppress

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.js import JetStreamContext

logger = logging.getLogger(__name__)

# Брокеры сообщений - консьюмеры

# Отправка приглашения пользователю
class SuccessAddConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            bot: Bot,
            subject_consumer: str,
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.bot = bot
        self.subject_consumer = subject_consumer
        self.stream = stream

    async def start(self) -> None:
        # можно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.success_add,
            manual_ack=True
        )

    async def success_add(self, msg: Msg):
        payload = json.loads(msg.data)
        await msg.ack()

        with suppress(TelegramBadRequest):
            await self.bot.send_message(
                chat_id=payload['chat_id'],
                text='Welcome!'
            )

    # не обязательно, но можно гибко использовать
    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscriber')


# отправка списка всех дк
class PushDkListConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            bot: Bot,
            subject_consumer: str,
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.bot = bot
        self.subject_consumer = subject_consumer
        self.stream = stream

    async def start(self) -> None:
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.push_dk_list,
            manual_ack=True
        )

    async def push_dk_list(self, msg:Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        chat_id = payload['chat_id']
        data = payload['data']

        # отправка сообщений в телеграм
        text = '\n'.join(f'{dk["last_name"]} - {dk["dk"]} - {dk['discount']}' for dk in data)

        with suppress(TelegramBadRequest):
            await self.bot.send_message(
                chat_id=chat_id,
                text=text
            )

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Unsubscribed from stream')


# отправка списка выданных промокодов
class PushPromocodeListConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            bot: Bot,
            subject_consumer: str,
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.bot = bot
        self.subject_consumer = subject_consumer
        self.stream = stream

    async def start(self) -> None:
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.push_promocode_list,
            manual_ack=True
        )

    async def push_promocode_list(self, msg:Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        chat_id = payload['chat_id']
        data = payload['data']

        # отправка сообщений в телеграм
        text = '\n'.join(f'{dk["last_name"]} - {dk["dk"]} - {dk['promocode']}' for dk in data)

        with suppress(TelegramBadRequest):
            await self.bot.send_message(
                chat_id=chat_id,
                text=text
            )

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Unsubscribed from stream')


# отправка информации по карте покупателя
class PushDKInfoConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            bot: Bot,
            subject_consumer: str,
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.bot = bot
        self.subject_consumer = subject_consumer
        self.stream = stream

    async def start(self) -> None:
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.push_dk_info,
            manual_ack=True
        )

    async def push_dk_info(self, msg:Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        chat_id = payload['chat_id']
        dk = payload['dk']
        dk_owner = payload['dk_owner']
        info = payload['info']

        # отправка сообщений в телеграм
        text = f'{dk_owner} - {dk} - {info}'

        with suppress(TelegramBadRequest):
            await self.bot.send_message(
                chat_id=chat_id,
                text=text
            )

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Unsubscribed from stream')


# выдача промокода покупателю
class PushPromocodeConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            bot: Bot,
            subject_consumer: str,
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.bot = bot
        self.subject_consumer = subject_consumer
        self.stream = stream

    async def start(self) -> None:
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.push_promocode,
            manual_ack=True
        )

    async def push_promocode(self, msg:Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        chat_id = payload['chat_id']
        promocode = payload['promocode']

        # отправка сообщений в телеграм
        text = 'Даннные не найдены.' if promocode == 'Даннные не найдены.' else f'Ваш промокод: {promocode}'

        with suppress(TelegramBadRequest):
            await self.bot.send_message(
                chat_id=chat_id,
                text=text
            )
