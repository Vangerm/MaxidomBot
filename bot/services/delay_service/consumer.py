import json
import logging
from contextlib import suppress

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.js import JetStreamContext

logger = logging.getLogger(__name__)


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
        text = f'Ваш промокод: {promocode}'

        with suppress(TelegramBadRequest):
            await self.bot.send_message(
                chat_id=chat_id,
                text=text
            )
