import json
import logging

# from services.delay_service.publisher import

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.js import JetStreamContext

logger = logging.getLogger(__name__)

# брокеры сообщений - консьюмеры

class GetDkListConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            subject_consumer: str,
            subject_publisher: str,
            stream: str,
            durable_name: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.subject_publisher = subject_publisher
        self.stream = stream
        self.durable_name = durable_name

    # консьюмер ловящий данные
    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.get_dk_list,
            durable=self.durable_name,
            manual_ack=True
        )

    async def get_dk_list(self, msg: Msg) -> None:
        # получение данных из бд
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            pass
        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')


class GetPromocodeListConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            subject_consumer: str,
            subject_publisher: str,
            stream: str,
            durable_name: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.subject_publisher = subject_publisher
        self.stream = stream
        self.durable_name = durable_name

    # консьюмер ловящий данные
    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.get_promocode_list,
            durable=self.durable_name,
            manual_ack=True
        )

    async def get_promocode_list(self, msg: Msg) -> None:
        # получение данных из бд
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            pass
        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')


class GetDkInfoConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            subject_consumer: str,
            subject_publisher: str,
            stream: str,
            durable_name: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.subject_publisher = subject_publisher
        self.stream = stream
        self.durable_name = durable_name

    # консьюмер ловящий данные
    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.get_dk_info,
            durable=self.durable_name,
            manual_ack=True
        )

    async def get_dk_info(self, msg: Msg) -> None:
        # получение данных из бд
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            pass
        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')


class GetPromocodeConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            subject_consumer: str,
            subject_publisher: str,
            stream: str,
            durable_name: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.subject_publisher = subject_publisher
        self.stream = stream
        self.durable_name = durable_name

    # консьюмер ловящий данные
    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.get_promocode,
            durable=self.durable_name,
            manual_ack=True
        )

    async def get_promocode(self, msg: Msg) -> None:
        # получение данных из бд
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            pass
        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')
