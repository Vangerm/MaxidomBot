import json
import logging
import csv
import random

from services.delay_service.publisher import (
    push_dk_list_publisher,
    push_promocode_list_publisher,
    push_dk_info_publisher,
    push_promocode_publisher
    )

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.js import JetStreamContext

logger = logging.getLogger(__name__)

# брокеры сообщений - консьюмеры

# получение списка всех дк
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

    # получение данных из бд
    async def get_dk_list(self, msg: Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            with open('data_dk.csv', newline='', encoding='utf-8') as file:
                data = list(csv.DictReader(file))
            await push_dk_list_publisher(
                self.js,
                payload['chat_id'],
                data,
                self.subject_publisher)

        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')


# получение списка выданных промокодов
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

    # получение данных из бд
    async def get_promocode_list(self, msg: Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            with open('data_dk.csv', newline='', encoding='utf-8') as file:
                data = list()
                for row in csv.DictReader(file):
                    if row['promocode'] != '':
                        data.append(row)
            await push_promocode_list_publisher(
                self.js,
                payload['chat_id'],
                data,
                self.subject_publisher)

        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')


# уточнение информации по карте покупателя
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

    # получение данных из бд
    async def get_dk_info(self, msg: Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            with open('data_dk.csv', newline='', encoding='utf-8') as file:
                for row in csv.DictReader(file):
                    if row['last_name'] == payload['dk_owner'] and row['dk'] == payload['dk']:
                        info = row['discount']
            await push_dk_info_publisher(
                self.js,
                payload['chat_id'],
                payload['dk'],
                payload['dk_owner'],
                info,
                self.subject_publisher)

        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')


# получение промокода покупателем
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

        self.promocodes = ['8qqLp7BH',
                        'uodK3MGq',
                        'OF5JCH7E',
                        'BnoIiEB3',
                        'E8FAxIJC',
                        '6BtOYWRM',
                        'XupjpWwf',
                        '1jqQ0VU5',
                        'ScPOWOvh',
                        '1syyOM2l']

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

    # получение данных из бд
    async def get_promocode(self, msg: Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            with open('data_dk.csv', newline='', encoding='utf-8') as file:
                data = list(csv.DictReader(file))
            for id, row in enumerate(data):
                if row['last_name'] == payload['dk_owner'] and row['dk'] == payload['dk']:
                    if row['promocode'] == '':
                        promocode = random.choice(self.promocodes)
                        data[id]['promocode'] = promocode
                        self.write_promocode(data)
                    else:
                        promocode = row['promocode']

            await push_promocode_publisher(
                self.js,
                payload['chat_id'],
                promocode,
                self.subject_publisher)

        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def write_promocode(self, data):
        with open('data_dk.csv', 'w', newline='', encoding='utf-8') as file:
            fieldnames = ['last_name', 'dk', 'discount', 'promocode']
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(data)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')
