import json
import logging
import csv
import random

from data_micro.services.delay_service.publisher import (
    push_dk_list_publisher,
    push_promocode_list_publisher,
    push_dk_info_publisher,
    push_promocode_publisher,
    sucsess_add_user_publisher
    )

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.js import JetStreamContext

logger = logging.getLogger(__name__)

# брокеры сообщений - консьюмеры

# Активация пользователя при старте бота
class DoUserActiveConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            subject_consumer: str,
            subject_publisher: str,
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.subject_publisher = subject_publisher
        self.stream = stream

    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.do_user_active,
            manual_ack=True
        )

    async def do_user_active(self, msg: Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            # добавление в БД и статс активен
            await sucsess_add_user_publisher(
                self.js,
                payload['user_id'],
                self.subject_publisher)

            logger.debug(f'Пользоватль {payload['user_name']} активен')

        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')


# Деактивация пользователя при бане бота
class DoUserInactiveConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            subject_consumer: str,
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.stream = stream

    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.do_user_inactive,
            manual_ack=True
        )

    # получение данных из бд
    async def do_user_inactive(self, msg: Msg) -> None:
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            # изменение статуса пользователя на неактивен в БД
            logger.debug(f'Пользоватль {payload['user_name']} не активен')

        except KeyboardInterrupt:
            logger.info('stop by keyboard')
        except Exception as e:
            logger.exception(e)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')

# получение списка всех дк
class GetDkListConsumer:
    def __init__(
            self,
            nc: Client,
            js: JetStreamContext,
            subject_consumer: str,
            subject_publisher: str,
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.subject_publisher = subject_publisher
        self.stream = stream

    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.get_dk_list,
            manual_ack=True
        )

    # получение данных из бд
    async def get_dk_list(self, msg: Msg) -> None:
        logger.debug('DK_LIST')
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            with open('data_micro/data_dk.csv', newline='', encoding='utf-8') as file:
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
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.subject_publisher = subject_publisher
        self.stream = stream

    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.get_promocode_list,
            manual_ack=True
        )

    # получение данных из бд
    async def get_promocode_list(self, msg: Msg) -> None:
        logger.debug('PROMOCODE_LIST')
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            with open('data_micro/data_dk.csv', newline='', encoding='utf-8') as file:
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
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.subject_publisher = subject_publisher
        self.stream = stream

    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.get_dk_info,
            manual_ack=True
        )

    # получение данных из бд
    async def get_dk_info(self, msg: Msg) -> None:
        logger.debug('DK_INFO')
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            with open('data_micro/data_dk.csv', newline='', encoding='utf-8') as file:
                for row in csv.DictReader(file):
                    logger.debug(f'row: {row}')
                    logger.debug(f'payload: {payload}')
                    if row['last_name'] == payload['dk_owner'] and row['dk'] == str(payload['dk']):
                        info = row['discount']
                        break
                    else:
                        info = False
            if not info:
                info = 'Даннные не найдены.'
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
            stream: str
    ) -> None:
        self.nc = nc
        self.js = js
        self.subject_consumer = subject_consumer
        self.subject_publisher = subject_publisher
        self.stream = stream

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

    async def start(self) -> None:
        # нужно так же указывать deliver_policy
        # (all, last, new, by_start_sequence)
        self.stream_sub = await self.js.subscribe(
            subject=self.subject_consumer,
            stream=self.stream,
            cb=self.get_promocode,
            manual_ack=True
        )

    # получение данных из бд
    async def get_promocode(self, msg: Msg) -> None:
        logger.debug('PROMOCODE')
        payload = json.loads(msg.data)
        await msg.ack()

        try:
            with open('data_micro/data_dk.csv', newline='', encoding='utf-8') as file:
                data = list(csv.DictReader(file))
            for id, row in enumerate(data):
                if row['last_name'] == payload['dk_owner'] and row['dk'] == str(payload['dk']):
                    if row['promocode'] == '':
                        promocode = random.choice(self.promocodes)
                        logger.debug(f'promocode: {promocode}')
                        data[id]['promocode'] = promocode
                        await self.write_promocode(data)
                    else:
                        promocode = row['promocode']
                    break
                else:
                    promocode = 'Даннные не найдены.'

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
        logger.debug('write_promocode')
        with open('data_micro/data_dk.csv', 'w', newline='', encoding='utf-8') as file:
            fieldnames = ['last_name', 'dk', 'discount', 'promocode']
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(data)

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info('Consumer unsubscribe')
