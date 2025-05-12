import logging

from aiogram import Bot
from bot.services.delay_service.consumer import (
    SuccessAddConsumer,
    PushDkListConsumer,
    PushPromocodeListConsumer,
    PushDKInfoConsumer,
    PushPromocodeConsumer
)

from nats.aio.client import Client
from nats.js.client import JetStreamContext


logger = logging.getLogger(__name__)


async def start_poll_user_active(
        nc: Client,
        js: JetStreamContext,
        bot: Bot,
        subject_consumer: str,
        stream: str
        ) -> None:
    logger.info('Start poll user active consumer')
    consumer = SuccessAddConsumer(
        nc=nc,
        js=js,
        bot=bot,
        subject_consumer=subject_consumer,
        stream=stream
    )
    await consumer.start()


async def start_poll_dk_info(
        nc: Client,
        js: JetStreamContext,
        bot: Bot,
        subject_consumer: str,
        stream: str
        ) -> None:
    consumer = PushDKInfoConsumer(
        nc=nc,
        js=js,
        bot=bot,
        subject_consumer=subject_consumer,
        stream=stream
    )
    logger.info('Start poll dk info consumer')
    await consumer.start()


async def start_poll_promocode(
        nc: Client,
        js: JetStreamContext,
        bot: Bot,
        subject_consumer: str,
        stream: str
        ) -> None:
    consumer = PushPromocodeConsumer(
        nc=nc,
        js=js,
        bot=bot,
        subject_consumer=subject_consumer,
        stream=stream
    )
    logger.info('Start poll promocode consumer')
    await consumer.start()


async def start_poll_promocode_list(
        nc: Client,
        js: JetStreamContext,
        bot: Bot,
        subject_consumer: str,
        stream: str
        ) -> None:
    consumer = PushPromocodeListConsumer(
        nc=nc,
        js=js,
        bot=bot,
        subject_consumer=subject_consumer,
        stream=stream
    )
    logger.info('Start poll promocode list consumer')
    await consumer.start()

async def start_poll_dk_list(
        nc: Client,
        js: JetStreamContext,
        bot: Bot,
        subject_consumer: str,
        stream: str
        ) -> None:
    consumer = PushDkListConsumer(
        nc=nc,
        js=js,
        bot=bot,
        subject_consumer=subject_consumer,
        stream=stream
    )
    logger.info('Start poll dk list consumer')
    await consumer.start()