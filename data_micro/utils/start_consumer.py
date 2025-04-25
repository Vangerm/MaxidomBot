import logging

from aiogram import Bot
from services.delay_service.consumer import VkPostConsumer

from nats.aio.client import Client
from nats.js.client import JetStreamContext


logger = logging.getLogger(__name__)


async def start_poll_dk_info(
        nc: Client,
        js: JetStreamContext,
        bot: Bot,
        subject_consumer: str,
        stream: str,
        durable_name: str
        ) -> None:
    consumer = VkPostConsumer(
        nc=nc,
        js=js,
        bot=bot,
        subject_consumer=subject_consumer,
        stream=stream,
        durable_name=durable_name
    )
    logger.info('Start poll dk info consumer')
    await consumer.start()


async def start_poll_promocode(
        nc: Client,
        js: JetStreamContext,
        bot: Bot,
        subject_consumer: str,
        stream: str,
        durable_name: str
        ) -> None:
    consumer = VkPostConsumer(
        nc=nc,
        js=js,
        bot=bot,
        subject_consumer=subject_consumer,
        stream=stream,
        durable_name=durable_name
    )
    logger.info('Start poll promocode consumer')
    await consumer.start()


async def start_poll_promocode_list(
        nc: Client,
        js: JetStreamContext,
        bot: Bot,
        subject_consumer: str,
        stream: str,
        durable_name: str
        ) -> None:
    consumer = VkPostConsumer(
        nc=nc,
        js=js,
        bot=bot,
        subject_consumer=subject_consumer,
        stream=stream,
        durable_name=durable_name
    )
    logger.info('Start poll promocode list consumer')
    await consumer.start()