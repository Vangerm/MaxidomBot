import logging

from data_micro.services.delay_service.consumer import (
    GetDkListConsumer,
    GetPromocodeListConsumer,
    GetDkInfoConsumer,
    GetPromocodeConsumer
)

from nats.aio.client import Client
from nats.js.client import JetStreamContext


logger = logging.getLogger(__name__)


async def start_poll_dk_list(
        nc: Client,
        js: JetStreamContext,
        subject_consumer: str,
        subject_publisher: str,
        stream: str
        ) -> None:
    logger.debug(f'subject: {subject_consumer}, stream: {stream}')
    consumer = GetDkListConsumer(
        nc=nc,
        js=js,
        subject_consumer=subject_consumer,
        subject_publisher=subject_publisher,
        stream=stream
    )
    logger.info('Start poll dk info consumer')
    await consumer.start()


async def start_poll_dk_info(
        nc: Client,
        js: JetStreamContext,
        subject_consumer: str,
        subject_publisher: str,
        stream: str
        ) -> None:
    logger.debug(f'subject: {subject_consumer}, stream: {stream}')
    consumer = GetDkInfoConsumer(
        nc=nc,
        js=js,
        subject_consumer=subject_consumer,
        subject_publisher=subject_publisher,
        stream=stream
    )
    logger.info('Start poll dk info consumer')
    await consumer.start()


async def start_poll_promocode(
        nc: Client,
        js: JetStreamContext,
        subject_consumer: str,
        subject_publisher: str,
        stream: str
        ) -> None:
    logger.debug(f'subject: {subject_consumer}, stream: {stream}')
    consumer = GetPromocodeConsumer(
        nc=nc,
        js=js,
        subject_consumer=subject_consumer,
        subject_publisher=subject_publisher,
        stream=stream
    )
    logger.info('Start poll promocode consumer')
    await consumer.start()


async def start_poll_promocode_list(
        nc: Client,
        js: JetStreamContext,
        subject_consumer: str,
        subject_publisher: str,
        stream: str
        ) -> None:
    logger.debug(f'subject: {subject_consumer}, stream: {stream}')
    consumer = GetPromocodeListConsumer(
        nc=nc,
        js=js,
        subject_consumer=subject_consumer,
        subject_publisher=subject_publisher,
        stream=stream
    )
    logger.info('Start poll promocode list consumer')
    await consumer.start()