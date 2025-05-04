import asyncio
import logging.config

from data_micro.loger.logging_settings import logging_config
from data_micro.config_data.config import load_config
from data_micro.utils.nats_connect import connect_to_nats
from data_micro.utils.start_consumer import (
    start_poll_dk_info,
    start_poll_promocode,
    start_poll_dk_list,
    start_poll_promocode_list
)


logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info('Starting microservice')

    # Получаем конфигурационные данные
    config = load_config()

    stream = config.stream_config.stream
    durable_name = config.stream_config.durable_name

    # Подключаемся к NATS
    nc, js = await connect_to_nats(servers=config.nats.servers)

    try:
        await asyncio.gather(
            start_poll_dk_list(
                nc=nc,
                js=js,
                subject_consumer=config.stream_config.subject_admin_dk_consumer,
                subject_publisher=config.stream_config.subject_admin_dk_publisher,
                stream=stream,
                durable_name=durable_name
            ),
            start_poll_dk_info(
                nc=nc,
                js=js,
                subject_consumer=config.stream_config.subject_user_dk_consumer,
                subject_publisher=config.stream_config.subject_user_dk_publisher,
                stream=stream,
                durable_name=durable_name
            ),
            start_poll_promocode(
                nc=nc,
                js=js,
                subject_consumer=config.stream_config.subject_user_promocode_consumer,
                subject_publisher=config.stream_config.subject_user_promocode_publisher,
                stream=stream,
                durable_name=durable_name
            ),
            start_poll_promocode_list(
                nc=nc,
                js=js,
                subject_consumer=config.stream_config.subject_admin_promocode_consumer,
                subject_publisher=config.stream_config.subject_admin_promocode_publisher,
                stream=stream,
                durable_name=durable_name
            )
        )
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info('Stop microservice')
    except Exception as e:
        logger.exception(e)
    finally:
        # Закрываем соединение с NATS
        await nc.close()
        logger.info('Connection to NATS closed')


if __name__ == '__main__':
    asyncio.run(main())
