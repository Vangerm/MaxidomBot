import asyncio
import logging.config

from aiogram import Bot, Dispatcher
# from fluentogram import TranslatorHub

from bot.loger.logging_settings import logging_config
from bot.config_data.config import load_config
from bot.handlers import get_routers
from bot.storage.nats_storage import NatsStorage
# from middlewares.i18n import TranslatorRunnerMiddleware
# from utils.i18n import create_translator_hub
from bot.utils.nats_connect import connect_to_nats
from bot.utils.start_consumer import (
    start_poll_dk_info,
    start_poll_promocode,
    start_poll_dk_list,
    start_poll_promocode_list)


# Подключаем логирование
logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info('Starting bot')

    # Получаем конфигурационные данные
    config = load_config()

    # Подключаемся к NATS
    nc, js = await connect_to_nats(servers=config.nats.servers)

    # Инициализируем хранилище на базе NATS
    storage: NatsStorage = await NatsStorage(nc=nc, js=js).create_storage()

    # Активация телеграмм бота
    bot: Bot = Bot(token=config.tg_bot.token)
    dp: Dispatcher = Dispatcher(storage=storage)

    # Создаем объект типа TranslatorHub
    # translator_hub: TranslatorHub = create_translator_hub()

    # Получаем роутеры
    dp.include_routers(*get_routers())

    # Инициализируем стрим
    stream = config.stream_config.stream

    # Регистрируем миддлварь для i18n
    # dp.update.middleware(TranslatorRunnerMiddleware())

    # Запускаем polling
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        # await dp.start_polling(bot, _translator_hub=translator_hub)
        # await dp.start_polling(bot)
        await asyncio.gather(
            dp.start_polling(
                bot,
                js=js,
                admin_ids=config.tg_bot.admin_ids,
                subject_admin_dk_publisher=config.stream_config.subject_admin_dk_publisher,
                subject_admin_promocode_publisher=config.stream_config.subject_admin_promocode_publisher,
                subject_user_dk_publisher=config.stream_config.subject_user_dk_publisher,
                subject_user_promocode_publisher=config.stream_config.subject_user_promocode_publisher
            ),
            start_poll_dk_info(
                nc=nc,
                js=js,
                bot=bot,
                subject_consumer=config.stream_config.subject_user_dk_consumer,
                stream=stream
            ),
            start_poll_promocode(
                nc=nc,
                js=js,
                bot=bot,
                subject_consumer=config.stream_config.subject_user_promocode_consumer,
                stream=stream
            ),
            start_poll_dk_list(
                nc=nc,
                js=js,
                bot=bot,
                subject_consumer=config.stream_config.subject_admin_dk_consumer,
                stream=stream
            ),
            start_poll_promocode_list(
                nc=nc,
                js=js,
                bot=bot,
                subject_consumer=config.stream_config.subject_admin_promocode_consumer,
                stream=stream
            )
        )
    except KeyboardInterrupt:
        logger.info('Stop bot')
    except Exception as e:
        logger.exception(e)
    finally:
        # Закрываем соединение с NATS
        await nc.close()
        logger.info('Connection to NATS closed')


asyncio.run(main())
