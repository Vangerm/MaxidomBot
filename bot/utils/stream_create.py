import logging

from nats.js.api import (
                        StreamConfig,
                        RetentionPolicy,
                        StorageType)
from nats.js.client import JetStreamContext


logger = logging.getLogger(__name__)


async def create_stream(js: JetStreamContext, stream: str) -> None:
    stream_config = StreamConfig(
        name=stream,
        subjects=[
            'user.>',
            'admin.>'
        ],
        retention=RetentionPolicy.WORK_QUEUE, # Политика удержания
        max_bytes=300 * 1024 * 1024,  # 300 MiB
        max_msg_size=10 * 1024 * 1024,  # 10 MiB
        storage=StorageType.FILE,  # Хранение сообщений на диске
        allow_direct=True,  # Разрешение получать сообщения без создания консьюмера
    )

    await js.add_stream(stream_config)
    logging.info(f"Stream '{stream}' created successfully.")
