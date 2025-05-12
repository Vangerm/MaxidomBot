from dataclasses import dataclass
from environs import Env


@dataclass
class NatsConfig:
    servers: list[str]


@dataclass
class NatsStreamConfig:
    subject_admin_dk_publisher: str
    subject_admin_promocode_publisher: str
    subject_user_dk_publisher: str
    subject_user_promocode_publisher: str
    subject_add_user_success_publisher: str

    subject_admin_dk_consumer: str
    subject_admin_promocode_consumer: str
    subject_user_dk_consumer: str
    subject_user_promocode_consumer: str
    subject_user_do_active_consumer: str
    subject_user_do_inactive_consumer: str

    stream: str


@dataclass
class Config:
    nats: NatsConfig
    stream_config: NatsStreamConfig


def load_config(path: str | None = None) -> Config:

    env: Env = Env()
    env.read_env(path)

    return Config(
        nats=NatsConfig(
            servers=env.list('NATS_SERVERS')
        ),
        stream_config=NatsStreamConfig(
            subject_admin_dk_publisher=env('NATS_ADMIN_DK_PUBLISHER'),
            subject_admin_promocode_publisher=env('NATS_ADMIN_PROMOCODE_PUBLISHER'),
            subject_user_dk_publisher=env('NATS_USER_DK_PUBLISHER'),
            subject_user_promocode_publisher=env('NATS_USER_PROMOCODE_PUBLISHER'),
            subject_add_user_success_publisher=env('NATS_ADD_USER_SUCCESS_PUBLISHER'),
            subject_admin_dk_consumer=env('NATS_ADMIN_DK_CONSUMER'),
            subject_admin_promocode_consumer=env('NATS_ADMIN_PROMOCODE_CONSUMER'),
            subject_user_dk_consumer=env('NATS_USER_DK_CONSUMER'),
            subject_user_promocode_consumer=env('NATS_USER_PROMOCODE_CONSUMER'),
            subject_user_do_active_consumer=env('NATS_USER_DO_ACTIVE_CONSUMER'),
            subject_user_do_inactive_consumer=env('NATS_USER_DO_INACTIVE_CONSUMER'),
            stream=env('NATS_STREAM_CONSUMER')
        )
    )
