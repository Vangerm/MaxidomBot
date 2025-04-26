import json

from nats.js.client import JetStreamContext

# брокеры сообщений - паблишеры

# получение списка всех дк
async def get_dk_list_publisher(
        js: JetStreamContext,
        chat_id: int,
        subject: str
) -> None:

    payload = json.dumps({
        'chat_id': chat_id
    }).encode()

    await js.publish(subject=subject, payload=payload)


# получение списка выданных промокодов
async def get_promocode_list_publisher(
        js: JetStreamContext,
        chat_id: int,
        subject: str
) -> None:

    payload = json.dumps({
        'chat_id': chat_id
    }).encode()

    await js.publish(subject=subject, payload=payload)


# уточнение информации по карте покупателя
async def get_dk_info_publisher(
        js: JetStreamContext,
        chat_id: int,
        dk: int,
        dk_owner: str,
        subject: str
):
    payload = json.dumps({
        'chat_id': chat_id,
        'dk': dk,
        'dk_owner': dk_owner
    })

    await js.publish(subject=subject, payload=payload)


# получение промокода покупателем
async def get_promocode_publisher(
        js: JetStreamContext,
        chat_id: int,
        dk: int,
        dk_owner: str,
        subject: str
):
    payload = json.dumps({
        'chat_id': chat_id,
        'dk': dk,
        'dk_owner': dk_owner
    })

    await js.publish(subject=subject, payload=payload)
