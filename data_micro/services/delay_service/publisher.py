import json

from nats.js.client import JetStreamContext
# from tenacity import retry # библиотека для повторных отправок сообщений

# брокеры сообщений - паблишеры

# отправка списка всех дк
async def push_dk_list_publisher(
        js: JetStreamContext,
        chat_id: int,
        data: list[dict],
        subject: str
) -> None:

    payload = json.dumps({
        'chat_id': chat_id,
        'data': data
    }).encode()

    await js.publish(subject=subject, payload=payload)


# отправка списка выданных промокодов
async def push_promocode_list_publisher(
        js: JetStreamContext,
        chat_id: int,
        data: list[dict],
        subject: str
) -> None:

    payload = json.dumps({
        'chat_id': chat_id,
        'data': data
    }).encode()

    await js.publish(subject=subject, payload=payload)


# отправка информации по карте покупателя
async def push_dk_info_publisher(
        js: JetStreamContext,
        chat_id: int,
        dk: int,
        dk_owner: str,
        info: str,
        subject: str
):
    payload = json.dumps({
        'chat_id': chat_id,
        'dk': dk,
        'dk_owner': dk_owner,
        'info': info
    }).encode()

    await js.publish(subject=subject, payload=payload)


# отправка промокода покупателем
async def push_promocode_publisher(
        js: JetStreamContext,
        chat_id: int,
        promocode: int,
        subject: str
):
    payload = json.dumps({
        'chat_id': chat_id,
        'promocode': promocode
    }).encode()

    await js.publish(subject=subject, payload=payload)
