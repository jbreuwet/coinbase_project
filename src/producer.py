import asyncio
import json
import os

import aio_pika
import websockets
from dotenv import load_dotenv

from logger_config import setup_logger

load_dotenv()
logger = setup_logger()

WS_URL        = os.getenv("COINBASE_WS_URL")
TRADING_PAIR  = os.getenv("TRADING_PAIR")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
QUEUE_NAME    = os.getenv("RABBITMQ_QUEUE")


async def get_rabbitmq_channel():
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        login=RABBITMQ_USER,
        password=RABBITMQ_PASS,
    )
    channel = await connection.channel()
    await channel.declare_queue(QUEUE_NAME, durable=True)
    logger.info("RabbitMQ connection established and queue declared")
    return connection, channel


async def publish_trade(channel, payload: dict):
    message = aio_pika.Message(
        body=json.dumps(payload).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    await channel.default_exchange.publish(
        message,
        routing_key=QUEUE_NAME,
    )


async def build_subscribe_message():
    return json.dumps({
        "type": "subscribe",
        "product_ids": [TRADING_PAIR],
        "channel": "market_trades",
    })


async def run():
    connection, channel = await get_rabbitmq_channel()
    backoff = 1

    while True:
        try:
            logger.info(f"Connecting to Coinbase WebSocket at {WS_URL}...")
            async with websockets.connect(WS_URL) as ws:
                subscribe_msg = await build_subscribe_message()
                await ws.send(subscribe_msg)
                logger.success(f"Subscribed to {TRADING_PAIR} market_trades channel")
                backoff = 1

                async for raw_message in ws:
                    data = json.loads(raw_message)

                    if data.get("channel") != "market_trades":
                        logger.debug(f"Skipping non-trade message: {data.get('channel')}")
                        continue

                    events = data.get("events", [])
                    for event in events:
                        for trade in event.get("trades", []):
                            await publish_trade(channel, trade)
                            logger.info(
                                f"Published | trade_id={trade.get('trade_id')} "
                                f"price={trade.get('price')} "
                                f"size={trade.get('size')} "
                                f"side={trade.get('side')}"
                            )

        except websockets.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e} — retrying in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

        except Exception as e:
            logger.error(f"Unexpected error: {e} — retrying in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
