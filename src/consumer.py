import asyncio
import json
import os

import asyncpg
import aio_pika
from dotenv import load_dotenv

from logger_config import setup_logger

load_dotenv()
logger = setup_logger()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
QUEUE_NAME    = os.getenv("RABBITMQ_QUEUE")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB   = os.getenv("POSTGRES_DB")


async def get_postgres_pool():
    pool = await asyncpg.create_pool(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASS,
        database=POSTGRES_DB,
        min_size=5,
        max_size=20
    )
    logger.info("Postgres pool established")
    return pool


async def get_rabbitmq_channel():
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        login=RABBITMQ_USER,
        password=RABBITMQ_PASS,
    )
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=100)
    logger.info("RabbitMQ connection established")
    return connection, channel


async def insert_trade(pool, payload: dict):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bronze.trades (raw_payload)
            VALUES ($1::jsonb)
            """,
            json.dumps(payload),
    )


async def process_message(message: aio_pika.IncomingMessage, pool):
    async with message.process(requeue=True):
        try:
            payload = json.loads(message.body.decode())
            await insert_trade(pool, payload)
            logger.info(
                f"Inserted | trade_id={payload.get('trade_id')} "
                f"price={payload.get('price')} "
                f"side={payload.get('side')}"
            )
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            raise


async def run():
    pool = await get_postgres_pool()
    connection, channel = await get_rabbitmq_channel()
    backoff = 1

    while True:
        try:
            queue = await channel.declare_queue(QUEUE_NAME, durable=True)
            logger.info(f"Listening on queue: {QUEUE_NAME}")

            await queue.consume(
                lambda message: asyncio.ensure_future(
                    process_message(message, pool)
                )
            )

            await asyncio.Future()

        except Exception as e:
            logger.error(f"Consumer error: {e} — retrying in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    logger.info("Consumer starting up...")
    asyncio.run(run())