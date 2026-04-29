import asyncio
from logger_config import setup_logger
from producer import run as producer_run
from consumer import run as consumer_run

logger = setup_logger()

async def main():
    await asyncio.gather(
        producer_run(),
        consumer_run()
    )
    
if __name__ == "__main__":
    asyncio.run(main())
    
