import asyncio
from logger_config import setup_logger
from producer import run as producer_run
from consumer import run as consumer_run

logger = setup_logger()

async def main():
    logger.info("Starting pipeline - Phase 3 test")
    await asyncio.gather(
        producer_run(),
        consumer_run()
    )
    
if __name__ == "__main__":
    logger.info("Starting pipeline - Phase 2 test")
    asyncio.run(main())
    
