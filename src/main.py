import asyncio
from logger_config import setup_logger
from producer import run

logger = setup_logger()

if __name__ == "__main__":
    logger.info("Starting pipeline - Phase 2 test")
    asyncio.run(run())