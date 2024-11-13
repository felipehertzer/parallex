import logging

from aiologger import Logger

logger = Logger.with_default_handlers(name="parallex")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
