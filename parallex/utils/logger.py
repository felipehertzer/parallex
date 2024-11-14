import logging

from aiologger.loggers.json import JsonLogger

logger = JsonLogger.with_default_handlers(name="parallex")


def setup_logger(level: str = "ERROR"):
    level = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }.get(level, logging.INFO)

    logger.setLevel = level
