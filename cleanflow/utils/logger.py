# import libraries
import logging
import sys


def get_logger(name: str = "cleanflow", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)

    # handler
    if logger.hasHandlers():
        return logger

    logger.setLevel(level)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Formatter (clean and readable)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    return logger