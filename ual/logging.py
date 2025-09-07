import logging
import sys


def get_logger(name: str = None):
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.WARNING)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.propagate = False

    return logger
