import inspect
from pathlib import Path

import yaml

from ual.logging import get_logger

logging = get_logger("get_config")


def get_config(file_path: str) -> dict:
    os_independent_path = _get_caller_directory(2) / Path(file_path)
    try:
        with open(os_independent_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error("No config found in directory")
        raise
    except PermissionError:
        logging.error("Application hasn´t the permission to read file")
        raise
    except UnicodeDecodeError:
        logging.error("Error in unicode decoding")
        raise
    except OSError:
        logging.error("IOError: An I/O error occurred")
        raise


def _get_caller_directory(stack_position: int) -> Path:
    caller_file = inspect.stack()[stack_position].filename
    return Path(caller_file).parent
