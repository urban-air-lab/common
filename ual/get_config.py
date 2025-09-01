import yaml
import inspect
from pathlib import Path

from ual.logging import get_logger

logging = get_logger()


def get_config(file: str) -> dict:
    os_independent_path = _get_caller_directory(2) / Path(file)
    try:
        with open(os_independent_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"No config found in directory")
        raise
    except IOError:
        logging.error(f"IOError: An I/O error occurred")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


def _get_caller_directory(stack_position: int) -> Path:
    caller_file = inspect.stack()[stack_position].filename
    return Path(caller_file).parent
