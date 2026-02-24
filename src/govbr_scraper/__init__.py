import os
import sys

from loguru import logger

# Configure loguru to respect LOG_LEVEL (default: INFO)
_log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logger.remove()
logger.add(sys.stderr, level=_log_level)
