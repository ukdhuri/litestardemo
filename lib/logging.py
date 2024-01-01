import logging
from litestar.logging import LoggingConfig
from loguru import logger
from datetime import datetime, time, timedelta
from lib.util import cfg
import sys


log_file_name = datetime.now().strftime("%Y-%m-%d") + ".log"
log_file=f"{cfg.logpath}/{log_file_name}"
logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} {level} {message} ", level="ERROR")
logger.add(f"{log_file}", format="{time:YYYY-MM-DD HH:mm:ss} {level} {message}", rotation=time(17, 0), compression="zip", retention=timedelta(days=10), enqueue=True)
logger.info("Logging Configured")


logging_config = LoggingConfig(
    root={"level": logging.getLevelName(logging.INFO), "handlers": ["console"]},
    formatters={
        "standard": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}
    },
)

