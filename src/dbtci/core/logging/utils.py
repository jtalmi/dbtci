import logging
import sys


def setup_logging(log_level=logging.INFO):
    logging.basicConfig(
        stream=sys.stderr,
        format="[%(threadName)10s][%(levelname)s][%(asctime)s] %(message)s",
        level=log_level,
    )
