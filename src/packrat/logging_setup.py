import logging, sys

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        stream=sys.stdout,
        format="time=%(asctime)s level=%(levelname)s msg=%(message)s",
    )
