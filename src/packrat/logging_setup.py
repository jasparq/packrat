import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

DEFAULT_FORMAT = "time=%(asctime)s level=%(levelname)s msg=%(message)s"
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
DEFAULT_BACKUP_COUNT = 7

def _resolve_log_file(
        log_file: Optional[str],
        log_dir: Optional[str],
) -> Optional[Path]:
    """
    Decide where to write the file log, honoring explicit args first,
    then environment variables, then sensible defaults.
    """
    # 1 Explicit args
    if log_file:
        return Path(log_file)
    if log_dir:
        return Path(log_dir) / "packrat.log"
    
    # 2 Environment variables
    env_log_file = os.getenv("PACKRAT_LOG_FILE")
    env_log_dir = os.getenv("PACKRAT_LOG_DIR")
    if env_log_file:
        return Path(env_log_file)
    if env_log_dir:
        return Path(env_log_dir) / "packrat.log"
    
    # 3 Default
    return Path("/var/log/packrat/packrat.log")

def setup_logging(
        level: str= "INFO",
        *,
        to_stdout: bool = True,
        to_file: bool = True,
        log_file: Optional[str] = None,
        log_dir: Optional[str] = None,
        fmt: str = DEFAULT_FORMAT,
        file_max_bytes: int = DEFAULT_MAX_BYTES,
        file_backup_count: int = DEFAULT_BACKUP_COUNT,
) -> None:
    """
    Configure root logging:
    - stdout handler
    - rotating file handler
    """
    lvl = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(lvl)

    # Remove any old handlers to prevent duplicates
    for h in list(root.handlers):
        root.removeHandler(h)

    formatter = logging.Formatter(fmt)

    # STDOUT handler
    if to_stdout:
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(lvl)
        sh.setFormatter(formatter)
        root.addHandler(sh)

    # File handler
    if to_file:
        target = _resolve_log_file(log_file, log_dir)
        if target:
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                fh = RotatingFileHandler(
                    target,
                    maxBytes=file_max_bytes,
                    backupCount=file_backup_count,
                    encoding="utf-8",
                    delay=True,
                )
                fh.setLevel(lvl)
                fh.setFormatter(formatter)
                root.addHandler(fh)
            except Exception as e:
                logging.getLogger(__name__).warning(
                    "File logging disabled (could not open %s): %s", target ,e
                )