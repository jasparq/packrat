from dataclasses import dataclass
from pathlib import Path
import os

@dataclass(frozen=True)
class Config:
    base_dir: Path
    archive_dir: Path
    meta_name: str = "archive.meta.json"
    buf_size: int = 1024*1024
    dry_run: bool = False
    lock_path: Path = Path("/var/lock/archive-daemon.lock")

    @staticmethod
    def from_env(
        base_dir: str | None = None,
        archive_dir: str | None = None,
        dry_run: bool | None = None,
    ) -> "Config":
        b = Path(base_dir or os.getenv("ARCHIVE_BASE_DIR", "/mnt/archive/STAGING"))
        a = Path(archive_dir or os.getenv("ARCHIVE_DIR", "/mnt/archive/VAULT"))
        a.mkdir(parents=True, exist_ok=True)
        dr = bool(int(os.getenv("ARCHIVE_DRY_RUN", "0"))) if dry_run is None else dry_run
        return Config(base_dir=b, archive_dir=a, dry_run=dr)
