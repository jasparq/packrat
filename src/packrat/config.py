from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import os


@dataclass(frozen=True)
class Config:
    base_dir: Path
    archive_dir: Path
    meta_name: str = "archive.meta.json"
    buf_size: int = 1024 * 1024
    dry_run: bool = False
    lock_path: Path = Path("/var/lock/archive-daemon.lock")

    # Dataverse variables
    dataverse_enabled: bool = False
    dataverse_url: Optional[str] = None
    dataverse_tenant_id: Optional[str] = None
    dataverse_client_id: Optional[str] = None
    dataverse_client_secret: Optional[str] = None

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

        dv_enabled = bool(int(os.getenv("DATAVERSE_ENABLED", "0")))
        dv_url = os.getenv("DATAVERSE_URL")
        dv_tenant = os.getenv("DATAVERSE_TENANT_ID")
        dv_client = os.getenv("DATAVERSE_CLIENT_ID")
        dv_secret = os.getenv("DATAVERSE_CLIENT_SECRET")

        return Config(
            base_dir=b,
            archive_dir=a,
            dry_run=dr,
            dataverse_enabled=dv_enabled,
            dataverse_url=dv_url,
            dataverse_tenant_id=dv_tenant,
            dataverse_client_id=dv_client,
            dataverse_client_secret=dv_secret,
        )
