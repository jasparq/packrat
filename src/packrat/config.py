from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import os

@dataclass(frozen=True)
class Config:
    base_dir: Path
    archive_dir: Path
    meta_name: str = "archive.meta.json"
    buf_size: int = 1024*1024
    dry_run: bool = False
    lock_path: Path = Path("/var/lock/archive-daemon.lock")

    # sharepoint variables
    sharepoint_enabled: bool = False
    sharepoint_tenant_id: Optional[str] = None
    sharepoint_client_id: Optional[str] = None
    sharepoint_client_secret: Optional[str] = None
    sharepoint_site_id: Optional[str] = None
    sharepoint_list_id: Optional[str] = None

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

        # read sharepoint configuration from environment
        sp_enabled = bool(int(os.getenv("SHAREPOINT_ENABLED","0")))
        sp_tenant = os.getenv("SHAREPOINT_TENANT_ID")
        sp_client = os.getenv("SHAREPOINT_CLIENT_ID")
        sp_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
        sp_site = os.getenv("SHAREPOINT_SITE_ID")
        sp_list = os.getenv("SHAREPOINT_LIST_ID")

        return Config(
            base_dir=b, 
            archive_dir=a, 
            dry_run=dr,
            sharepoint_enabled=sp_enabled,
            sharepoint_tenant_id=sp_tenant,
            sharepoint_client_id=sp_client,
            sharepoint_client_secret=sp_secret,
            sharepoint_site_id=sp_site,
            sharepoint_list_id=sp_list
            )
