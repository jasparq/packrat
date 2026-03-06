import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

from filelock import FileLock, Timeout

from packrat.archive_jobs import (
    fetch_ready_for_archive_jobs,
    fetch_stage_ready_archive_indexes,
    update_archive_index_hash,
    update_job_to_archived,
    update_job_to_failed,
    update_job_to_staging,
)
from packrat.archiver import archive_folder_dataverse, ensure_staging_folders
from packrat.config import Config
from packrat.dataverse.client import DataverseClient
from packrat.logging_setup import setup_logging


def load_env_file(env_path: str) -> None:
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key, value)
    except Exception as e:
        print(f"Warning: could not load env file {env_path}: {e}")


env_path = os.environ.get("PACKRAT_ENV_FILE", "/home/packrat/tests/.env.test")
load_env_file(env_path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir")
    ap.add_argument("--archive-dir")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--lock-path", default=None)
    args = ap.parse_args()

    setup_logging(args.log_level)
    cfg = Config.from_env(args.base_dir, args.archive_dir, args.dry_run)
    if args.lock_path:
        cfg = cfg.__class__(**{**cfg.__dict__, "lock_path": Path(args.lock_path)})

    lock = FileLock(str(cfg.lock_path))
    try:
        lock.acquire(timeout=0)
    except Timeout:
        logging.info("Another run is active; exiting.")
        return 0

    try:
        if not cfg.dataverse_enabled:
            logging.info("Dataverse integration is disabled; nothing to do.")
            return 0

        dv_url = cfg.dataverse_url
        dv_tenant = cfg.dataverse_tenant_id
        dv_client = cfg.dataverse_client_id
        dv_secret = cfg.dataverse_client_secret

        if dv_url is None or dv_tenant is None or dv_client is None or dv_secret is None:
            raise RuntimeError(
                "Dataverse is enabled but required settings are missing: "
                "DATAVERSE_URL, DATAVERSE_TENANT_ID, DATAVERSE_CLIENT_ID, DATAVERSE_CLIENT_SECRET"
            )

        client = DataverseClient(
            url=dv_url,
            tenant_id=dv_tenant,
            client_id=dv_client,
            client_secret=dv_secret,
        )

        # 1) PreparingIndex -> create staging folders -> Staging
        prep_jobs = fetch_stage_ready_archive_indexes(client)
        created, skipped = ensure_staging_folders(cfg, prep_jobs)
        for item in created:
            update_job_to_staging(client, item["id"], item["path"])
        logging.info("Dataverse staging: %d created, %d skipped", len(created), skipped)

        # 2) ReadyForArchive -> archive staging folder -> Archived/Failed
        ready_jobs = fetch_ready_for_archive_jobs(client)
        logging.info("ReadyForArchive jobs found: %d", len(ready_jobs))

        for job in ready_jobs:
            temp_tar = None
            try:
                staging_path = job["staging_path"]
                entry_name = job["archive_index"] or Path(staging_path).name
                temp_tar, final_tar, tar_hash = archive_folder_dataverse(cfg, entry_name, staging_path)
                logging.info("Archived %s to %s", entry_name, final_tar)
                logging.info("Tar hash for %s: %s", entry_name, tar_hash)

                index_guid = job.get("index_guid")
                if not index_guid:
                    raise RuntimeError("Archive index lookup is missing on ready-for-archive job")

                update_archive_index_hash(client, index_guid, tar_hash)

                if not cfg.dry_run:
                    os.replace(temp_tar, final_tar)
                    shutil.rmtree(staging_path)

                update_job_to_archived(client, job["id"])
            except Exception:
                logging.exception("ReadyForArchive failed: %s", job.get("staging_path"))
                try:
                    if temp_tar and Path(temp_tar).exists():
                        Path(temp_tar).unlink()
                except Exception:
                    logging.exception("Failed to remove temp tar: %s", temp_tar)

                update_job_to_failed(client, job["id"])

        return 0
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
