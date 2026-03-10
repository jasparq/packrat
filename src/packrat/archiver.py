from __future__ import annotations

import hashlib
import os
import re
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence
from packrat.archive_jobs import PreparingJob

from packrat.config import Config

SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitize(name: str) -> str:
    return SAFE_NAME.sub("_", name).strip("._-") or "archive"


def _bucket_from_archive_index(archive_index: str) -> str:
    return "STUDY" if archive_index.startswith("S-") else "NONSTUDY"


def ensure_staging_folders(
    cfg: Config,
    jobs: Sequence[PreparingJob],
) -> tuple[list[dict[str, str]], int, int]:
    staged: list[dict[str, str]] = []
    created = 0
    skipped = 0

    for job in jobs:
        safe = _sanitize(str(job["archive_index"]))
        folder = cfg.base_dir / safe

        if folder.exists():
            skipped += 1
            staged.append({"id": job["id"], "path": str(folder)})
            continue

        folder.mkdir(parents=True, exist_ok=False)
        created += 1
        staged.append({"id": job["id"], "path": str(folder)})

    return staged, created, skipped


def archive_folder_dataverse(
    cfg: Config,
    entry_name: str,
    folder_abs: str | Path,
) -> tuple[Path, Path, str]:
    folder = Path(folder_abs)

    bucket = _bucket_from_archive_index(entry_name)
    year = datetime.now(timezone.utc).strftime("%Y")
    safe_name = _sanitize(entry_name)
    out_dir = cfg.archive_dir / bucket / year
    out_dir.mkdir(parents=True, exist_ok=True)

    final_tar = out_dir / f"{safe_name}.tar.gz"
    temp_tar = final_tar.with_suffix(final_tar.suffix + ".part")

    if cfg.dry_run:
        return temp_tar, final_tar, ""

    with tarfile.open(temp_tar, "w:gz") as tf:
        for root, _, files in os.walk(folder):
            for name in files:
                src_abs = Path(root) / name
                rel = src_abs.relative_to(folder).as_posix()
                arc_rel = f"{entry_name}/{rel}"
                tf.add(src_abs, arcname=arc_rel, recursive=False)

    sha256_hash = hashlib.sha256()
    with open(temp_tar, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)

    return temp_tar, final_tar, sha256_hash.hexdigest()
