from __future__ import annotations
import os, io, tarfile, shutil, hashlib
from pathlib import Path
import re
from datetime import datetime, timezone
from packrat.config import Config
from packrat.metadata import load_seed_metadata, make_final_metadata

class HashingReader:
    def __init__(self, fp, hasher): self._fp, self._hasher = fp, hasher
    def read(self, size=-1):
        chunk = self._fp.read(size)
        if chunk: self._hasher.update(chunk)
        return chunk
    def close(self): 
        try: self._fp.close()
        except Exception: pass

SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")

def _sanitize(name:str) -> str:
    return SAFE_NAME.sub("_", name).strip("._-") or "archive"

def _bucket_from_meta(meta:dict) -> str:
    t = str((meta or {}).get("type","")).strip().lower()
    return "STUDY" if t == "study" else "NONSTUDY"

def stream_into_tar_with_sha256(tar: tarfile.TarFile, src_abs: Path, arc_rel: str):
    st = src_abs.stat()
    ti = tarfile.TarInfo(name=arc_rel)
    ti.size = st.st_size
    ti.mtime = int(st.st_mtime)
    ti.mode = st.st_mode & 0o777
    hasher = hashlib.sha256()
    with open(src_abs, "rb", buffering=0) as f:
        tar.addfile(ti, fileobj=HashingReader(f, hasher))
    return st.st_size, hasher.hexdigest()

def archive_one_folder_single_pass(cfg: Config, entry_name: str, folder_abs: str | Path) -> str:
    folder = Path(folder_abs)
    meta_path = folder / cfg.meta_name
    seed = load_seed_metadata(str(meta_path))
    if seed.get("ready_to_archive") is not True:
        return f"SKIP   {entry_name}: not ready_to_archive"

    final_tar = cfg.archive_dir / f"{entry_name}.tar.gz"
    temp_tar = final_tar.with_suffix(".tar.gz.part")

    # shard: <archive_root>/<type>/<YYYY><safe_name>.tar.gz
    bucket = _bucket_from_meta(seed)    # "study" or "nonstudy"
    year = datetime.now(timezone.utc).strftime("%Y")
    safe_name = _sanitize(entry_name)
    out_dir = (cfg.archive_dir / bucket / year)
    out_dir.mkdir(parents=True, exist_ok=True)
    final_tar = out_dir / f"{safe_name}.tar.gz"
    temp_tar = final_tar.with_suffix(final_tar.suffix + ".part")

    manifest: list[dict] = []

    if cfg.dry_run:
        return f"DRY    {entry_name}: would write {final_tar}"

    with tarfile.open(temp_tar, "w:gz") as tf:
        for root, _, files in os.walk(folder):
            for name in files:
                if name == cfg.meta_name:
                    continue
                src_abs = Path(root) / name
                rel = src_abs.relative_to(folder).as_posix()
                arc_rel = f"{entry_name}/{rel}"
                size, sha = stream_into_tar_with_sha256(tf, src_abs, arc_rel)
                manifest.append({"path": rel, "size_bytes": size, "sha256": sha})

        manifest.sort(key=lambda x: x["path"])
        meta_bytes = make_final_metadata(seed, manifest)
        ti = tarfile.TarInfo(name=f"{entry_name}/{cfg.meta_name}")
        ti.size = len(meta_bytes)
        ti.mtime = int(datetime.now(timezone.utc).timestamp())
        ti.mode = 0o644
        tf.addfile(ti, fileobj=io.BytesIO(meta_bytes))

    os.replace(temp_tar, final_tar)
    shutil.rmtree(folder)
    return f"OK     {entry_name}: -> {final_tar} ({len(manifest)} files)"

def find_ready_folders(cfg: Config) -> list[tuple[str, str]]:
    ready = []
    for entry in sorted(os.listdir(cfg.base_dir)):
        # don’t eat the vault
        if entry == cfg.archive_dir.name:
            continue
        folder_abs = Path(cfg.base_dir) / entry
        if not folder_abs.is_dir():
            continue
        if (folder_abs / cfg.meta_name).is_file():
            ready.append((entry, str(folder_abs)))
    return ready
