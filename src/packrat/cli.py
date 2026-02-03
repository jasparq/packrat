import argparse, logging, sys, os
from filelock import FileLock, Timeout
from pathlib import Path
from packrat.config import Config
from packrat.logging_setup import setup_logging
from packrat.archiver import find_ready_folders, archive_one_folder_single_pass

def load_env_file(env_path):
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

env_path = os.environ.get("PACKRAT_ENV_FILE","/home/packrat/tests/.env.test")
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
        lock.acquire(timeout=0)   # fail fast if overlapping
    except Timeout:
        logging.info("Another run is active; exiting.")
        return 0

    try:
        jobs = find_ready_folders(cfg)
        if not jobs:
            logging.info("Nothing to do.")
            return 0

        ok = 0
        for entry_name, folder_abs in jobs:
            try:
                msg = archive_one_folder_single_pass(cfg, entry_name, folder_abs)
                logging.info(msg)
                ok += 1
            except Exception as e:
                logging.exception("Job failed: %s", entry_name)
        return 0 if ok == len(jobs) else 2
    finally:
        lock.release()

if __name__ == "__main__":
    sys.exit(main())
