import json
from datetime import datetime, timezone

def load_seed_metadata(meta_path: str) -> dict:
    try:
        with open(meta_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def build_base_metadata(seed: dict) -> dict:
    m = dict(seed or {})
    m.setdefault("description", "No description provided.")
    m["timestamp"] = datetime.now(timezone.utc).isoformat()
    m["ready_to_archive"] = True
    return m

def make_final_metadata(seed: dict, manifest: list[dict]) -> bytes:
    base = build_base_metadata(seed)
    base["manifest"] = manifest
    return json.dumps(base, indent=2).encode("utf-8")
