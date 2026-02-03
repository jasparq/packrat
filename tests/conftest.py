import os
from pathlib import Path

# load environment variables from .env.test
env_file = Path(__file__).parent / ".env.test"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ[key] = value