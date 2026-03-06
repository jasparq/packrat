#!/usr/bin/env python3
import os
import sys
import argparse
import requests


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
    except FileNotFoundError:
        return
    except Exception as e:
        print(f"Warning: could not load env file {env_path}: {e}")


def get_env(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def require(value: str | None, name: str) -> str:
    if not value:
        raise SystemExit(f"Missing required setting: {name}")
    return value


def fetch_token(*, tenant_id: str, client_id: str, client_secret: str, org_url: str) -> str:
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": f"{org_url}/.default",
    }
    resp = requests.post(token_url, data=token_payload, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise SystemExit("Token response did not include access_token.")
    return token


def whoami(*, org_url: str, access_token: str) -> dict:
    whoami_url = f"{org_url}/api/data/v9.2/WhoAmI"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }
    resp = requests.get(whoami_url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

def get_system_user(*, org_url: str, access_token: str, user_id: str) -> dict:
    user_url = f"{org_url}/api/data/v9.2/systemusers({user_id})?$select=fullname,applicationid,systemuserid,internalemailaddress"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }
    resp = requests.get(user_url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

def main() -> int:
    ap = argparse.ArgumentParser(description="Dataverse WhoAmI test")
    ap.add_argument("--env-file", default="/home/packrat/tests/.env.test")
    ap.add_argument("--url", help="Dataverse org URL, e.g. https://org.crm.dynamics.com")
    ap.add_argument("--tenant-id")
    ap.add_argument("--client-id")
    ap.add_argument("--client-secret")
    args = ap.parse_args()

    load_env_file(args.env_file)

    org_url = args.url or get_env("DATAVERSE_URL")
    tenant_id = args.tenant_id or get_env("DATAVERSE_TENANT_ID")
    client_id = args.client_id or get_env("DATAVERSE_CLIENT_ID")
    client_secret = args.client_secret or get_env("DATAVERSE_CLIENT_SECRET")

    org_url = require(org_url, "DATAVERSE_URL")
    tenant_id = require(tenant_id, "DATAVERSE_TENANT_ID")
    client_id = require(client_id, "DATAVERSE_CLIENT_ID")
    client_secret = require(client_secret, "DATAVERSE_CLIENT_SECRET")

    token = fetch_token(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        org_url=org_url,
    )
    result = whoami(org_url=org_url, access_token=token)
    print(result)

    user_id = result.get("UserId")
    if user_id:
        user_info = get_system_user(org_url=org_url, access_token=token, user_id=user_id)
        print(user_info)

    return 0


if __name__ == "__main__":
    sys.exit(main())
