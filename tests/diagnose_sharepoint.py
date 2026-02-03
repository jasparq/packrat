"""Diagnose SharePoint list field names."""
import os
import json
from pathlib import Path
from packrat.sharepoint.graph_client import GraphClient

# Load environment variables from .env.test
env_file = Path(__file__).parent / ".env.test"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
client_id = os.getenv("SHAREPOINT_CLIENT_ID")
client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
site_id = os.getenv("SHAREPOINT_SITE_ID")
list_id = os.getenv("SHAREPOINT_LIST_ID")

graph_client = GraphClient(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret,
)

# Get list columns/fields
endpoint = f"/sites/{site_id}/lists/{list_id}/columns"
response = graph_client.request("GET",endpoint)

print("\n Raw Response:")
print("=" * 60)
print(json.dumps(response,indent=2))