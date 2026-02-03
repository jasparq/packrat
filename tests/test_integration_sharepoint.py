"""Integration tests for SharePoint archive indexing."""
import os
import pytest
from datetime import datetime, timezone
from packrat.sharepoint.graph_client import GraphClient
from packrat.sharepoint.archive_index import ArchiveIndex

@pytest.fixture
def sharepoint_config():
    """Load Sharepoint config from environment variables."""
    tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
    client_id = os.getenv("SHAREPOINT_CLIENT_ID")
    client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
    site_id = os.getenv("SHAREPOINT_SITE_ID")
    list_id = os.getenv("SHAREPOINT_LIST_ID")

    assert tenant_id, "SHAREPOINT_TENANT_ID not set"
    assert client_id, "SHAREPOINT_CLIENT_ID not set"
    assert client_secret, "SHAREPOINT_CLIENT_SECRET not set"
    assert site_id, "SHAREPOINT_SITE_ID not set"
    assert list_id, "SHAREPOINT_LIST_ID not set"

    return {
        "tenant_id": tenant_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "site_id": site_id,
        "list_id": list_id,
    }

@pytest.fixture
def archive_index(sharepoint_config):
    """Create ArchiveIndex with real SharePoint connection."""
    graph_client = GraphClient(
        tenant_id=sharepoint_config["tenant_id"],
        client_id=sharepoint_config["client_id"],
        client_secret=sharepoint_config["client_secret"],
    )
    return ArchiveIndex(
        graph_client=graph_client,
        site_id=sharepoint_config["site_id"],
        list_id=sharepoint_config["list_id"],
    )

def test_create_and_retrieve_archive_item(archive_index):
    """Test creating an archive item and retrieving it."""
    timestamp =datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    test_name = f"test_archive_{timestamp}"
    test_tar_path = f"/tmp/{test_name}.tar.gz"
    test_count = 42
    test_hash = "dummyhashvalue1234567890abcdef"

    print(f"\n1. Creating archive item '{test_name}'")
    response = archive_index.index_archive(
        archive_name=test_name,
        tar_path=test_tar_path,
        manifest_count=test_count,
        tar_hash=test_hash,
    )

    item_id = response.get("id")
    assert item_id, "Failed to get item ID from response"
    print(f" ✓ Created archive item with ID: {item_id}")

    # Retrieve the item
    print(f"2. Retrieving archive item ID {item_id}")
    retrieved = archive_index.get_list_item(item_id)
    assert retrieved["id"] == item_id
    print(f" ✓ Retrieved archive item successfully")

def test_search_archives(archive_index):
    """Test searching for archives by name."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    test_name = f"test_search_{timestamp}"

    print(f"\n1. Creating archive for search: {test_name}")
    archive_index.index_archive(
        archive_name=test_name,
        tar_path=f"/tmp/{test_name}.tar.gz",
        manifest_count=10,
        tar_hash="dummyhashvalue",
    )
    print(f" ✓ Created archive item")

    # Search for it
    print(f"2. Searching for archive by name: {test_name}")
    results = archive_index.search_archives(test_name)
    items = results.get("value",[])

    assert len(items) > 0, f"No items found for {test_name}"
    found = any(item["fields"]["Title"] == test_name for item in items)
    assert found, f"Archive {test_name} not found in search results"
    print(f" ✓ Found {len(items)} matching items")

    return items[0]["id"]

def test_delete_archive(archive_index):
    """Test deleting an archive item."""
    # Create an archive
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    test_name = f"test_delete_{timestamp}"

    print(f"\n1. Creating archive to delete: {test_name}")
    response = archive_index.index_archive(
        archive_name=test_name,
        tar_path=f"/tmp/{test_name}.tar.gz",
        manifest_count=5,
        tar_hash="dummyhashvalue",
    )
    item_id = response.get("id")
    print(f" ✓ Created archive item with ID: {item_id}")

    # Delete the item
    print(f"2. Deleting archive item ID {item_id}")
    archive_index.delete_list_item(item_id)
    print(f" ✓ Deleted archive item")
