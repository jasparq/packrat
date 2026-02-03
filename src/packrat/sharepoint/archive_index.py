"""Packrat archive indexing in SharePoint."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from .graph_client import GraphClient

logger = logging.getLogger(__name__)

class ArchiveIndex:
    """Manages archive indexing in SharePoint."""

    def __init__(
        self,
        graph_client: GraphClient,
        site_id: str,
        list_id: str,
    ):
        """
        Initialize ArchiveIndex.

        Args:
            graph_client: Authenticated GraphClient instance
            site_id: SharePoint site ID
            list_id: SharePoint list ID for archives
        """
        self.client = graph_client
        self.site_id = site_id
        self.list_id = list_id

    def create_list_item(self, fields: dict) -> dict:
        """Create a new item in the archive list."""
        endpoint = f"/sites/{self.site_id}/lists/{self.list_id}/items"
        payload = {"fields":fields}

        logger.info(f"Creating archive list item")
        result = self.client.request("POST", endpoint, json=payload)
        return result
    
    def get_list_items(self, filter_query: str | None = None, expand: str | None = None) -> dict:
        """Get items from archive list with optional filter."""
        endpoint = f"/sites/{self.site_id}/lists/{self.list_id}/items"
        params = {}
        if filter_query:
            params["$filter"] = filter_query
        if expand:
            params["$expand"] = expand

        logger.info(f"Fetching archive list items")
        return self.client.request("GET",endpoint,params=params)        
    
    def get_list_item(self, item_id: str) -> dict:
        """Get a single archive item by ID."""
        endpoint = f"/sites/{self.site_id}/lists/{self.list_id}/items/{item_id}"
        return self.client.request("GET", endpoint)
    
    def update_list_item(self, item_id: str, fields: dict) -> dict:
        """Update an archive item."""
        endpoint = f"/sites/{self.site_id}/lists/{self.list_id}/items{item_id}"
        payload = {"fields":fields}

        logger.info(f"Updating archive item {item_id}")
        return self.client.request("PATCH", endpoint, json=payload)
    
    def delete_list_item(self, item_id: str) -> None:
        """Delete an archive item."""
        endpoint = f"/sites/{self.site_id}/lists/{self.list_id}/items/{item_id}"

        logger.info(f"Deleting archive item {item_id}")
        self.client.request("DELETE", endpoint)

    def index_archive(
            self,
            archive_name: str,
            tar_path: str,
            manifest_count: int,
            tar_hash: str,
            archived_by: str = "packrat",
    ) -> dict:
        """Add archive to SharePoint index (packrat-specific wrapper.)"""
        fields = {
            "Title": archive_name,
            "ArchivePath": tar_path,
            "FileCount": manifest_count,
            "Hash": tar_hash,
            "ArchivedBy": archived_by,
            "ArchivedDate": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(f"Indexing archive '{archive_name}' in SharePoint")
        result = self.create_list_item(fields)
        logger.info(f"Successfully indexed archive '{archive_name}'")
        return result
    
    def search_archives(self, archive_name: str) -> dict:
        """Search for archives by name."""
        filter_query = f"fields/Title eq '{archive_name}'"
        logger.info(f"Searching for archives with name: {archive_name}")
        return self.get_list_items(filter_query=filter_query, expand="fields")
    
    def upload_attachment(self, item_id: str, file_path: str) -> dict:
        """Upload a file as attachment to a list item."""
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_name = file_path.name
        endpoint = f"/sites/{self.site_id}/lists/{self.list_id}/items/{item_id}/attachments/createUploadSession"

        with open(file_path, "rb") as f:
            files = {"file": (file_name, f)}
            logger.info(f"Uploading attachment '{file_name}' to item {item_id}")
            result = self.client.request_multipart("POST", endpoint, files=files)
            logger.info(f"Successfully uploaded attachment '{file_name}'")
            return result
