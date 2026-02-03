"""Microsoft Graph API client for SharePoint operations."""
from __future__ import annotations

import logging
import requests
from typing import Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class GraphClient:
    """Client for interacting with Microsoft Graph API for SharePoint operations."""

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ):
        """
        Initialize GraphClient with Azure app credentials.

        Args:
            tenant_id (str): Azure tenant ID.
            client_id (str): Azure client ID.
            client_secret (str): Azure client secret.
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: Optional[str] = None
        self.base_url = "https://graph.microsoft.com/v1.0"

    def _get_access_token(self) -> str:
        """Obtain access token from Azure AD."""
        if self.access_token:
            return self.access_token
        
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }

        try:
            response = requests.post(url, data=data, timeout=10)
            response.raise_for_status()
            token: str = response.json()["access_token"]
            self.access_token = token
            return token
        except requests.RequestException as e:
            logger.error(f"Failed to get access token: {e}")
            raise
    
    def _get_headers(self) -> dict:
        """Get headers with authorization token."""
        token = self._get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    
    def request(
        self,
        method: str,
        endpoint: str,
        json=None,
        params=None,
    ):
        """
        Make authenticated HTTP request to Microsoft Graph API.

        Args:
            method: HTTP method (GET, POST, PATH, DELETE)
            endpoint: API endpoint path (e.g., "/sites/{id}/lists/{id}/items")
            json: Request body (for POST/PATCH)
            params: Query parameters

        Returns:
            Response JSON
        """
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.request(
                method,
                url,
                json=json,
                params=params,
                headers=self._get_headers(),
                timeout=10,
            )
            response.raise_for_status()
            return response.json() if response.text else None
        except requests.RequestException as e:
            try:
                error_detail = response.json()
            except:
                error_detail =response.text
            logger.error(f"Microsoft Graph API requests failed: ({method} {endpoint}): {e}")
            logger.error(f"Response: {error_detail}")
            raise
    
    def request_multipart(
        self,
        method: str,
        endpoint: str,
        files=None,
    ):
        """
        Make authenticated multipart HTTP request (for file uploads).
        
        Args:
            method: HTTP method (POST, PATCH, DELETE)
            endpoint: API endpoint path
            files: Files dict for multipart upload
        
        Returns:
            Response JSON
        """
        url = f"{self.base_url}{endpoint}"

        try:
            # For multipart, don't set Content-Type header (requests will set it with boundary)
            headers = {"Authorization": f"Bearer {self._get_access_token()}"}

            response = requests.request(
                method,
                url,
                files=files,
                headers=headers,
                timeout=10,
            )
            response.raise_for_status()
            return response.json() if response.text else None
        except requests.RequestException as e:
            logger.error(f"Microsoft Graph API multipart request failed ({method} {endpoint}): {e}")
            try:
                error_detail = response.json()
            except:
                error_detail = response.text
            logger.error(f"Response: {error_detail}")
            raise