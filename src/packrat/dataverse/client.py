from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)


class DataverseClient:
    def __init__(self, *, url: str, tenant_id: str, client_id: str, client_secret: str):
        self.url = url.rstrip("/")
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token: str | None = None

    def _get_access_token(self) -> str:
        if self._access_token:
            return self._access_token

        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": f"{self.url}/.default",
        }
        resp = requests.post(token_url, data=payload, timeout=30)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            raise RuntimeError("Token response did not include access_token.")
        self._access_token = token
        return token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }

    def get(self, *, entity_set: str, select: str | None = None, filter_expr: str | None = None) -> dict:
        url = f"{self.url}/api/data/v9.2/{entity_set}"
        params: dict[str, str] = {}
        if select:
            params["$select"] = select
        if filter_expr:
            params["$filter"] = filter_expr

        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
        if not resp.ok:
            logger.error("Dataverse error body: %s", resp.text)
        resp.raise_for_status()
        return resp.json()

    def patch(self, *, entity_set: str, record_id: str, payload: dict) -> None:
        url = f"{self.url}/api/data/v9.2/{entity_set}({record_id})"
        resp = requests.patch(url, headers=self._headers(), json=payload, timeout=30)
        if not resp.ok:
            logger.error("Dataverse error body: %s", resp.text)
        resp.raise_for_status()

    def post(self, *, entity_set: str, payload: dict) -> dict:
        url = f"{self.url}/api/data/v9.2/{entity_set}"
        resp = requests.post(url, headers=self._headers(), json=payload, timeout=30)
        if not resp.ok:
            logger.error("Dataverse error body: %s", resp.text)
        resp.raise_for_status()
        return resp.json() if resp.text else {}
