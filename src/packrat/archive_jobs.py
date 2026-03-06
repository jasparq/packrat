from __future__ import annotations

from typing import TypedDict

from packrat.dataverse.client import DataverseClient

ENTITY_SET = "it_archivejobs"
STATUS_FIELD = "it_status"

PREPARING_STATUS_VALUE = 123470000
STAGING_STATUS_VALUE = 123470004
READY_STATUS_VALUE = 123470001
ARCHIVED_STATUS_VALUE = 123470002
FAILED_STATUS_VALUE = 123470003

INDEX_FIELD = "it_archiveindex"
ID_FIELD = "it_archivejobid"
STAGING_PATH_FIELD = "it_stagingpath"
INDEXLOOKUP_FIELD = "it_indexlookup"
INDEXLOOKUP_API_FIELD = f"_{INDEXLOOKUP_FIELD}_value"

ARCHIVEINDEX_ENTITY_SET = "it_archiveindexes"
HASH_FIELD = "it_hash"


class PreparingJob(TypedDict):
    id: str
    archive_index: str


class ReadyJob(TypedDict):
    id: str
    archive_index: str
    staging_path: str
    index_guid: str | None


def fetch_stage_ready_archive_indexes(client: DataverseClient) -> list[PreparingJob]:
    filter_expr = f"{STATUS_FIELD} eq {PREPARING_STATUS_VALUE}"
    select = f"{ID_FIELD},{INDEX_FIELD}"

    data = client.get(entity_set=ENTITY_SET, select=select, filter_expr=filter_expr)
    rows = data.get("value", [])

    jobs: list[PreparingJob] = []
    for row in rows:
        archive_index = row.get(INDEX_FIELD)
        job_id = row.get(ID_FIELD)
        if archive_index and job_id:
            jobs.append({"id": str(job_id), "archive_index": str(archive_index)})
    return jobs


def fetch_ready_for_archive_jobs(client: DataverseClient) -> list[ReadyJob]:
    filter_expr = f"{STATUS_FIELD} eq {READY_STATUS_VALUE}"
    select = f"{ID_FIELD},{INDEX_FIELD},{STAGING_PATH_FIELD},{INDEXLOOKUP_API_FIELD}"

    data = client.get(entity_set=ENTITY_SET, select=select, filter_expr=filter_expr)
    rows = data.get("value", [])

    jobs: list[ReadyJob] = []
    for row in rows:
        job_id = row.get(ID_FIELD)
        archive_index = row.get(INDEX_FIELD)
        staging_path = row.get(STAGING_PATH_FIELD)
        index_guid = row.get(INDEXLOOKUP_API_FIELD)

        if job_id and archive_index and staging_path:
            jobs.append(
                {
                    "id": str(job_id),
                    "archive_index": str(archive_index),
                    "staging_path": str(staging_path),
                    "index_guid": str(index_guid) if index_guid else None,
                }
            )
    return jobs


def update_job_to_staging(client: DataverseClient, job_id: str, staging_path: str) -> None:
    payload = {STATUS_FIELD: STAGING_STATUS_VALUE, STAGING_PATH_FIELD: staging_path}
    client.patch(entity_set=ENTITY_SET, record_id=job_id, payload=payload)


def update_job_to_archived(client: DataverseClient, job_id: str) -> None:
    payload = {STATUS_FIELD: ARCHIVED_STATUS_VALUE}
    client.patch(entity_set=ENTITY_SET, record_id=job_id, payload=payload)


def update_job_to_failed(client: DataverseClient, job_id: str) -> None:
    payload = {STATUS_FIELD: FAILED_STATUS_VALUE}
    client.patch(entity_set=ENTITY_SET, record_id=job_id, payload=payload)


def update_archive_index_hash(client: DataverseClient, index_guid: str, tar_hash: str) -> None:
    payload = {HASH_FIELD: tar_hash}
    client.patch(entity_set=ARCHIVEINDEX_ENTITY_SET, record_id=index_guid, payload=payload)
