from __future__ import annotations

import contextlib
import io
import os
import pickle
import re
import time
from datetime import datetime
from typing import Any

import pandas as pd
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pymongo import ReplaceOne

from extract.base_extractor import BaseExtractor

FOLDER_MIME = "application/vnd.google-apps.folder"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
XLS_MIME = "application/vnd.ms-excel"


class StaffExtractor(BaseExtractor):
    """
    Extracts raw staff matrices from Google Drive and loads them into MongoDB.

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI.
    db_name : str
        Database name.
    collection_name : str, optional
        Collection name (default: "staff").
    client : pymongo.MongoClient, optional
        Existing MongoDB client instance.
    drive_root_folder_id : str
        Google Drive folder ID where institution subfolders live.
    google_token_pickle : str
        Path to the pickle containing Google Drive API credentials.
    cache_dir : str, optional
        Local directory used to cache downloads (default: "/opt/airflow/cache/staff").
    keep_only_latest_per_institution : bool, optional
        If True, deletes prior docs for the institution before loading the latest Excel (default: True).
    """

    def __init__(
        self,
        mongodb_uri: str,
        db_name: str,
        drive_root_folder_id: str,
        google_token_pickle: str,
        collection_name: str = "staff",
        client: Any = None,
        cache_dir: str = "/opt/airflow/cache/staff",
        keep_only_latest_per_institution: bool = True,
    ):
        super().__init__(mongodb_uri, db_name, collection_name, client=client)

        self.drive_root_folder_id = drive_root_folder_id
        self.google_token_pickle = google_token_pickle
        self.cache_dir = cache_dir
        self.keep_only_latest_per_institution = keep_only_latest_per_institution

        os.makedirs(self.cache_dir, exist_ok=True)

        self._drive_service = None  # lazy init
        self.create_indexes()

    # Mongo
    def create_indexes(self) -> None:
        """
        Ensure minimal indexes to support faster queries and upserts.
        """
        self.logger.info("Ensuring indexes for staff collection...")
        # _id is already unique; add a few practical indexes:
        self.collection.create_index("institution_id")
        self.collection.create_index("drive_file_id")
        self.collection.create_index([("institution_id", 1), ("drive_file_id", 1)])
        self.collection.create_index("extracted_at")

    # Google Drive helpers
    def _get_drive_service(self):
        """
        Build a Google Drive v3 service from credentials stored in a pickle.
        """
        if self._drive_service is not None:
            return self._drive_service

        with open(self.google_token_pickle, "rb") as f:
            creds = pickle.load(f)

        # Refresh credentials if needed
        if hasattr(creds, "expired") and creds.expired and getattr(creds, "refresh_token", None):
            creds.refresh(Request())

        self._drive_service = build("drive", "v3", credentials=creds)
        return self._drive_service

    def _list_files(
        self,
        q: str,
        fields: str = "nextPageToken, files(id, name, mimeType, modifiedTime, size)",
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Generic Drive list() helper with pagination.
        """
        service = self._get_drive_service()
        out: list[dict[str, Any]] = []
        page_token = None

        while True:
            resp = (
                service.files()
                .list(
                    q=q,
                    pageSize=page_size,
                    fields=fields,
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    spaces="drive",
                )
                .execute()
            )

            out.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        return out

    def _list_institution_folders(self) -> list[dict[str, Any]]:
        """
        list all first-level institution folders under the root folder.
        """
        q = (
            f"'{self.drive_root_folder_id}' in parents "
            f"and mimeType='{FOLDER_MIME}' and trashed=false"
        )
        return self._list_files(q=q)

    def _parse_institution_folder_name(self, folder_name: str) -> tuple[str | None, str]:
        """
        Expected folder naming format: RORID_INSTITUTION_NAME

        Returns
        -------
        (ror_id, institution_name)
            ror_id can be None if the format is invalid.
        """
        if "_" not in folder_name:
            return None, folder_name

        ror_id, rest = folder_name.split("_", 1)
        return ror_id.strip(), rest.strip()

    def _list_staff_excels(self, folder_id: str) -> list[dict[str, Any]]:
        """
        list Excel files (xlsx/xls) in a given institution folder.

        If any file names start with "staff_", prefer those.
        """
        q = (
            f"'{folder_id}' in parents and trashed=false and ("
            f"mimeType='{XLSX_MIME}' or mimeType='{XLS_MIME}'"
            f")"
        )
        files = self._list_files(q=q)

        # (Optional) prioritize those named staff_...
        staff_like = [f for f in files if (f.get("name") or "").lower().startswith("staff_")]
        return staff_like if staff_like else files

    @staticmethod
    def _pick_latest_by_modified_time(files: list[dict[str, Any]]) -> dict[str, Any] | None:
        """
        Pick the file with the latest modifiedTime (Drive metadata).
        """
        if not files:
            return None

        def key(f: dict[str, Any]) -> str:
            # ISO 8601 timestamps are lexicographically comparable
            return f.get("modifiedTime") or ""

        return max(files, key=key)

    def _download_file_to_cache(self, file_id: str, file_name: str) -> str:
        """
        Download a Drive file into the local cache (overwrites if it exists).
        """
        service = self._get_drive_service()

        safe_name = re.sub(r"[^\w\-.]+", "_", file_name)
        local_path = os.path.join(self.cache_dir, f"{file_id}__{safe_name}")

        request = service.files().get_media(fileId=file_id)

        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        return local_path

    # Excel -> Mongo
    def _read_excel_as_records(self, local_path: str) -> list[dict[str, Any]]:
        """
        Read an Excel file into a list of row dicts (raw, no normalization).
        """
        df = pd.read_excel(local_path, dtype=str)  # keep codes/IDs as strings
        # Mongo-friendly: NaN -> None
        df = df.where(pd.notnull(df), None)
        return df.to_dict(orient="records")

    def _already_loaded(
        self, institution_id: str, drive_file_id: str, drive_modified_time: str
    ) -> bool:
        """
        Avoid reprocessing if we already loaded at least one doc for that fileId+modifiedTime.

        If a file is overwritten in Drive, modifiedTime changes, and we will reprocess.
        """
        return (
            self.collection.find_one(
                {
                    "institution_id": institution_id,
                    "drive_file_id": drive_file_id,
                    "drive_modified_time": drive_modified_time,
                },
                {"_id": 1},
            )
            is not None
        )

    def _replace_institution_data(
        self,
        institution_id: str,
        institution_name: str,
        folder_id: str,
        file_meta: dict[str, Any],
        records: list[dict[str, Any]],
        chunk_size: int = 2000,
    ) -> None:
        """
        Delete and reload the institution's staff data (raw extract only).
        """
        drive_file_id = file_meta["id"]
        drive_file_name = file_meta.get("name", "")
        drive_modified_time = file_meta.get("modifiedTime", "")
        drive_size = file_meta.get("size")

        if self.keep_only_latest_per_institution:
            self.logger.info(f"Deleting previous staff docs for institution {institution_id}...")
            self.collection.delete_many({"institution_id": institution_id})

        extracted_at = datetime.now(datetime.UTC).isoformat()

        ops: list[ReplaceOne] = []
        for i, row in enumerate(records):
            # Raw document containing the Excel columns as-is.
            doc = dict(row)
            doc["_id"] = f"{institution_id}:{drive_file_id}:{i}"
            doc["institution_id"] = institution_id
            doc["institution_name"] = institution_name
            doc["drive_folder_id"] = folder_id
            doc["drive_file_id"] = drive_file_id
            doc["drive_file_name"] = drive_file_name
            doc["drive_modified_time"] = drive_modified_time
            doc["drive_file_size"] = drive_size
            doc["row_number"] = i
            doc["extracted_at"] = extracted_at

            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))

        total = len(ops)
        if total == 0:
            self.logger.warning(f"No rows found in {drive_file_name} for {institution_id}.")
            return

        self.logger.info(f"Writing {total} staff docs for {institution_id} (bulk upsert)...")
        for start in range(0, total, chunk_size):
            chunk = ops[start : start + chunk_size]
            self.collection.bulk_write(chunk, ordered=False)

    # Public API
    def process_all_institutions(self, force: bool = False) -> dict[str, Any]:
        """
        Process all institution subfolders (one per institution).

        Parameters
        ----------
        force : bool
            If True, reprocess even if the latest file was already loaded.

        Returns
        -------
        dict
            Simple processing metrics.
        """
        folders = self._list_institution_folders()
        self.logger.info(f"Found {len(folders)} institution folders under root.")

        stats = {"folders": len(folders), "processed": 0, "skipped": 0, "empty": 0, "errors": 0}

        for folder in folders:
            folder_id = folder["id"]
            folder_name = folder.get("name", "")
            ror_id, inst_name = self._parse_institution_folder_name(folder_name)

            if not ror_id:
                self.logger.warning(f"Skipping folder with unexpected name format: {folder_name}")
                stats["skipped"] += 1
                continue

            try:
                excel_files = self._list_staff_excels(folder_id)
                latest = self._pick_latest_by_modified_time(excel_files)
                if not latest:
                    self.logger.warning(f"No excel files found in {folder_name}")
                    stats["empty"] += 1
                    continue

                drive_file_id = latest["id"]
                drive_modified_time = latest.get("modifiedTime", "")

                if not force and self._already_loaded(ror_id, drive_file_id, drive_modified_time):
                    self.logger.info(
                        f"[SKIP] {ror_id} latest file already loaded: {latest.get('name')}"
                    )
                    stats["skipped"] += 1
                    continue

                self.logger.info(
                    f"[{ror_id}] Latest staff file: {latest.get('name')} ({drive_modified_time})"
                )

                local_path = self._download_file_to_cache(
                    drive_file_id, latest.get("name", "staff.xlsx")
                )
                records = self._read_excel_as_records(local_path)

                self._replace_institution_data(
                    institution_id=ror_id,
                    institution_name=inst_name,
                    folder_id=folder_id,
                    file_meta=latest,
                    records=records,
                )

                stats["processed"] += 1
                # Optional checkpoint
                with contextlib.suppress(Exception):
                    self.save_checkpoint(
                        f"staff_{ror_id}",
                        {
                            "drive_file_id": drive_file_id,
                            "drive_modified_time": drive_modified_time,
                        },
                    )

                # Small courtesy delay
                time.sleep(0.1)

            except Exception as e:
                self.logger.error(f"Error processing folder {folder_name}: {e}")
                stats["errors"] += 1

        return stats

    def run(self, force: bool = False) -> None:
        """
        Entry point, consistent with other extractors.
        """
        self.process_all_institutions(force=force)
