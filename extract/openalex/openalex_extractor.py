"""OpenAlex snapshot extractor module."""

from __future__ import annotations

import gzip
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from pymongo import UpdateOne

from extract.base_extractor import BaseExtractor

ENTITY_TYPES = [
    "authors",
    "awards",
    "concepts",
    "continents",
    "countries",
    "domains",
    "fields",
    "funders",
    "institution-types",
    "institutions",
    "keywords",
    "languages",
    "licenses",
    "publishers",
    "sdgs",
    "source-types",
    "sources",
    "subfields",
    "topics",
    "work-types",
    "works",
]

# Default bulk size — works is ~250 M records; keep memory tight
DEFAULT_CHUNK_SIZE = 500
DEFAULT_MAX_WORKERS = 72


class OpenAlexExtractor(BaseExtractor):
    """
    Extractor for OpenAlex S3 snapshot data.

    Loads gzip-compressed NDJSON partition files from a local snapshot directory
    into MongoDB. Supports checkpoint-based resume — already-processed partition
    files are skipped based on lexicographic file path ordering.

    The expected directory structure (produced by `aws s3 sync s3://openalex`) is::

        <snapshot_dir>/
        └── data/
            ├── works/
            │   ├── manifest
            │   └── updated_date=YYYY-MM-DD/
            │       ├── part_000.gz
            │       └── part_001.gz
            ├── authors/
            └── ...

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI (unused when ``client`` is provided).
    db_name : str
        Target database name.
    entity_type : str
        OpenAlex entity type — must be one of ``ENTITY_TYPES``.
    snapshot_dir : str
        Path to the ``openalex-snapshot`` directory.
    client : pymongo.MongoClient, optional
        Existing MongoDB client (for Airflow MongoHook / test injection).
    chunk_size : int, optional
        Number of documents per ``bulk_write`` batch (default: 500).
    max_workers : int, optional
        Number of threads for parallel partition loading (default: 72).
    """

    def __init__(
        self,
        mongodb_uri: str,
        db_name: str,
        entity_type: str,
        snapshot_dir: str = "/storage/raw_data/openalex/openalex-snapshot",
        client: Any = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_workers: int = DEFAULT_MAX_WORKERS,
        drop_db: bool = False,
    ) -> None:
        if entity_type not in ENTITY_TYPES:
            raise ValueError(f"entity_type must be one of {ENTITY_TYPES}, got '{entity_type}'")
        super().__init__(mongodb_uri, db_name, collection_name=entity_type, client=client)
        self.entity_type = entity_type
        self.snapshot_dir = Path(snapshot_dir)
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.create_indexes()

    # ------------------------------------------------------------------
    # Index management
    # ------------------------------------------------------------------

    def create_indexes(self) -> None:
        """Create unique index on the OpenAlex ``id`` field.

        If a non-unique index with the same key already exists (e.g. from a
        previous run before uniqueness was enforced), drop it first so MongoDB
        can create the correct unique index.
        """
        from pymongo.errors import OperationFailure

        self.logger.info("Ensuring index for collection: %s", self.collection_name)
        try:
            self.collection.create_index([("id", 1)], unique=True)
        except OperationFailure as exc:
            if exc.code == 86:  # IndexKeySpecsConflict
                self.logger.warning(
                    "Non-unique 'id_1' index found on '%s'; dropping and recreating as unique.",
                    self.collection_name,
                )
                self.collection.drop_index("id_1")
                self.collection.create_index([("id", 1)], unique=True)
            else:
                raise

    # ------------------------------------------------------------------
    # Partition discovery
    # ------------------------------------------------------------------

    def _iter_partition_files(self) -> list[Path]:
        """Return sorted list of ``.gz`` partition files for this entity type."""
        entity_dir = self.snapshot_dir / "data" / self.entity_type
        if not entity_dir.exists():
            self.logger.warning("Entity directory not found: %s", entity_dir)
            return []
        return sorted(entity_dir.rglob("*.gz"))

    # ------------------------------------------------------------------
    # Single-partition loader
    # ------------------------------------------------------------------

    def _load_partition(self, gz_file: Path) -> dict[str, int]:
        """
        Stream-load a single gzip NDJSON partition into MongoDB.

        Returns
        -------
        dict
            ``{"records": int, "upserted": int, "matched": int, "modified": int, "errors": int}``
        """
        counts: dict[str, int] = {
            "records": 0,
            "upserted": 0,
            "matched": 0,
            "modified": 0,
            "errors": 0,
        }
        ops: list[UpdateOne] = []

        with gzip.open(gz_file, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    doc = json.loads(line)
                except json.JSONDecodeError as exc:
                    self.logger.warning("JSON decode error in %s: %s", gz_file.name, exc)
                    counts["errors"] += 1
                    continue

                doc_id = doc.get("id")
                if not doc_id:
                    counts["errors"] += 1
                    continue

                ops.append(UpdateOne({"id": doc_id}, {"$set": doc}, upsert=True))
                counts["records"] += 1

                if len(ops) >= self.chunk_size:
                    self._flush(ops, counts)

        if ops:
            self._flush(ops, counts)

        return counts

    def _flush(self, ops: list[UpdateOne], counts: dict[str, int]) -> None:
        """Execute a bulk write and update ``counts`` in place."""
        try:
            result = self.collection.bulk_write(ops, ordered=False)
            counts["upserted"] += len(result.upserted_ids)
            counts["matched"] += result.matched_count
            counts["modified"] += result.modified_count
        except Exception as exc:  # noqa: BLE001
            self.logger.error("bulk_write failed: %s", exc)
            counts["errors"] += len(ops)
        finally:
            ops.clear()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self, max_workers: int | None = None, **kwargs: Any) -> dict[str, Any]:
        """
        Load all unprocessed partition files for this entity type into MongoDB.

        Partition files are processed in parallel using a ``ThreadPoolExecutor``.
        The checkpoint stores the **set** of already-processed file paths so that
        any file completed in a previous run is skipped regardless of order.
        After each file completes the checkpoint is updated atomically.

        Parameters
        ----------
        max_workers : int, optional
            Overrides ``self.max_workers`` for this call.

        Returns
        -------
        dict
            Summary: ``files_processed``, ``files_skipped``, ``total_records``,
            ``total_upserted``, ``total_errors``, ``elapsed_seconds``.
        """
        workers = max_workers if max_workers is not None else self.max_workers
        ckpt_key = f"openalex_{self.entity_type}"

        # Checkpoint is a list of already-processed absolute file paths
        raw_ckpt = self.get_checkpoint(ckpt_key)
        processed_files: set[str] = set(raw_ckpt) if isinstance(raw_ckpt, list) else set()

        partition_files = self._iter_partition_files()
        if not partition_files:
            self.logger.warning("No partition files found for '%s'", self.entity_type)
            return {"files_processed": 0, "files_skipped": 0}

        pending = [f for f in partition_files if str(f) not in processed_files]
        skipped = len(partition_files) - len(pending)

        total: dict[str, Any] = {
            "files_processed": 0,
            "files_skipped": skipped,
            "total_records": 0,
            "total_upserted": 0,
            "total_errors": 0,
        }

        if not pending:
            self.logger.info("[%s] All %d files already processed.", self.entity_type, skipped)
            return total

        self.logger.info(
            "[%s] %d files pending, %d already done — using %d workers",
            self.entity_type,
            len(pending),
            skipped,
            workers,
        )

        # Lock protects the shared ``processed_files`` set and the checkpoint write
        lock = threading.Lock()
        t0 = time.monotonic()

        def _process_one(gz_file: Path) -> dict[str, int]:
            self.logger.info("[%s] Loading %s", self.entity_type, gz_file.name)
            counts = self._load_partition(gz_file)
            with lock:
                processed_files.add(str(gz_file))
                self.save_checkpoint(ckpt_key, list(processed_files))
            self.logger.info(
                "[%s] Done %s — records=%d upserted=%d errors=%d",
                self.entity_type,
                gz_file.name,
                counts["records"],
                counts["upserted"],
                counts["errors"],
            )
            return counts

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_process_one, f): f for f in pending}
            for future in as_completed(futures):
                gz_file = futures[future]
                try:
                    counts = future.result()
                    total["files_processed"] += 1
                    total["total_records"] += counts["records"]
                    total["total_upserted"] += counts["upserted"]
                    total["total_errors"] += counts["errors"]
                except Exception as exc:  # noqa: BLE001
                    self.logger.error("[%s] Failed %s: %s", self.entity_type, gz_file.name, exc)
                    total["total_errors"] += 1

        elapsed = round(time.monotonic() - t0, 2)
        total["elapsed_seconds"] = elapsed
        self.logger.info(
            "[%s] Finished — processed=%d skipped=%d records=%d elapsed=%.1fs",
            self.entity_type,
            total["files_processed"],
            total["files_skipped"],
            total["total_records"],
            elapsed,
        )
        return total

    # ------------------------------------------------------------------
    # Unified multi-entity entry point
    # ------------------------------------------------------------------

    @classmethod
    def run_all(
        cls,
        db_name: str,
        snapshot_dir: str,
        client: Any,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> dict[str, Any]:
        """
        Load ALL entity types into MongoDB using a single shared thread pool.

        Unlike calling ``run()`` per entity (which wastes threads when small
        entities finish early), this method builds a unified work queue of every
        pending .gz file across all entities and feeds them to one
        ``ThreadPoolExecutor``.  All 72 (or ``max_workers``) threads stay busy
        until the last file is processed.

        Checkpoints are maintained per entity and are updated atomically after
        each file, so the run can be safely resumed if interrupted.

        Returns
        -------
        dict
            ``files_processed``, ``files_skipped``, ``total_records``,
            ``total_errors``, ``elapsed_seconds``, ``per_entity`` breakdown.
        """
        logger = logging.getLogger("airflow.task.OpenAlexExtractor")

        # One extractor instance per entity (owns its collection + checkpoint refs)
        extractors: dict[str, OpenAlexExtractor] = {
            entity: cls(
                mongodb_uri="",
                db_name=db_name,
                entity_type=entity,
                snapshot_dir=snapshot_dir,
                client=client,
                chunk_size=chunk_size,
            )
            for entity in ENTITY_TYPES
        }

        # Build unified work queue: list of (extractor, gz_file)
        all_work: list[tuple[OpenAlexExtractor, Path]] = []
        skipped_total = 0
        for entity, ext in extractors.items():
            ckpt_key = f"openalex_{entity}"
            raw_ckpt = ext.get_checkpoint(ckpt_key)
            processed: set[str] = set(raw_ckpt) if isinstance(raw_ckpt, list) else set()
            files = ext._iter_partition_files()
            pending = [f for f in files if str(f) not in processed]
            skipped_total += len(files) - len(pending)
            all_work.extend((ext, f) for f in pending)
            logger.info("[%s] %d pending, %d skipped", entity, len(pending), len(files) - len(pending))

        if not all_work:
            logger.info("All %d files already processed across all entities.", skipped_total)
            return {"files_processed": 0, "files_skipped": skipped_total}

        logger.info(
            "Total: %d pending files across %d entities — %d workers",
            len(all_work), len(ENTITY_TYPES), max_workers,
        )

        total_files = len(all_work)

        # Per-entity lock for checkpoint read-modify-write
        locks: dict[str, threading.Lock] = {e: threading.Lock() for e in ENTITY_TYPES}
        entity_stats: dict[str, dict[str, int]] = {
            e: {"processed": 0, "records": 0, "upserted": 0, "errors": 0}
            for e in ENTITY_TYPES
        }
        counter = {"done": 0}
        counter_lock = threading.Lock()

        def _process_one(item: tuple[OpenAlexExtractor, Path]) -> tuple[str, dict[str, int]]:
            ext, gz_file = item
            counts = ext._load_partition(gz_file)
            ckpt_key = f"openalex_{ext.entity_type}"
            with locks[ext.entity_type]:
                raw = ext.get_checkpoint(ckpt_key)
                done: set[str] = set(raw) if isinstance(raw, list) else set()
                done.add(str(gz_file))
                ext.save_checkpoint(ckpt_key, list(done))
            return ext.entity_type, counts

        t0 = time.monotonic()

        # Per-entity totals for pending files (to show n/total per entity)
        entity_total_files: dict[str, int] = {e: 0 for e in ENTITY_TYPES}
        for ext, _ in all_work:
            entity_total_files[ext.entity_type] += 1

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_process_one, item): item for item in all_work}
            for future in as_completed(futures):
                _, gz_file = futures[future]
                try:
                    entity, counts = future.result()
                    entity_stats[entity]["processed"] += 1
                    entity_stats[entity]["records"] += counts.get("records", 0)
                    entity_stats[entity]["upserted"] += counts.get("upserted", 0)
                    entity_stats[entity]["errors"] += counts.get("errors", 0)
                    with counter_lock:
                        counter["done"] += 1
                        n = counter["done"]
                    if n % 50 == 0 or n == total_files:
                        elapsed = time.monotonic() - t0
                        rate = n / elapsed if elapsed > 0 else 0
                        logger.info(
                            "Progress: %d/%d files (%.1f files/s) elapsed=%.0fs",
                            n, total_files, rate, elapsed,
                        )
                        # Per-entity breakdown: files done / total  |  docs
                        lines = []
                        for e in ENTITY_TYPES:
                            s = entity_stats[e]
                            if entity_total_files[e] == 0:
                                continue
                            lines.append(
                                f"  {e}: {s['processed']}/{entity_total_files[e]} files"
                                f" | {s['records']:,} docs"
                                + (f" | {s['errors']} errors" if s["errors"] else "")
                            )
                        logger.info("Per-entity status:\n%s", "\n".join(lines))
                except Exception as exc:  # noqa: BLE001
                    logger.error("Failed %s: %s", gz_file.name, exc)

        elapsed = round(time.monotonic() - t0, 2)
        total_records = sum(s["records"] for s in entity_stats.values())
        total_errors = sum(s["errors"] for s in entity_stats.values())

        summary_lines = []
        for e in ENTITY_TYPES:
            s = entity_stats[e]
            if entity_total_files[e] == 0 and s["records"] == 0:
                continue
            summary_lines.append(
                f"  {e}: {s['processed']} files | {s['records']:,} docs"
                + (f" | {s['errors']} errors" if s["errors"] else "")
            )
        logger.info(
            "Completed — %d files in %.1fs — total records=%d errors=%d\nFinal per-entity:\n%s",
            counter["done"], elapsed, total_records, total_errors, "\n".join(summary_lines),
        )
        return {
            "files_processed": counter["done"],
            "files_skipped": skipped_total,
            "total_records": total_records,
            "total_errors": total_errors,
            "elapsed_seconds": elapsed,
            "per_entity": entity_stats,
        }
