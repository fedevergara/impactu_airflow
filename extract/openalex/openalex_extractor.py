"""OpenAlex snapshot extractor module."""

from __future__ import annotations

import gzip
import json
import logging
import multiprocessing
import multiprocessing.pool
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pymongo
from pymongo import UpdateOne

from extract.base_extractor import BaseExtractor

# ---------------------------------------------------------------------------
# Non-daemon pool — bypass Airflow's daemon worker restriction
# ---------------------------------------------------------------------------
# Python's ``Process.start()`` checks ``_current_process._config['daemon']``
# (the *parent* process flag), not the child's property.  Overriding the
# child's daemon property has no effect.
#
# Fix: patch _current_process._config to remove 'daemon' for the duration
# of each worker start() call, then restore it.  All Pool bookkeeping
# (join, terminate) still works correctly because workers are real processes.
# ---------------------------------------------------------------------------


class _NonDaemonProcess(multiprocessing.Process):
    """Process that temporarily unsets the parent's daemon flag on start().

    This allows Pool to spawn workers even when the parent (Airflow task
    runner) is itself a daemon process.
    """

    def start(self) -> None:
        import multiprocessing

        cfg = multiprocessing.current_process()._config  # type: ignore[attr-defined]
        was = cfg.pop("daemon", None)
        try:
            super().start()
        finally:
            if was is not None:
                cfg["daemon"] = was


class _NoDaemonPool(multiprocessing.pool.Pool):
    """Pool that creates non-daemon workers from inside an Airflow daemon task."""

    @staticmethod
    def Process(ctx: Any, *args: Any, **kwds: Any) -> _NonDaemonProcess:  # type: ignore[override]  # noqa: N802
        kwds.pop("daemon", None)
        return _NonDaemonProcess(*args, **kwds)


# ---------------------------------------------------------------------------
# Per-process worker state — set once by _worker_init in each worker process
# ---------------------------------------------------------------------------
_worker_state: dict[str, Any] = {}


def _worker_init(mongodb_uri: str, db_name: str, chunk_size: int) -> None:
    """Open one MongoClient per worker process (called by Pool initializer)."""
    _worker_state["client"] = pymongo.MongoClient(mongodb_uri)
    _worker_state["db"] = _worker_state["client"][db_name]
    _worker_state["chunk_size"] = chunk_size


def _process_file(args: tuple[str, str]) -> dict[str, Any]:
    """Process one .gz NDJSON partition and upsert into MongoDB.

    Each worker process owns its own MongoClient (set up by _worker_init),
    its own GIL, and its own CPU core — true parallel execution with no
    socket contention.

    Parameters
    ----------
    args : (gz_file_str, entity_type)

    Returns
    -------
    dict with ``entity_type``, ``file``, ``records``, ``upserted``,
    ``modified``, ``errors``.
    """
    gz_file_str, entity_type = args
    gz_file = Path(gz_file_str)
    db = _worker_state["db"]
    collection = db[entity_type]
    chunk_size = _worker_state["chunk_size"]

    counts: dict[str, int] = {"records": 0, "upserted": 0, "modified": 0, "errors": 0}
    ops: list[UpdateOne] = []

    def _flush() -> None:
        if not ops:
            return
        try:
            result = collection.bulk_write(ops, ordered=False)
            counts["upserted"] += len(result.upserted_ids)
            counts["modified"] += result.modified_count
        except Exception:  # noqa: BLE001
            counts["errors"] += len(ops)
        finally:
            ops.clear()

    with gzip.open(gz_file, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                counts["errors"] += 1
                continue
            doc_id = doc.get("id")
            if not doc_id:
                counts["errors"] += 1
                continue
            ops.append(UpdateOne({"id": doc_id}, {"$set": doc}, upsert=True))
            counts["records"] += 1
            if len(ops) >= chunk_size:
                _flush()
    _flush()

    # Atomic checkpoint — $addToSet requires no read-modify-write, no locks.
    db["etl_checkpoints"].update_one(
        {"_id": f"openalex_{entity_type}"},
        {"$addToSet": {"last_value": gz_file_str}},
        upsert=True,
    )
    return {"entity_type": entity_type, "file": gz_file_str, **counts}


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

# Chunk size: 2000 docs per bulk_write balances memory and round-trip overhead
DEFAULT_CHUNK_SIZE = 2000
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

        # Sparse index on title — used by openalex_es_load to filter and
        # chunk documents efficiently without a full collection scan.
        if self.collection_name == "works":
            self.collection.create_index(
                [("title", 1)], sparse=True, background=True, name="title_1"
            )
            self.logger.info("Ensured sparse index 'title_1' on works collection.")

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
        mongodb_uri: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> dict[str, Any]:
        """
        Load ALL entity types into MongoDB using a real ``multiprocessing.Pool``.

        Uses ``_NoDaemonPool`` (non-daemon process override) to bypass the
        Airflow daemon worker restriction.  Each worker process has its own
        GIL and ``MongoClient``, delivering true CPU parallelism for gzip
        decompression, JSON parsing, and MongoDB writes.

        Checkpoints use ``$addToSet`` — atomic, no locking, no read-modify-write.

        Parameters
        ----------
        mongodb_uri : str
            Passed to worker processes via ``_worker_init`` so each can open
            its own ``MongoClient`` (workers cannot inherit the parent socket).
        """
        logger = logging.getLogger("airflow.task.OpenAlexExtractor")

        # One extractor per entity — used only for index creation and
        # reading the initial checkpoint (uses the parent's client).
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

        # Build work queue: list of (gz_file_str, entity_type)
        all_work: list[tuple[str, str]] = []
        skipped_total = 0
        entity_total_files: dict[str, int] = dict.fromkeys(ENTITY_TYPES, 0)

        for entity, ext in extractors.items():
            ckpt_key = f"openalex_{entity}"
            raw_ckpt = ext.get_checkpoint(ckpt_key)
            processed: set[str] = set(raw_ckpt) if isinstance(raw_ckpt, list) else set()
            files = ext._iter_partition_files()
            pending = [f for f in files if str(f) not in processed]
            skipped = len(files) - len(pending)
            skipped_total += skipped
            entity_total_files[entity] = len(pending)
            all_work.extend((str(f), entity) for f in pending)
            logger.info("[%s] %d pending, %d skipped", entity, len(pending), skipped)

        if not all_work:
            logger.info("All %d files already processed.", skipped_total)
            return {"files_processed": 0, "files_skipped": skipped_total}

        total_files = len(all_work)
        logger.info(
            "Total: %d files across %d entities — %d worker processes (non-daemon)",
            total_files,
            len(ENTITY_TYPES),
            max_workers,
        )

        entity_stats: dict[str, dict[str, int]] = {
            e: {"processed": 0, "records": 0, "upserted": 0, "errors": 0} for e in ENTITY_TYPES
        }
        done_count = 0
        t0 = time.monotonic()

        with _NoDaemonPool(
            processes=max_workers,
            initializer=_worker_init,
            initargs=(mongodb_uri, db_name, chunk_size),
        ) as pool:
            for result in pool.imap_unordered(_process_file, all_work, chunksize=1):
                try:
                    entity = result["entity_type"]
                    entity_stats[entity]["processed"] += 1
                    entity_stats[entity]["records"] += result.get("records", 0)
                    entity_stats[entity]["upserted"] += result.get("upserted", 0)
                    entity_stats[entity]["errors"] += result.get("errors", 0)
                    done_count += 1
                    if done_count % 50 == 0 or done_count == total_files:
                        elapsed = time.monotonic() - t0
                        rate = done_count / elapsed if elapsed > 0 else 0
                        logger.info(
                            "Progress: %d/%d files (%.1f files/s) elapsed=%.0fs",
                            done_count,
                            total_files,
                            rate,
                            elapsed,
                        )
                        lines = [
                            f"  {e}: {entity_stats[e]['processed']}/{entity_total_files[e]} files"
                            f" | {entity_stats[e]['records']:,} docs"
                            + (
                                f" | {entity_stats[e]['errors']} errors"
                                if entity_stats[e]["errors"]
                                else ""
                            )
                            for e in ENTITY_TYPES
                            if entity_total_files[e] > 0
                        ]
                        logger.info("Per-entity:\n%s", "\n".join(lines))
                except Exception as exc:  # noqa: BLE001
                    logger.error("Worker error: %s", exc)

        elapsed = round(time.monotonic() - t0, 2)
        total_records = sum(s["records"] for s in entity_stats.values())
        total_errors = sum(s["errors"] for s in entity_stats.values())

        summary = [
            f"  {e}: {entity_stats[e]['processed']} files | {entity_stats[e]['records']:,} docs"
            + (f" | {entity_stats[e]['errors']} errors" if entity_stats[e]["errors"] else "")
            for e in ENTITY_TYPES
            if entity_total_files[e] > 0
        ]
        logger.info(
            "Completed — %d files in %.1fs — records=%d errors=%d\nFinal per-entity:\n%s",
            done_count,
            elapsed,
            total_records,
            total_errors,
            "\n".join(summary),
        )
        return {
            "files_processed": done_count,
            "files_skipped": skipped_total,
            "total_records": total_records,
            "total_errors": total_errors,
            "elapsed_seconds": elapsed,
            "per_entity": entity_stats,
        }
