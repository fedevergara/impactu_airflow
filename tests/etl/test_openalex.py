"""Tests for OpenAlexExtractor."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, cast

import mongomock
import pytest
from mongomock.collection import BulkOperationBuilder

from extract.openalex.openalex_extractor import ENTITY_TYPES, OpenAlexExtractor

# ---------------------------------------------------------------------------
# Mongomock / pymongo 4.x compatibility patch
# ---------------------------------------------------------------------------
_cls = cast(Any, BulkOperationBuilder)
if hasattr(_cls, "add_update"):
    _orig = _cls.add_update

    def _patched(self, f, d, upsert=False, multi=False, **kw):
        kw.pop("sort", None)
        return _orig(self, f, d, upsert, multi, **kw)

    _cls.add_update = _patched


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_snapshot_dir(base: Path, entity_type: str, records: list[dict]) -> None:
    """Create a minimal snapshot directory with one .gz partition file."""
    partition_dir = base / "data" / entity_type / "updated_date=2024-01-01"
    partition_dir.mkdir(parents=True, exist_ok=True)
    gz_path = partition_dir / "part_000.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def _make_extractor(snapshot_dir: str, entity_type: str = "sources", client=None, max_workers: int = 2) -> OpenAlexExtractor:
    return OpenAlexExtractor(
        mongodb_uri="",
        db_name="test_openalex",
        entity_type=entity_type,
        snapshot_dir=snapshot_dir,
        client=client or mongomock.MongoClient(),
        chunk_size=10,
        max_workers=max_workers,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_mongo():
    return mongomock.MongoClient()


@pytest.fixture
def tmp_snapshot(tmp_path):
    """Return tmp_path; callers populate it."""
    return tmp_path


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

def test_invalid_entity_type():
    with pytest.raises(ValueError, match="entity_type must be one of"):
        OpenAlexExtractor(
            mongodb_uri="",
            db_name="test",
            entity_type="invalid_entity",
            client=mongomock.MongoClient(),
        )


def test_valid_entity_types():
    for entity in ENTITY_TYPES:
        ext = OpenAlexExtractor(
            mongodb_uri="",
            db_name="test",
            entity_type=entity,
            client=mongomock.MongoClient(),
        )
        assert ext.entity_type == entity
        assert ext.collection_name == entity


def test_initialization(mock_mongo, tmp_snapshot):
    ext = _make_extractor(str(tmp_snapshot), client=mock_mongo)
    assert ext.db_name == "test_openalex"
    assert ext.chunk_size == 10
    assert ext.collection is not None


# ---------------------------------------------------------------------------
# No partition files
# ---------------------------------------------------------------------------

def test_run_no_entity_dir(mock_mongo, tmp_snapshot):
    ext = _make_extractor(str(tmp_snapshot), client=mock_mongo)
    result = ext.run()
    assert result["files_processed"] == 0
    assert result["files_skipped"] == 0


# ---------------------------------------------------------------------------
# Basic insert
# ---------------------------------------------------------------------------

def test_run_inserts_documents(mock_mongo, tmp_snapshot):
    records = [
        {"id": "https://openalex.org/S1", "display_name": "Nature"},
        {"id": "https://openalex.org/S2", "display_name": "Science"},
    ]
    _make_snapshot_dir(tmp_snapshot, "sources", records)

    ext = _make_extractor(str(tmp_snapshot), client=mock_mongo)
    result = ext.run()

    assert result["files_processed"] == 1
    assert result["total_records"] == 2
    count = mock_mongo["test_openalex"]["sources"].count_documents({})
    assert count == 2


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def test_run_idempotent(mock_mongo, tmp_snapshot):
    records = [{"id": "https://openalex.org/S1", "display_name": "Nature"}]
    _make_snapshot_dir(tmp_snapshot, "sources", records)

    ext = _make_extractor(str(tmp_snapshot), client=mock_mongo)
    ext.run()
    ext.run()  # second run: file already checkpointed

    count = mock_mongo["test_openalex"]["sources"].count_documents({})
    assert count == 1  # no duplicates


# ---------------------------------------------------------------------------
# Checkpoint saved
# ---------------------------------------------------------------------------

def test_checkpoint_is_saved(mock_mongo, tmp_snapshot):
    records = [{"id": "https://openalex.org/S1", "display_name": "Nature"}]
    _make_snapshot_dir(tmp_snapshot, "sources", records)

    ext = _make_extractor(str(tmp_snapshot), client=mock_mongo)
    assert ext.get_checkpoint("openalex_sources") is None

    ext.run()

    ckpt = ext.get_checkpoint("openalex_sources")
    assert isinstance(ckpt, list)
    assert len(ckpt) == 1
    assert "part_000.gz" in ckpt[0]


# ---------------------------------------------------------------------------
# Checkpoint resume — second partition skipped
# ---------------------------------------------------------------------------

def test_checkpoint_resume(mock_mongo, tmp_snapshot):
    # Two partition files
    entity_dir = tmp_snapshot / "data" / "sources"

    part_a = entity_dir / "updated_date=2024-01-01"
    part_a.mkdir(parents=True, exist_ok=True)
    with gzip.open(part_a / "part_000.gz", "wt") as fh:
        fh.write(json.dumps({"id": "S1", "display_name": "A"}) + "\n")

    part_b = entity_dir / "updated_date=2024-01-02"
    part_b.mkdir(parents=True, exist_ok=True)
    with gzip.open(part_b / "part_000.gz", "wt") as fh:
        fh.write(json.dumps({"id": "S2", "display_name": "B"}) + "\n")

    ext = _make_extractor(str(tmp_snapshot), client=mock_mongo)

    # First run: both partitions processed
    result1 = ext.run()
    assert result1["files_processed"] == 2
    assert result1["files_skipped"] == 0

    # Second run: both partitions already in checkpoint set
    result2 = ext.run()
    assert result2["files_processed"] == 0
    assert result2["files_skipped"] == 2

    assert mock_mongo["test_openalex"]["sources"].count_documents({}) == 2


def test_max_workers_respected(mock_mongo, tmp_snapshot):
    """Extractor accepts max_workers param and still produces correct results."""
    records = [{"id": f"S{i}", "display_name": f"Journal {i}"} for i in range(20)]
    _make_snapshot_dir(tmp_snapshot, "sources", records)

    ext = _make_extractor(str(tmp_snapshot), client=mock_mongo, max_workers=4)
    result = ext.run()

    assert result["total_records"] == 20
    assert mock_mongo["test_openalex"]["sources"].count_documents({}) == 20


# ---------------------------------------------------------------------------
# Malformed lines are skipped gracefully
# ---------------------------------------------------------------------------

def test_malformed_lines_skipped(mock_mongo, tmp_snapshot):
    entity_dir = tmp_snapshot / "data" / "sources" / "updated_date=2024-01-01"
    entity_dir.mkdir(parents=True, exist_ok=True)
    gz_path = entity_dir / "part_000.gz"
    with gzip.open(gz_path, "wt") as fh:
        fh.write("not valid json\n")
        fh.write(json.dumps({"id": "S1", "display_name": "Good"}) + "\n")
        fh.write("\n")  # empty line

    ext = _make_extractor(str(tmp_snapshot), client=mock_mongo)
    result = ext.run()

    assert result["total_records"] == 1
    assert result["total_errors"] == 1
    assert mock_mongo["test_openalex"]["sources"].count_documents({}) == 1


# ---------------------------------------------------------------------------
# Records without id field are counted as errors
# ---------------------------------------------------------------------------

def test_drop_db_clears_data_and_checkpoints(tmp_snapshot):
    client = mongomock.MongoClient()
    records = [{"id": "S1", "display_name": "Nature"}]
    _make_snapshot_dir(tmp_snapshot, "sources", records)

    # First load: populate DB and checkpoint
    ext = _make_extractor(str(tmp_snapshot), client=client)
    ext.run()
    assert client["test_openalex"]["sources"].count_documents({}) == 1
    assert ext.get_checkpoint("openalex_sources") is not None

    # Second run with drop_db=True: DB and checkpoints are wiped, then reloaded
    ext2 = OpenAlexExtractor(
        mongodb_uri="",
        db_name="test_openalex",
        entity_type="sources",
        snapshot_dir=str(tmp_snapshot),
        client=client,
        chunk_size=10,
        max_workers=1,
        drop_db=True,
    )
    result = ext2.run()
    assert result["files_processed"] == 1
    assert client["test_openalex"]["sources"].count_documents({}) == 1


def test_missing_id_counted_as_error(mock_mongo, tmp_snapshot):
    records = [
        {"display_name": "No ID here"},
        {"id": "S1", "display_name": "Has ID"},
    ]
    _make_snapshot_dir(tmp_snapshot, "sources", records)

    ext = _make_extractor(str(tmp_snapshot), client=mock_mongo)
    result = ext.run()

    assert result["total_errors"] == 1
    assert result["total_records"] == 1
    assert mock_mongo["test_openalex"]["sources"].count_documents({}) == 1
