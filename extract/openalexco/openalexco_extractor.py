"""OpenAlexCO extractor: Colombia cut from the full OpenAlex MongoDB database."""

from __future__ import annotations

import time
from typing import Any

from joblib import Parallel, delayed
from pymongo import MongoClient

from extract.base_extractor import BaseExtractor

# ---------------------------------------------------------------------------
# Collections to copy verbatim (full copy, no country filter)
# ---------------------------------------------------------------------------
COPY_COLLECTIONS = [
    "concepts",
    "funders",
    "institutions",
    "publishers",
    "sources",
    "domains",
    "fields",
    "subfields",
    "topics",
]


class OpenAlexCOExtractor(BaseExtractor):
    """
    Extracts the Colombia cut from the full OpenAlex MongoDB database.

    Steps (mirroring the original colombia_cut.py):
    1. Works with Colombian authorship (aggregate $out).
    2. Works from Colombian sources (parallel joblib).
    3. Works by DOI match across multiple source databases.
    4. Works by Minciencias Gruplac/CvLAC title+author similarity.
    5. Dedup works via aggregate.
    6. Authors referenced in works.
    7. Verbatim copy of auxiliary collections.

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI.
    db_in : str
        Source database name (default: ``openalex``).
    db_out : str
        Target database name (default: ``openalexco``).
    es_index : str
        Elasticsearch index name used by Minciencias similarity search.
    jobs : int
        Parallelism for joblib tasks.
    backend : str
        joblib backend (default: ``threading``).
    es_uri : str
        Elasticsearch URI.
    es_auth : tuple[str, str]
        Elasticsearch (user, password).
    client : pymongo.MongoClient, optional
        Existing MongoClient (overrides mongodb_uri).
    """

    def __init__(
        self,
        db_in: str = "openalex",
        db_out: str = "openalexco",
        es_index: str = "openalex_index",
        jobs: int = 72,
        backend: str = "threading",
        es_uri: str = "http://localhost:9200",
        es_auth: tuple[str, str] = ("elastic", "colav"),
        client: Any = None,
        mongodb_uri: str = "",
    ) -> None:
        # BaseExtractor expects (uri, db_name, collection_name) — use db_out as primary db.
        super().__init__(mongodb_uri, db_out, collection_name="works", client=client)
        self.db_in = db_in
        self.db_out = db_out
        self.es_index = es_index
        self.jobs = jobs
        self.backend = backend
        self.es_uri = es_uri
        self.es_auth = es_auth
        # Raw MongoClient for multi-db access
        self._client: MongoClient = self.client  # type: ignore[assignment]
        self.create_indexes()

    def create_indexes(self) -> None:
        db_in = self._client[self.db_in]
        db_out = self._client[self.db_out]

        # --- db_in (openalex) indexes needed for the cut queries ---
        # Step 1: authorship country filter
        db_in["works"].create_index("authorships.countries", background=True)
        db_in["works"].create_index("authorships.institutions.country_code", background=True)
        # Step 2: source country + works by source
        db_in["sources"].create_index("country_code", background=True)
        db_in["works"].create_index("locations.source.id", background=True)
        # Step 3: DOI lookup
        db_in["works"].create_index("doi", background=True)

        # --- db_out (openalexco) indexes ---
        # Compound index used by $merge on "id" and type queries
        db_out["works"].create_index(
            [("type", 1), ("type_crossref", 1), ("id", 1)], background=True
        )
        # DOI lookup for dedup in step 3
        db_out["works"].create_index("doi", background=True)
        # Author extraction in step 6
        db_out["works"].create_index("authorships.author.id", background=True)

    # ------------------------------------------------------------------
    # Step 1 — Colombian authorship works
    # ------------------------------------------------------------------
    def _cut_works_by_authorship(self) -> None:
        self.logger.info("Step 1: cutting works by Colombian authorship …")
        t0 = time.time()
        pipeline = [
            {"$project": {"_id": 0}},
            {
                "$match": {
                    "$or": [
                        {"authorships.countries": "CO"},
                        {"authorships.institutions.country_code": "CO"},
                    ]
                }
            },
            {"$out": {"db": self.db_out, "coll": "works"}},
        ]
        self._client[self.db_in]["works"].aggregate(pipeline)
        self.logger.info("Step 1 done in %.1fs", time.time() - t0)

    # ------------------------------------------------------------------
    # Step 2 — Works from Colombian sources
    # ------------------------------------------------------------------
    def _cut_works_by_sources(self) -> None:
        self.logger.info("Step 2: cutting works from Colombian sources …")
        t0 = time.time()

        # $out in step 1 drops and recreates the collection, removing all indexes.
        # $merge requires a unique index on the join field — recreate it here.
        self._client[self.db_out]["works"].create_index("id", unique=True, background=True)

        source_ids = [
            s["id"]
            for s in self._client[self.db_in]["sources"].find(
                {"country_code": "CO"}, {"id": 1, "_id": 0}
            )
            if s.get("id")
        ]
        self.logger.info("Step 2: found %d Colombian sources", len(source_ids))

        # Single server-side pipeline: match works from Colombian sources,
        # merge into db_out.works keeping already-inserted docs untouched.
        pipeline = [
            {"$match": {"locations.source.id": {"$in": source_ids}}},
            {"$project": {"_id": 0}},
            {
                "$merge": {
                    "into": {"db": self.db_out, "coll": "works"},
                    "on": "id",
                    "whenMatched": "keepExisting",
                    "whenNotMatched": "insert",
                }
            },
        ]
        self._client[self.db_in]["works"].aggregate(pipeline)
        self.logger.info("Step 2 done in %.1fs", time.time() - t0)

    # ------------------------------------------------------------------
    # Step 3 — DOI-based cut
    # ------------------------------------------------------------------
    def _cut_works_by_dois(self) -> None:
        from extract.openalexco.colombia_cut_dois import colombia_cut_dois

        self.logger.info("Step 3: cutting works by DOI …")
        t0 = time.time()
        colombia_cut_dois(
            db_in=self.db_in,
            db_out=self.db_out,
            jobs=self.jobs,
            backend=self.backend,
            client=self._client,
        )
        self.logger.info("Step 3 done in %.1fs", time.time() - t0)

    # ------------------------------------------------------------------
    # Step 4 — Minciencias similarity cut
    # ------------------------------------------------------------------
    def _cut_works_by_minciencias(self) -> None:
        from extract.openalexco.colombia_cut_minciencias import colombia_cut_minciencias

        self.logger.info("Step 4: cutting works via Minciencias similarity …")
        t0 = time.time()
        colombia_cut_minciencias(
            db_in=self.db_in,
            db_out=self.db_out,
            es_index=self.es_index,
            jobs=self.jobs,
            backend=self.backend,
            client=self._client,
            es_uri=self.es_uri,
            es_auth=self.es_auth,
        )
        self.logger.info("Step 4 done in %.1fs", time.time() - t0)

    # ------------------------------------------------------------------
    # Step 5 — Dedup works
    # ------------------------------------------------------------------
    def _dedup_works(self) -> None:
        self.logger.info("Step 5: deduplicating works …")
        t0 = time.time()
        pipeline = [
            {"$group": {"_id": "$id", "uniquedoc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$uniquedoc"}},
        ]
        for doc in self._client[self.db_out]["works"].aggregate(pipeline):
            self._client[self.db_out]["works_tmp"].insert_one(doc)
        self._client[self.db_out]["works"].drop()
        self._client[self.db_out]["works_tmp"].rename("works")
        self.logger.info("Step 5 done in %.1fs", time.time() - t0)

    # ------------------------------------------------------------------
    # Step 6 — Authors
    # ------------------------------------------------------------------
    def _cut_authors(self) -> None:
        self.logger.info("Step 6: extracting referenced authors …")
        t0 = time.time()
        pipeline = [
            {"$project": {"_id": 0, "authorships.author.id": 1}},
            {"$unwind": "$authorships"},
            {"$group": {"_id": None, "authors": {"$addToSet": "$authorships.author.id"}}},
            {"$unwind": "$authors"},
            {"$project": {"_id": 0}},
        ]
        authors_ids = list(self._client[self.db_out]["works"].aggregate(pipeline))

        client = self._client

        def _save_author(aid: str) -> None:
            author = client[self.db_in]["authors"].find_one({"id": aid})
            if author is not None:
                client[self.db_out]["authors"].insert_one(author)

        Parallel(n_jobs=self.jobs, verbose=10, backend=self.backend, batch_size=100)(
            delayed(_save_author)(a["authors"]) for a in authors_ids
        )
        self.logger.info("Step 6 done in %.1fs", time.time() - t0)

    # ------------------------------------------------------------------
    # Step 7 — Verbatim copy of auxiliary collections
    # ------------------------------------------------------------------
    def _copy_auxiliary_collections(self) -> None:
        for coll in COPY_COLLECTIONS:
            self.logger.info("Step 7: copying %s …", coll)
            t0 = time.time()
            pipeline_copy = [
                {"$match": {}},
                {"$out": {"db": self.db_out, "coll": coll}},
            ]
            self._client[self.db_in][coll].aggregate(pipeline_copy)
            self.logger.info("  %s done in %.1fs", coll, time.time() - t0)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def run(self, **kwargs: Any) -> None:  # type: ignore[override]
        """Execute the full Colombia cut pipeline."""
        self._cut_works_by_authorship()
        self._cut_works_by_sources()
        self._cut_works_by_dois()
        self._cut_works_by_minciencias()
        self._dedup_works()
        self._cut_authors()
        self._copy_auxiliary_collections()
        self.logger.info("Colombia cut complete.")
