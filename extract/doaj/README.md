# DOAJ extractor

This extractor downloads and ingests the DOAJ public data dumps (journals and articles) into MongoDB.

## Configuration

Required:
- `DOAJ_API_KEY`: DOAJ API key for the public dump endpoints.
- `MONGODB_URI` or Airflow connection `mongodb_default`.

Optional:
- `cache_dir`: custom staging folder for downloads/extracted dumps (default: `/tmp/impactu_airflow_cache/doaj`).

## MongoDB layout

Target database name is computed by period:
- `doaj_{YYYY}_{S}` where `S=1` (Jan–Jun) or `S=2` (Jul–Dec)

Collections (defaults, can be overridden in the DAG params):
- `stage` (journals)
- `articles`

Records are upserted by a stable identifier (DOI when available for articles, DOAJ ID otherwise, or ISSN for journals). The document `_id` is built from that identifier to enforce idempotency; if no identifier exists, a stable hash of the record is used as a fallback. Journals go to the `stage` collection by default.

## Running locally

```bash
python -c "from extract.doaj.doaj_extractor import DoajExtractor; \
    ex=DoajExtractor(mongodb_uri='mongodb://localhost:27017', db_name='doaj_2026_1', api_key='YOUR_KEY'); \
    ex.run(download_journals=True, download_articles=False)"
```

## Airflow DAG

DAG: `extract_doaj`

Parameters (all optional unless noted):
- `api_key` (string): DOAJ API key (overrides Airflow Variables `doaj_api_key` or `DOAJ_API_KEY`)
- `download_journals` (bool)
- `download_articles` (bool)
- `chunk_size` (int)
- `force_download` (bool)
- `cache_dir` (string)
- `keep_cache` (bool)
- `journals_collection` (string)
- `articles_collection` (string)
- `period_year` / `period_semester` (int): override DB period

Notes:
- Runs are idempotent (upsert by `_id`), so re-running will not duplicate data.
- By default `keep_cache=False`, so downloaded/extracted files are removed after each run.
- If you want to avoid re-downloading when running multiple times, set `keep_cache=True`.

Schedule:
- Airflow Variable `doaj_schedule` controls the DAG schedule (default: `@monthly`).

If period overrides are not provided, the DAG computes the DB name from the logical execution date.
