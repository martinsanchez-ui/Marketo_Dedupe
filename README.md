# Marketo_Dedupe

## Running the pipeline (single-run diagnostics)

1. Ensure credentials are available in `datafiles/app_credentials.env` (see `etl_scripts/global_settings.py` for expected keys).
2. Execute the main script once: `python etl_scripts/marketo_dedupe.py`
3. Each run produces a timestamped diagnostics folder under `etl_scripts/diagnostics/<YYYYMMDD_HHMMSS>/` containing:
   - `filters.jsonl`: per-chunk date filters.
   - `api_responses/`: sanitized Marketo API responses for every call.
   - `job_status/`: status polls plus final status snapshots.
   - `downloads/`: raw CSV exports saved as `<export_id>.csv`.
   - `counts/`: per-export row counts and combined summaries.
   - `sqlite/`: the SQLite database file and load metrics.
   - `run_summary.json`: high-level metrics and anomalies for the run.
