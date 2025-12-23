"""
diagnostics.py
    Utilities for capturing structured diagnostics and artifacts for Marketo exports.
"""
import csv
import hashlib
import json
import os
import statistics
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


SENSITIVE_KEYS = {"token", "secret", "password", "client_secret", "clientid", "client_id", "access_token"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class DiagnosticsManager:
    """Tracks per-run diagnostics artifacts and metadata."""

    def __init__(self, base_path: str, logger) -> None:
        self.log = logger
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.root = os.path.join(base_path, self.run_id)
        self.paths = {
            "api_responses": os.path.join(self.root, "api_responses"),
            "job_status": os.path.join(self.root, "job_status"),
            "downloads": os.path.join(self.root, "downloads"),
            "counts": os.path.join(self.root, "counts"),
            "sqlite": os.path.join(self.root, "sqlite"),
            "filters": os.path.join(self.root, "filters.jsonl"),
            "run_summary": os.path.join(self.root, "run_summary.json"),
        }
        self._ensure_directories()

        self.api_call_counters: Dict[str, int] = {}
        self.export_chunks: Dict[str, Dict[str, Any]] = {}
        self.job_status_final: Dict[str, Dict[str, Any]] = {}
        self.download_records: List[Dict[str, Any]] = []
        self.sqlite_insert_records: List[Dict[str, Any]] = []
        self.sqlite_total_rows: Optional[int] = None
        self.phase_times: Dict[str, Dict[str, Any]] = {}
        self.anomalies: List[str] = []
        self.total_bytes_downloaded: int = 0
        self.median_row_count: float = 0
        self.low_row_threshold: int = 0

    @property
    def filters_path(self) -> str:
        return self.paths["filters"]

    @property
    def downloads_dir(self) -> str:
        return self.paths["downloads"]

    @property
    def counts_dir(self) -> str:
        return self.paths["counts"]

    @property
    def sqlite_dir(self) -> str:
        return self.paths["sqlite"]

    def _ensure_directories(self) -> None:
        os.makedirs(self.root, exist_ok=True)
        os.makedirs(self.paths["api_responses"], exist_ok=True)
        os.makedirs(self.paths["job_status"], exist_ok=True)
        os.makedirs(self.paths["downloads"], exist_ok=True)
        os.makedirs(self.paths["counts"], exist_ok=True)
        os.makedirs(self.paths["sqlite"], exist_ok=True)

    def _write_jsonl(self, path: str, payload: Dict[str, Any]) -> None:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")

    def sanitize(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            cleaned = {}
            for key, value in payload.items():
                if any(s in key.lower() for s in SENSITIVE_KEYS):
                    continue
                cleaned[key] = self.sanitize(value)
            return cleaned
        if isinstance(payload, list):
            return [self.sanitize(item) for item in payload]
        return payload

    def log_filter(self, export_id: str, start_at: str, end_at: str, year: int, month: int) -> None:
        entry = {
            "timestamp": _now_iso(),
            "export_id": export_id,
            "year": year,
            "month": month,
            "startAt": start_at,
            "endAt": end_at,
        }
        self.export_chunks[export_id] = entry
        self._write_jsonl(self.filters_path, entry)

    def _increment_api_counter(self, method: str, export_id: Optional[str]) -> str:
        key = f"{method}_{export_id or 'none'}"
        self.api_call_counters[key] = self.api_call_counters.get(key, 0) + 1
        return f"{key}_{self.api_call_counters[key]:03d}"

    def _extract_response_metadata(self, response: Any) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {"result_length": None, "success": None, "errors": None, "warnings": None}
        if isinstance(response, (bytes, bytearray)):
            metadata["result_length"] = len(response)
            return metadata

        safe_response = self.sanitize(response)

        if isinstance(safe_response, list) and safe_response:
            first = safe_response[0]
            if isinstance(first, dict):
                metadata.update(
                    {
                        "success": first.get("success", first.get("Success")),
                        "errors": first.get("errors"),
                        "warnings": first.get("warnings"),
                        "result_length": len(safe_response),
                    }
                )
        elif isinstance(safe_response, dict):
            metadata.update(
                {
                    "success": safe_response.get("success", safe_response.get("Success")),
                    "errors": safe_response.get("errors"),
                    "warnings": safe_response.get("warnings"),
                }
            )
            try:
                metadata["result_length"] = len(safe_response.get("result", []))
            except Exception:
                metadata["result_length"] = None
        return metadata

    def record_api_call(
        self,
        method: str,
        export_id: Optional[str],
        request_payload: Dict[str, Any],
        response: Any = None,
        error: Optional[str] = None,
    ) -> None:
        counter_key = self._increment_api_counter(method, export_id)
        metadata = self._extract_response_metadata(response) if response is not None else {}

        api_log = {
            "timestamp": _now_iso(),
            "method": method,
            "export_id": export_id,
            "request": self.sanitize(request_payload),
            "metadata": metadata,
        }
        file_path = os.path.join(self.paths["api_responses"], f"{counter_key}.json")

        if error:
            api_log["error"] = error
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(api_log, f, ensure_ascii=False, indent=2)
            return

        if isinstance(response, (bytes, bytearray)):
            api_log["response"] = {"byte_length": len(response), "preview_hex": response[:200].hex()}
        else:
            api_log["response"] = self.sanitize(response)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(api_log, f, ensure_ascii=False, indent=2)

    def execute_marketo(self, mc, method: str, export_id: Optional[str] = None, **kwargs) -> Any:
        request_summary = {"method": method, **self.sanitize(kwargs)}
        try:
            response = mc.execute(method=method, **kwargs)
            self.record_api_call(method, export_id, request_summary, response=response)
            return response
        except Exception as exc:
            self.record_api_call(method, export_id, request_summary, error=str(exc))
            raise

    def record_job_status(self, export_id: str, status_payload: Dict[str, Any]) -> None:
        snapshot = {"timestamp": _now_iso(), "export_id": export_id}
        snapshot.update(self.sanitize(status_payload))
        status_path = os.path.join(self.paths["job_status"], f"{export_id}.jsonl")
        self._write_jsonl(status_path, snapshot)
        self.job_status_final[export_id] = snapshot

    def write_final_status(self, export_id: str) -> None:
        final_status = self.job_status_final.get(export_id, {})
        final_path = os.path.join(self.paths["job_status"], f"final_{export_id}.json")
        with open(final_path, "w", encoding="utf-8") as f:
            json.dump(final_status, f, ensure_ascii=False, indent=2)

    def record_download_result(
        self,
        export_id: str,
        file_path: str,
        bytes_downloaded: int,
        file_size_on_disk: int,
        header: List[str],
        row_count: int,
        anomalies: Iterable[str],
        created_at_range: Optional[Dict[str, Optional[str]]],
        expected_file_size: Optional[int],
        expected_record_count: Optional[int],
    ) -> None:
        checksum = self._calculate_checksum(file_path)
        chunk = self.export_chunks.get(export_id, {})
        record = {
            "timestamp": _now_iso(),
            "export_id": export_id,
            "file_path": file_path,
            "bytes_downloaded": bytes_downloaded,
            "file_size_on_disk": file_size_on_disk,
            "header": header,
            "row_count": row_count,
            "anomalies": list(anomalies),
            "created_at_range": created_at_range,
            "expected_file_size": expected_file_size,
            "expected_record_count": expected_record_count,
            "checksum_sha256": checksum,
            "startAt": chunk.get("startAt"),
            "endAt": chunk.get("endAt"),
            "month": chunk.get("month"),
            "year": chunk.get("year"),
        }
        per_export_path = os.path.join(self.counts_dir, f"{export_id}.json")
        with open(per_export_path, "w", encoding="utf-8") as f:
            json.dump(self.sanitize(record), f, ensure_ascii=False, indent=2)

        self.download_records.append(record)
        self.total_bytes_downloaded = sum(item["bytes_downloaded"] for item in self.download_records)

    def _calculate_checksum(self, file_path: str) -> Optional[str]:
        if not os.path.exists(file_path):
            return None
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def record_sqlite_insert(self, export_id: str, rows_inserted: int) -> None:
        record = {
            "timestamp": _now_iso(),
            "export_id": export_id,
            "rows_inserted": rows_inserted,
        }
        self.sqlite_insert_records.append(record)

    def set_sqlite_total_rows(self, total_rows: int) -> None:
        self.sqlite_total_rows = total_rows

    def write_sqlite_metrics(self) -> None:
        metrics = {
            "rows_inserted_by_export": self.sqlite_insert_records,
            "total_rows": self.sqlite_total_rows,
            "timestamp": _now_iso(),
        }
        metrics_path = os.path.join(self.sqlite_dir, "metrics.json")
        with open(metrics_path, "w", encoding="utf-8") as f:
            json.dump(self.sanitize(metrics), f, ensure_ascii=False, indent=2)

    def mark_phase_start(self, phase: str) -> None:
        self.phase_times[phase] = {"start": time.monotonic(), "start_ts": _now_iso()}

    def mark_phase_end(self, phase: str) -> None:
        phase_record = self.phase_times.get(phase, {})
        phase_record["end"] = time.monotonic()
        phase_record["end_ts"] = _now_iso()
        if phase_record.get("start") is not None:
            phase_record["duration_seconds"] = phase_record["end"] - phase_record["start"]
        self.phase_times[phase] = phase_record

    def _detect_anomalies(self, median_row_count: float) -> None:
        if not self.download_records:
            return
        low_threshold = 0
        if median_row_count:
            low_threshold = max(1, int(median_row_count * 0.5))

        for record in self.download_records:
            anomalies = set(record.get("anomalies", []))
            if record["row_count"] == 0:
                anomalies.add("empty_file")
            if low_threshold and record["row_count"] < low_threshold:
                anomalies.add("below_median_threshold")
            if record.get("expected_file_size") and record["file_size_on_disk"] != record["expected_file_size"]:
                anomalies.add("file_size_mismatch")
            record["median_row_count"] = median_row_count
            record["low_row_threshold"] = low_threshold
            record["anomalies"] = sorted(anomalies)
            if anomalies:
                self.anomalies.extend(f"{record['export_id']}: {item}" for item in anomalies)

    def write_counts_summary(self) -> None:
        median_row_count = statistics.median([record["row_count"] for record in self.download_records]) if self.download_records else 0
        self.median_row_count = median_row_count
        self.low_row_threshold = max(1, int(median_row_count * 0.5)) if median_row_count else 0
        self._detect_anomalies(median_row_count)

        summary_path_json = os.path.join(self.counts_dir, "summary.json")
        summary_records: List[Dict[str, Any]] = []

        for record in self.download_records:
            summary_records.append(self.sanitize(record))

        with open(summary_path_json, "w", encoding="utf-8") as f:
            json.dump(
                {"median_row_count": median_row_count, "low_row_threshold": self.low_row_threshold, "records": summary_records},
                f,
                ensure_ascii=False,
                indent=2,
            )

        summary_path_csv = os.path.join(self.counts_dir, "summary.csv")
        if summary_records:
            fieldnames = [
                "export_id",
                "year",
                "month",
                "startAt",
                "endAt",
                "bytes_downloaded",
                "file_size_on_disk",
                "row_count",
                "median_row_count",
                "low_row_threshold",
                "expected_file_size",
                "expected_record_count",
                "checksum_sha256",
                "created_at_min",
                "created_at_max",
                "anomalies",
            ]
            with open(summary_path_csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for record in summary_records:
                    created_at_range = record.get("created_at_range") or {}
                    writer.writerow(
                        {
                            "export_id": record.get("export_id"),
                            "year": record.get("year"),
                            "month": record.get("month"),
                            "startAt": record.get("startAt"),
                            "endAt": record.get("endAt"),
                            "bytes_downloaded": record.get("bytes_downloaded"),
                            "file_size_on_disk": record.get("file_size_on_disk"),
                            "row_count": record.get("row_count"),
                            "median_row_count": record.get("median_row_count"),
                            "low_row_threshold": record.get("low_row_threshold"),
                            "expected_file_size": record.get("expected_file_size"),
                            "expected_record_count": record.get("expected_record_count"),
                            "checksum_sha256": record.get("checksum_sha256"),
                            "created_at_min": created_at_range.get("min"),
                            "created_at_max": created_at_range.get("max"),
                            "anomalies": ";".join(record.get("anomalies", [])),
                        }
                    )

    def write_run_summary(self, total_export_jobs: int) -> None:
        summary = {
            "timestamp": _now_iso(),
            "run_id": self.run_id,
            "total_export_jobs": total_export_jobs,
            "total_bytes_downloaded": self.total_bytes_downloaded,
            "total_rows_across_csvs": sum(record.get("row_count", 0) for record in self.download_records),
            "total_rows_inserted_sqlite": self.sqlite_total_rows,
            "rows_inserted_by_export": self.sqlite_insert_records,
            "anomalies": sorted(set(self.anomalies)),
            "phase_timings": self.phase_times,
            "artifact_root": self.root,
            "median_row_count": self.median_row_count,
            "low_row_threshold": self.low_row_threshold,
        }

        with open(self.paths["run_summary"], "w", encoding="utf-8") as f:
            json.dump(self.sanitize(summary), f, ensure_ascii=False, indent=2)
