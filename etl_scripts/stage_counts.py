import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class StageCounts:
    def __init__(self, run_dir: str, logger: Optional[Any] = None, run_id: Optional[str] = None) -> None:
        self.run_dir = run_dir
        os.makedirs(self.run_dir, exist_ok=True)
        self.log = logger
        self.run_id = run_id or os.path.basename(os.path.abspath(self.run_dir))
        self.created_at = _now_iso()
        self.stages: List[Dict[str, Any]] = []
        self.json_path = os.path.join(self.run_dir, "stage_counts.json")
        self.jsonl_path = os.path.join(self.run_dir, "stage_counts.jsonl")

    def add(self, stage_name: str, metrics: Dict[str, Any]) -> None:
        entry = {"stage": stage_name, "ts": _now_iso(), "metrics": metrics}
        self.stages.append(entry)
        if self.log:
            self.log.info(f"[StageCounts] Recorded stage {stage_name} with metrics keys: {list(metrics.keys())}")
        self.write(latest_entry=entry)

    def write(self, latest_entry: Optional[Dict[str, Any]] = None) -> None:
        payload = {
            "run_id": self.run_id,
            "created_at": self.created_at,
            "stages": self.stages,
        }
        os.makedirs(self.run_dir, exist_ok=True)
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        if latest_entry:
            line = {"run_id": self.run_id, "created_at": self.created_at, **latest_entry}
            with open(self.jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(line, ensure_ascii=False) + "\n")
