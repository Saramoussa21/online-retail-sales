"""
Simple ETL Job Scheduler 
"""
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import threading
import time
import json
from typing import Dict, List, Optional, Callable

import schedule  

from ..etl.pipeline import run_retail_csv_etl
from ..utils.logging_config import get_logger

_logger = get_logger(__name__)

_JOBS_FILE = Path(__file__).resolve().parents[2] / ".scheduled_jobs.json"


@dataclass
class SimpleJob:
    name: str
    csv_path: str
    time: str
    schedule_type: str  # "daily", "hourly", "weekly"
    enabled: bool = True
    last_run: Optional[datetime] = None


class ETLScheduler:
    def __init__(self):
        self._jobs_meta: Dict[str, Dict] = {}   # name -> meta (csv_path, time, schedule_type)
        self._scheduled_refs: Dict[str, schedule.Job] = {}  # name -> schedule job ref
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.is_running = False

    def _persist_update_last_run(self, name: str, ts: datetime):
        try:
            if not _JOBS_FILE.exists():
                return
            jobs = json.loads(_JOBS_FILE.read_text(encoding="utf-8"))
            changed = False
            for j in jobs:
                if j.get("name") == name:
                    j["last_run"] = ts.isoformat()
                    changed = True
            if changed:
                _JOBS_FILE.write_text(json.dumps(jobs, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            _logger.exception("Failed to update last_run for %s", name)

    def _run_job(self, name: str):
        meta = self._jobs_meta.get(name)
        if not meta:
            _logger.error("Job metadata not found for %s", name)
            return
        csv_path = meta.get("csv_path")
        job_name = name
        _logger.info("🔔 Running scheduled job: %s (csv=%s)", job_name, csv_path)
        try:
            metrics = run_retail_csv_etl(csv_path, job_name)
            _logger.info("✅ Scheduled job finished: %s result=%s", job_name, getattr(metrics, "status", str(metrics)))
        except Exception as exc:
            _logger.exception("Scheduled job %s failed: %s", job_name, exc)
        # record last run in memory and persist
        now = datetime.utcnow()
        meta["last_run"] = now.isoformat()
        self._persist_update_last_run(name, now)

    def add_daily_job(self, name: str, csv_path: str, time_str: str = "02:00"):
        """Register a daily job at HH:MM (24h)"""
        # store meta
        self._jobs_meta[name] = {"csv_path": csv_path, "time": time_str, "schedule_type": "daily", "last_run": None}
        # remove existing schedule if present
        if name in self._scheduled_refs:
            try:
                schedule.cancel_job(self._scheduled_refs[name])
            except Exception:
                pass
        job = schedule.every().day.at(time_str).do(self._run_job, name)
        self._scheduled_refs[name] = job
        _logger.info("✅ Added daily job '%s' at %s", name, time_str)

    def add_hourly_job(self, name: str, csv_path: str, hours: int = 1):
        self._jobs_meta[name] = {"csv_path": csv_path, "hours": hours, "schedule_type": "hourly", "last_run": None}
        if name in self._scheduled_refs:
            try:
                schedule.cancel_job(self._scheduled_refs[name])
            except Exception:
                pass
        job = schedule.every(hours).hours.do(self._run_job, name)
        self._scheduled_refs[name] = job
        _logger.info("✅ Added hourly job '%s' every %d hour(s)", name, hours)

    def add_weekly_job(self, name: str, csv_path: str, day: str = "monday", time_str: str = "02:00"):
        self._jobs_meta[name] = {"csv_path": csv_path, "day": day, "time": time_str, "schedule_type": "weekly", "last_run": None}
        if name in self._scheduled_refs:
            try:
                schedule.cancel_job(self._scheduled_refs[name])
            except Exception:
                pass
        # schedule.every().monday.at("10:00").do(...)
        day_attr = getattr(schedule.every(), day, None)
        if callable(day_attr):
            job = day_attr.at(time_str).do(self._run_job, name)
            self._scheduled_refs[name] = job
            _logger.info("✅ Added weekly job '%s' on %s at %s", name, day, time_str)
        else:
            _logger.error("Invalid weekday for weekly job: %s", day)

    def remove_job(self, name: str) -> bool:
        if name in self._scheduled_refs:
            try:
                schedule.cancel_job(self._scheduled_refs[name])
            except Exception:
                pass
            del self._scheduled_refs[name]
        if name in self._jobs_meta:
            del self._jobs_meta[name]
        return True

    def enable_job(self, name: str) -> bool:
        # schedule module does not have enable/disable flag; implement by canceling/adding
        if name not in self._jobs_meta:
            return False
        meta = self._jobs_meta[name]
        if name in self._scheduled_refs:
            # already enabled
            return True
        # re-add depending on schedule_type
        st = meta.get("schedule_type")
        if st == "daily":
            self.add_daily_job(name, meta["csv_path"], meta["time"])
            return True
        if st == "hourly":
            self.add_hourly_job(name, meta["csv_path"], int(meta.get("hours", 1)))
            return True
        if st == "weekly":
            self.add_weekly_job(name, meta["csv_path"], meta.get("day", "monday"), meta.get("time", "02:00"))
            return True
        return False

    def disable_job(self, name: str) -> bool:
        if name in self._scheduled_refs:
            try:
                schedule.cancel_job(self._scheduled_refs[name])
            except Exception:
                pass
            del self._scheduled_refs[name]
            return True
        return False

    def list_jobs(self) -> List[SimpleJob]:
        out: List[SimpleJob] = []
        for name, meta in self._jobs_meta.items():
            last = None
            if meta.get("last_run"):
                try:
                    last = datetime.fromisoformat(meta["last_run"])
                except Exception:
                    last = None
            enabled = name in self._scheduled_refs
            sj = SimpleJob(name=name,
                           csv_path=meta.get("csv_path", ""),
                           time=meta.get("time", ""),
                           schedule_type=meta.get("schedule_type", "unknown"),
                           enabled=enabled,
                           last_run=last)
            out.append(sj)
        return out

    def _run_scheduler(self):
        while not self._stop_event.is_set():
            try:
                schedule.run_pending()
            except Exception:
                _logger.exception("Error while running pending scheduled jobs")
            time.sleep(1)

    def start(self):
        if self.is_running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self._thread.start()
        self.is_running = True
        _logger.info("🚀 ETL Scheduler started")

    def stop(self):
        if not self.is_running:
            return
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        self.is_running = False
        _logger.info("⏹️ ETL Scheduler stopped")


scheduler = ETLScheduler()