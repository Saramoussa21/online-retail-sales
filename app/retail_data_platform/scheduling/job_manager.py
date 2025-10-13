"""
Job Manager
"""
import json
import time
from pathlib import Path
from uuid import uuid4
from typing import Dict, Any, List

from .scheduler import scheduler
from retail_data_platform.utils.logging_config import get_logger

_logger = get_logger(__name__)

_JOBS_FILE = Path(__file__).resolve().parents[2] / ".scheduled_jobs.json"


def _ensure_jobs_file() -> None:
    _JOBS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not _JOBS_FILE.exists():
        _JOBS_FILE.write_text("[]", encoding="utf-8")


def _load_jobs() -> List[Dict[str, Any]]:
    _ensure_jobs_file()
    try:
        return json.loads(_JOBS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        _logger.error("Failed to load persisted jobs: %s", exc)
        return []


def _save_jobs(jobs: List[Dict[str, Any]]) -> None:
    _ensure_jobs_file()
    try:
        _JOBS_FILE.write_text(json.dumps(jobs, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        _logger.error("Failed to save persisted jobs: %s", exc)


class JobManager:
    def create_daily_job(self, name: str, csv_path: str, time_str: str = "02:00"):
        try:
            scheduler.add_daily_job(name, csv_path, time_str)
        except Exception:
            _logger.debug("scheduler.add_daily_job failed for %s", name, exc_info=True)

        jobs = _load_jobs()
        if not any(j.get("name") == name for j in jobs):
            job = {
                "id": str(uuid4()),
                "name": name,
                "type": "daily",
                "time": time_str,
                "csv_path": csv_path,
                "created_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
                "last_run": None,
            }
            jobs.append(job)
            _save_jobs(jobs)
        print(f"✅ Created daily job: {name} at {time_str}")

    def list_jobs(self):
        persisted = {j["name"]: j for j in _load_jobs()}
        try:
            runtime = {j.name: j for j in scheduler.list_jobs()}
        except Exception:
            runtime = {}
        if not persisted and not runtime:
            print("📋 No scheduled jobs")
            return
        all_names = sorted(set(list(persisted.keys()) + list(runtime.keys())))
        print(f"📋 Scheduled Jobs ({len(all_names)} total):")
        print("-" * 60)
        for name in all_names:
            p = persisted.get(name)
            r = runtime.get(name)
            jtype = p.get("type") if p else getattr(r, "schedule_type", "daily")
            time_str = p.get("time") if p else getattr(r, "time", "unknown")
            csv = p.get("csv_path") if p else getattr(r, "csv_path", "unknown")
            last_run = "Never"
            if p and p.get("last_run"):
                last_run = p.get("last_run")
            elif r and getattr(r, "last_run", None):
                lr = getattr(r, "last_run")
                last_run = lr.strftime("%Y-%m-%d %H:%M:%S") if hasattr(lr, "strftime") else str(lr)
            status = "✅ Enabled" if (r and getattr(r, "enabled", True)) or (name in scheduler._scheduled_refs) else "❌ Disabled"
            print(f"📄 {name}")
            print(f"   Type: {jtype}")
            print(f"   Time: {time_str}")
            print(f"   CSV: {csv}")
            print(f"   Status: {status}")
            print(f"   Last Run: {last_run}")
            print()

    def start_scheduler(self):
        jobs = _load_jobs()
        for j in jobs:
            try:
                if j.get("type") == "daily":
                    scheduler.add_daily_job(j["name"], j["csv_path"], j["time"])
                elif j.get("type") == "hourly":
                    scheduler.add_hourly_job(j["name"], j["csv_path"], int(j.get("hours", 1)))
                elif j.get("type") == "weekly":
                    scheduler.add_weekly_job(j["name"], j["csv_path"], j.get("day"), j.get("time"))
            except Exception:
                _logger.debug("Failed to register persisted job %s", j.get("name"), exc_info=True)

        scheduler.start()
        print("🚀 Scheduler started (press Ctrl+C to stop)")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("🛑 Scheduler stopping by user")
            try:
                scheduler.stop()
            except Exception:
                pass
            print("⏹️ Scheduler stopped")

    def stop_scheduler(self):
        try:
            scheduler.stop()
        except Exception:
            _logger.debug("scheduler.stop failed", exc_info=True)
        print("⏹️ Scheduler stopped")


# global instance
job_manager = JobManager()