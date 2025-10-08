"""
Simple ETL Job Scheduler
"""
import schedule
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

from ..etl.pipeline import run_retail_csv_etl
from ..utils.logging_config import ETLLogger

@dataclass
class SimpleJob:
    """Simple job representation"""
    name: str
    csv_path: str
    schedule_type: str  # 'daily', 'hourly', 'weekly'
    time: str          # '02:00' for daily, '2' for hourly interval, 'monday' for weekly
    enabled: bool = True
    last_run: Optional[datetime] = None

class ETLScheduler:
    """Simple ETL Scheduler"""
    
    def __init__(self):
        self.logger = ETLLogger(self.__class__.__name__)
        self.jobs: Dict[str, SimpleJob] = {}
        self.is_running = False
        self.scheduler_thread = None
    
    def add_daily_job(self, name: str, csv_path: str, time: str = "02:00"):
        """Add daily job - runs every day at specified time"""
        job = SimpleJob(name, csv_path, 'daily', time)
        
        schedule.every().day.at(time).do(
            self._run_job, job_name=name
        ).tag(name)
        
        self.jobs[name] = job
        self.logger.info(f"✅ Added daily job '{name}' at {time}")
    
    def add_hourly_job(self, name: str, csv_path: str, hours: int = 1):
        """Add hourly job - runs every X hours"""
        job = SimpleJob(name, csv_path, 'hourly', str(hours))
        
        schedule.every(hours).hours.do(
            self._run_job, job_name=name
        ).tag(name)
        
        self.jobs[name] = job
        self.logger.info(f"✅ Added hourly job '{name}' every {hours} hour(s)")
    
    def add_weekly_job(self, name: str, csv_path: str, day: str = "monday", time: str = "02:00"):
        """Add weekly job - runs every week on specified day"""
        job = SimpleJob(name, csv_path, 'weekly', f"{day}@{time}")
        
        getattr(schedule.every(), day.lower()).at(time).do(
            self._run_job, job_name=name
        ).tag(name)
        
        self.jobs[name] = job
        self.logger.info(f"✅ Added weekly job '{name}' on {day} at {time}")
    
    def _run_job(self, job_name: str):
        """Execute a scheduled job"""
        job = self.jobs.get(job_name)
        if not job or not job.enabled:
            return
        
        try:
            self.logger.info(f"🚀 Starting job: {job_name}")
            job.last_run = datetime.utcnow()
            
            # Run the ETL pipeline
            metrics = run_retail_csv_etl(
                csv_file_path=job.csv_path,
                job_name=f"{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            self.logger.info(f"✅ Job '{job_name}' completed successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Job '{job_name}' failed: {e}")
    
    def start(self):
        """Start the scheduler"""
        if self.is_running:
            self.logger.warning("Scheduler already running")
            return
        
        self.is_running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        self.logger.info("🚀 ETL Scheduler started")
    
    def stop(self):
        """Stop the scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        self.logger.info("⏹️ ETL Scheduler stopped")
    
    def _run_scheduler(self):
        """Main scheduler loop"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(30)  # Check every 30 seconds
    
    def list_jobs(self) -> List[SimpleJob]:
        """List all jobs"""
        return list(self.jobs.values())
    
    def enable_job(self, name: str) -> bool:
        """Enable a job"""
        if name in self.jobs:
            self.jobs[name].enabled = True
            self.logger.info(f"✅ Enabled job: {name}")
            return True
        return False
    
    def disable_job(self, name: str) -> bool:
        """Disable a job"""
        if name in self.jobs:
            self.jobs[name].enabled = False
            self.logger.info(f"❌ Disabled job: {name}")
            return True
        return False
    
    def remove_job(self, name: str) -> bool:
        """Remove a job"""
        if name in self.jobs:
            schedule.clear(name)  
            del self.jobs[name]  
            self.logger.info(f"🗑️ Removed job: {name}")
            return True
        return False
    
    def get_next_runs(self) -> List[Dict]:
        """Get next run times for all jobs"""
        next_runs = []
        for job in schedule.jobs:
            next_runs.append({
                'job': job.tags,
                'next_run': job.next_run.strftime('%Y-%m-%d %H:%M:%S') if job.next_run else 'Not scheduled'
            })
        return next_runs

# Global scheduler instance
scheduler = ETLScheduler()