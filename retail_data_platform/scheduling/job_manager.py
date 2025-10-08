"""
Simple Job Manager
"""
from .scheduler import scheduler

class JobManager:
    """Simple job management interface"""
    
    def create_daily_job(self, name: str, csv_path: str, time: str = "02:00"):
        """Create daily job"""
        scheduler.add_daily_job(name, csv_path, time)
        print(f"✅ Created daily job: {name} at {time}")
    
    def create_hourly_job(self, name: str, csv_path: str, hours: int = 1):
        """Create hourly job"""
        scheduler.add_hourly_job(name, csv_path, hours)
        print(f"✅ Created hourly job: {name} every {hours} hour(s)")
    
    def create_weekly_job(self, name: str, csv_path: str, day: str = "monday", time: str = "02:00"):
        """Create weekly job"""
        scheduler.add_weekly_job(name, csv_path, day, time)
        print(f"✅ Created weekly job: {name} on {day} at {time}")
    
    def list_jobs(self):
        """List all jobs"""
        jobs = scheduler.list_jobs()
        
        if not jobs:
            print("📋 No scheduled jobs")
            return
        
        print(f"📋 Scheduled Jobs ({len(jobs)} total):")
        print("-" * 60)
        
        for job in jobs:
            status = "✅ Enabled" if job.enabled else "❌ Disabled"
            last_run = job.last_run.strftime('%Y-%m-%d %H:%M:%S') if job.last_run else "Never"
            
            print(f"📄 {job.name}")
            print(f"   Type: {job.schedule_type}")
            print(f"   Time: {job.time}")
            print(f"   CSV: {job.csv_path}")
            print(f"   Status: {status}")
            print(f"   Last Run: {last_run}")
            print()
        
        # Show next runs
        next_runs = scheduler.get_next_runs()
        if next_runs:
            print("⏰ Next Scheduled Runs:")
            for run in next_runs:
                print(f"   {run['job']}: {run['next_run']}")
    
    def enable_job(self, name: str):
        """Enable job"""
        if scheduler.enable_job(name):
            print(f"✅ Enabled: {name}")
        else:
            print(f"❌ Job not found: {name}")
    
    def disable_job(self, name: str):
        """Disable job"""
        if scheduler.disable_job(name):
            print(f"❌ Disabled: {name}")
        else:
            print(f"❌ Job not found: {name}")
    
    def remove_job(self, name: str):
        """Remove job"""
        if scheduler.remove_job(name):
            print(f"🗑️ Removed: {name}")
        else:
            print(f"❌ Job not found: {name}")
    
    def start_scheduler(self):
        """Start scheduler"""
        scheduler.start()
        print("🚀 Scheduler started")
    
    def stop_scheduler(self):
        """Stop scheduler"""
        scheduler.stop()
        print("⏹️ Scheduler stopped")

# Global manager instance  
job_manager = JobManager()