"""Queue manager - facade over storage layer"""

from datetime import datetime
from typing import Optional, List, Dict, Any

from queuectl.models import Job, JobState
from queuectl.storage import SQLiteStorage
from queuectl.exceptions import JobNotFoundException, InvalidJobStateException
from queuectl.config import Config


class QueueManager:
    """Manages job queue operations"""

    def __init__(self, config: Config):
        self.storage = SQLiteStorage(config.db_path)
        self.config = config

    def enqueue(self, job_id: str, command: str, max_retries: Optional[int] = None, 
                priority: Optional[int] = None, run_at: Optional[datetime] = None) -> Job:
        """Add new job to queue with optional priority and scheduling"""
        job = Job(
            id=job_id,
            command=command,
            max_retries=max_retries if max_retries is not None else self.config.max_retries,
            priority=priority if priority is not None else 5,
            run_at=run_at,
        )
        self.storage.insert_job(job)
        return job

    def claim_job(self) -> Optional[Job]:
        """Atomically claim next pending job"""
        return self.storage.claim_job()

    def mark_completed(self, job_id: str, duration_ms: Optional[int] = None) -> None:
        """Mark job as completed and record metric"""
        self.storage.update_job(job_id, {
            'state': JobState.COMPLETED.value,
            'updated_at': datetime.utcnow().isoformat(),
        })
        # Record completion metric via storage
        with self.storage._transaction() as conn:
            self.storage._record_metric(conn, job_id, 'completed', duration_ms=duration_ms)

    def mark_pending(self, job_id: str, attempts: int, error: str) -> None:
        """Return job to pending for retry"""
        self.storage.update_job(job_id, {
            'state': JobState.PENDING.value,
            'attempts': attempts,
            'error_message': error,
            'updated_at': datetime.utcnow().isoformat(),
        })
        # Record failed attempt metric
        with self.storage._transaction() as conn:
            self.storage._record_metric(conn, job_id, 'failed', error_message=error)

    def mark_dead(self, job_id: str, attempts: int, error: str) -> None:
        """Move job to DLQ and record metric"""
        self.storage.update_job(job_id, {
            'state': JobState.DEAD.value,
            'attempts': attempts,
            'error_message': error,
            'updated_at': datetime.utcnow().isoformat(),
        })
        # Record DLQ metric
        with self.storage._transaction() as conn:
            self.storage._record_metric(conn, job_id, 'dlq', error_message=error)

    def get_stats(self) -> Dict[str, int]:
        """Return counts by state"""
        return self.storage.get_job_counts()

    def list_jobs(self, state: Optional[JobState] = None) -> List[Job]:
        """List jobs, optionally filtered by state"""
        return self.storage.list_jobs(state)

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID"""
        return self.storage.get_job(job_id)

    def retry_dlq_job(self, job_id: str) -> None:
        """Reset DLQ job to pending with 0 attempts"""
        job = self.storage.get_job(job_id)
        if not job:
            raise JobNotFoundException(f"Job '{job_id}' not found in queue")
        
        if job.state != JobState.DEAD:
            raise InvalidJobStateException(
                f"Cannot retry job '{job_id}': expected state 'dead', got '{job.state.value}'"
            )

        self.storage.update_job(job_id, {
            'state': JobState.PENDING.value,
            'attempts': 0,
            'error_message': None,
            'updated_at': datetime.utcnow().isoformat(),
        })
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get metrics summary including historical statistics"""
        return self.storage.get_metrics_summary()
