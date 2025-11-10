"""Abstract storage interface"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict
from queuectl.models import Job, JobState


class StorageInterface(ABC):
    """Abstract base class for job storage implementations"""

    @abstractmethod
    def insert_job(self, job: Job) -> None:
        """Insert a new job into storage"""
        pass

    @abstractmethod
    def claim_job(self) -> Optional[Job]:
        """Atomically claim a pending job"""
        pass

    @abstractmethod
    def update_job_state(self, job_id: str, state: JobState) -> None:
        """Update job state"""
        pass

    @abstractmethod
    def update_job(self, job_id: str, updates: dict) -> None:
        """Update job with arbitrary fields"""
        pass

    @abstractmethod
    def get_job(self, job_id: str) -> Optional[Job]:
        """Retrieve a job by ID"""
        pass

    @abstractmethod
    def list_jobs(self, state: Optional[JobState] = None) -> List[Job]:
        """List all jobs, optionally filtered by state"""
        pass

    @abstractmethod
    def get_job_counts(self) -> Dict[str, int]:
        """Get count of jobs by state"""
        pass
