"""Job model with state management"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class JobState(Enum):
    """Job state enumeration"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    """Job model representing a background task"""
    id: str
    command: str
    state: JobState = JobState.PENDING
    attempts: int = 0
    max_retries: int = 3
    priority: int = 5  # 1-10, higher number = higher priority (default: 5)
    run_at: Optional[datetime] = None  # Scheduled execution time (None = run immediately)
    created_at: datetime = field(default_factory=lambda: datetime.utcnow())
    updated_at: datetime = field(default_factory=lambda: datetime.utcnow())
    error_message: Optional[str] = None
    last_executed_at: Optional[datetime] = None

    def should_retry(self) -> bool:
        """Check if job should be retried"""
        return self.attempts < self.max_retries

    def calculate_backoff(self, base: float = 2.0) -> float:
        """Calculate exponential backoff delay in seconds"""
        return base ** self.attempts
    
    def is_ready_to_run(self) -> bool:
        """Check if scheduled job is ready to run"""
        if self.run_at is None:
            return True
        return datetime.utcnow() >= self.run_at

    def to_dict(self) -> dict:
        """Serialize job to dictionary for storage"""
        return {
            'id': self.id,
            'command': self.command,
            'state': self.state.value,
            'attempts': self.attempts,
            'max_retries': self.max_retries,
            'priority': self.priority,
            'run_at': self.run_at.isoformat() if self.run_at else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'error_message': self.error_message,
            'last_executed_at': self.last_executed_at.isoformat() if self.last_executed_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Job':
        """Deserialize job from dictionary"""
        return cls(
            id=data['id'],
            command=data['command'],
            state=JobState(data['state']),
            attempts=data['attempts'],
            max_retries=data['max_retries'],
            priority=data.get('priority', 5),
            run_at=datetime.fromisoformat(data['run_at']) if data.get('run_at') else None,
            created_at=datetime.fromisoformat(data['created_at']),
            updated_at=datetime.fromisoformat(data['updated_at']),
            error_message=data.get('error_message'),
            last_executed_at=datetime.fromisoformat(data['last_executed_at']) if data.get('last_executed_at') else None,
        )
