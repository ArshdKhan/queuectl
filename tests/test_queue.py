"""Unit tests for queue operations"""

import pytest
import tempfile
from pathlib import Path

from queuectl.config import Config
from queuectl.models import Job, JobState
from queuectl.queue import QueueManager
from queuectl.exceptions import InvalidJobStateException


@pytest.fixture
def test_config():
    """Create isolated test configuration"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Config(
            db_path=str(Path(tmpdir) / "test.db"),
            max_retries=3,
            backoff_base=2.0
        )
        yield config


def test_enqueue_job(test_config):
    """Test basic job enqueue"""
    manager = QueueManager(test_config)
    job = manager.enqueue("test1", "echo 'hello'")
    
    assert job.id == "test1"
    assert job.command == "echo 'hello'"
    assert job.state == JobState.PENDING
    assert job.attempts == 0


def test_claim_job(test_config):
    """Test job claiming updates state"""
    manager = QueueManager(test_config)
    manager.enqueue("test1", "echo 'hello'")
    
    job = manager.claim_job()
    assert job is not None
    assert job.id == "test1"
    assert job.state == JobState.PROCESSING


def test_claim_job_fifo_order(test_config):
    """Test jobs are claimed in FIFO order"""
    manager = QueueManager(test_config)
    manager.enqueue("test1", "echo '1'")
    manager.enqueue("test2", "echo '2'")
    manager.enqueue("test3", "echo '3'")
    
    job1 = manager.claim_job()
    job2 = manager.claim_job()
    job3 = manager.claim_job()
    
    assert job1.id == "test1"
    assert job2.id == "test2"
    assert job3.id == "test3"


def test_no_job_available(test_config):
    """Test claiming when no jobs available"""
    manager = QueueManager(test_config)
    job = manager.claim_job()
    assert job is None


def test_mark_completed(test_config):
    """Test marking job as completed"""
    manager = QueueManager(test_config)
    manager.enqueue("test1", "echo 'hello'")
    manager.claim_job()
    manager.mark_completed("test1")
    
    job = manager.get_job("test1")
    assert job.state == JobState.COMPLETED


def test_mark_pending_for_retry(test_config):
    """Test marking job as pending for retry"""
    manager = QueueManager(test_config)
    manager.enqueue("test1", "exit 1")
    job = manager.claim_job()
    
    manager.mark_pending("test1", 1, "Exit code 1")
    
    job = manager.get_job("test1")
    assert job.state == JobState.PENDING
    assert job.attempts == 1
    assert job.error_message == "Exit code 1"


def test_mark_dead(test_config):
    """Test moving job to DLQ"""
    manager = QueueManager(test_config)
    manager.enqueue("test1", "exit 1")
    manager.claim_job()

    manager.mark_dead("test1", 3, "Max retries exceeded")
    job = manager.get_job("test1")
    assert job.state == JobState.DEAD
    assert job.error_message == "Max retries exceeded"


def test_list_jobs_by_state(test_config):
    """Test listing jobs filtered by state"""
    manager = QueueManager(test_config)
    manager.enqueue("test1", "echo '1'")
    manager.enqueue("test2", "echo '2'")
    manager.claim_job()
    
    pending = manager.list_jobs(JobState.PENDING)
    processing = manager.list_jobs(JobState.PROCESSING)
    
    assert len(pending) == 1
    assert len(processing) == 1


def test_get_stats(test_config):
    """Test getting job counts by state"""
    manager = QueueManager(test_config)
    manager.enqueue("test1", "echo '1'")
    manager.enqueue("test2", "echo '2'")
    manager.enqueue("test3", "echo '3'")
    manager.claim_job()
    
    stats = manager.get_stats()
    assert stats['pending'] == 2
    assert stats['processing'] == 1
    assert stats['completed'] == 0


def test_retry_dlq_job(test_config):
    """Test retrying DLQ job resets state"""
    manager = QueueManager(test_config)
    manager.enqueue("test1", "exit 1", max_retries=0)
    manager.claim_job()
    manager.mark_dead("test1", 1, "Failed")
    
    manager.retry_dlq_job("test1")
    
    job = manager.get_job("test1")
    assert job.state == JobState.PENDING
    assert job.attempts == 0
    assert job.error_message is None


def test_retry_non_dlq_job_raises_error(test_config):
    """Test retrying non-DLQ job raises error"""
    manager = QueueManager(test_config)
    manager.enqueue("test1", "echo 'hello'")
    
    with pytest.raises(InvalidJobStateException, match="expected state 'dead'"):
        manager.retry_dlq_job("test1")


def test_job_should_retry(test_config):
    """Test job should_retry logic"""
    job = Job(id="test", command="exit 1", max_retries=3, attempts=0)
    assert job.should_retry() is True
    
    job.attempts = 3
    assert job.should_retry() is False


def test_job_calculate_backoff(test_config):
    """Test exponential backoff calculation"""
    job = Job(id="test", command="exit 1")
    
    job.attempts = 0
    assert job.calculate_backoff(2.0) == 1.0  # 2^0
    
    job.attempts = 1
    assert job.calculate_backoff(2.0) == 2.0  # 2^1
    
    job.attempts = 2
    assert job.calculate_backoff(2.0) == 4.0  # 2^2
    
    job.attempts = 3
    assert job.calculate_backoff(2.0) == 8.0  # 2^3
