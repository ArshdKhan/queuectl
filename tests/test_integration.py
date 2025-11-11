"""Integration tests for end-to-end workflows"""

import pytest
import time
import tempfile
from pathlib import Path

from queuectl.config import Config
from queuectl.models import JobState
from queuectl.queue import QueueManager


@pytest.fixture
def test_config():
    """Create isolated test configuration"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Config(
            db_path=str(Path(tmpdir) / "test.db"),
            max_retries=3,
            backoff_base=2.0,
            worker_poll_interval=0.1
        )
        yield config


def test_job_persistence_across_restart(test_config):
    """Test jobs survive system restart"""
    # Create manager and enqueue job
    manager1 = QueueManager(test_config)
    manager1.enqueue("persistent1", "echo 'test'")
    manager1.enqueue("persistent2", "echo 'test2'")
    
    # Simulate restart by creating new manager with same config
    del manager1
    
    manager2 = QueueManager(test_config)
    jobs = manager2.list_jobs(JobState.PENDING)
    
    assert len(jobs) == 2
    assert {j.id for j in jobs} == {"persistent1", "persistent2"}


def test_failed_job_moves_to_dlq(test_config):
    """Test job moves to DLQ after max retries"""
    config = Config(
        db_path=test_config.db_path,
        max_retries=2,
        backoff_base=2.0
    )
    manager = QueueManager(config)
    
    # Enqueue failing job
    job = manager.enqueue("fail_job", "exit 1", max_retries=2)
    
    # Simulate worker processing with retries
    for attempt in range(3):  # Initial + 2 retries
        claimed = manager.claim_job()
        assert claimed is not None
        
        if attempt < 2:
            # Retry
            manager.mark_pending(claimed.id, attempt + 1, f"Failed attempt {attempt + 1}")
        else:
            # Final failure - move to DLQ
            manager.mark_dead(claimed.id, attempt + 1, "Max retries exceeded")
    
    # Verify job in DLQ
    dlq_jobs = manager.list_jobs(JobState.DEAD)
    assert len(dlq_jobs) == 1
    assert dlq_jobs[0].id == "fail_job"
    assert dlq_jobs[0].attempts == 3  # 3 attempts total (initial + 2 retries)


def test_successful_job_lifecycle(test_config):
    """Test complete lifecycle of successful job"""
    manager = QueueManager(test_config)
    
    # Enqueue
    job = manager.enqueue("success_job", "echo 'hello'")
    assert job.state == JobState.PENDING
    
    # Claim
    claimed = manager.claim_job()
    assert claimed.id == "success_job"
    assert claimed.state == JobState.PROCESSING
    
    # Complete
    manager.mark_completed("success_job")
    
    # Verify
    final_job = manager.get_job("success_job")
    assert final_job.state == JobState.COMPLETED


def test_multiple_jobs_processed_sequentially(test_config):
    """Test multiple jobs are processed without overlap"""
    manager = QueueManager(test_config)
    
    # Enqueue multiple jobs
    for i in range(5):
        manager.enqueue(f"job{i}", f"echo '{i}'")
    
    # Process all jobs
    completed = 0
    while True:
        job = manager.claim_job()
        if not job:
            break
        manager.mark_completed(job.id)
        completed += 1
    
    assert completed == 5
    
    # Verify all completed
    stats = manager.get_stats()
    assert stats['completed'] == 5
    assert stats['pending'] == 0


def test_dlq_retry_functionality(test_config):
    """Test retrying DLQ job works correctly"""
    manager = QueueManager(test_config)
    
    # Create DLQ job
    manager.enqueue("dlq_job", "exit 1", max_retries=0)
    job = manager.claim_job()
    manager.mark_dead("dlq_job", 1, "Failed")
    
    # Verify in DLQ
    assert manager.get_job("dlq_job").state == JobState.DEAD
    
    # Retry
    manager.retry_dlq_job("dlq_job")
    
    # Verify back in pending with reset attempts
    retried = manager.get_job("dlq_job")
    assert retried.state == JobState.PENDING
    assert retried.attempts == 0
    
    # Should be claimable again
    claimed = manager.claim_job()
    assert claimed.id == "dlq_job"


def test_config_persistence(test_config):
    """Test configuration saves and loads correctly"""
    config_file = Path.home() / ".queuectl" / "config_test.json"
    
    try:
        # Save config
        config = Config(
            max_retries=5,
            backoff_base=3.0,
            db_path="test.db"
        )
        Config.CONFIG_FILE = config_file
        config.save()
        
        # Load config
        loaded = Config.load()
        assert loaded.max_retries == 5
        assert loaded.backoff_base == 3.0
    
    finally:
        if config_file.exists():
            config_file.unlink()
        # Restore default
        Config.CONFIG_FILE = Path.home() / ".queuectl" / "config.json"


def test_exponential_backoff_timing(test_config):
    """Test retry backoff increases exponentially"""
    from queuectl.models import Job
    
    job = Job(id="test", command="exit 1", max_retries=3)
    
    # Test backoff calculation
    delays = []
    for i in range(3):
        job.attempts = i
        delays.append(job.calculate_backoff(2.0))
    
    # Verify exponential growth: 1s, 2s, 4s
    assert delays == [1.0, 2.0, 4.0]
