"""Tests for scheduled job functionality"""

import pytest
import time
from datetime import datetime, timedelta
from queuectl.config import Config
from queuectl.queue import QueueManager


@pytest.fixture
def manager(tmp_path):
    """Create test queue manager"""
    config = Config(
        max_retries=3,
        backoff_base=2.0,
        db_path=str(tmp_path / "test.db"),
        worker_poll_interval=0.1,
        job_timeout=5
    )
    return QueueManager(config)


class TestScheduledJobs:
    """Tests for scheduled job functionality"""
    
    def test_future_job_not_claimed(self, manager):
        """Jobs with future run_at should not be claimed"""
        future_time = datetime.utcnow() + timedelta(hours=1)
        manager.enqueue('future', 'echo future', run_at=future_time)
        
        # Should not be claimed yet
        claimed = manager.claim_job()
        assert claimed is None
    
    def test_past_job_is_claimed(self, manager):
        """Jobs with past run_at should be claimed immediately"""
        past_time = datetime.utcnow() - timedelta(hours=1)
        manager.enqueue('past', 'echo past', run_at=past_time)
        
        # Should be claimed
        claimed = manager.claim_job()
        assert claimed is not None
        assert claimed.id == 'past'
    
    def test_null_run_at_claimed_immediately(self, manager):
        """Jobs with no run_at should be claimed immediately"""
        manager.enqueue('immediate', 'echo now', run_at=None)
        
        claimed = manager.claim_job()
        assert claimed is not None
        assert claimed.id == 'immediate'
    
    def test_scheduled_job_becomes_claimable(self, manager):
        """Job should become claimable when run_at time arrives"""
        # Schedule job 100ms in future
        near_future = datetime.utcnow() + timedelta(milliseconds=100)
        manager.enqueue('scheduled', 'echo scheduled', run_at=near_future)
        
        # Not claimable yet
        assert manager.claim_job() is None
        
        # Wait for scheduled time
        time.sleep(0.15)
        
        # Now claimable
        claimed = manager.claim_job()
        assert claimed is not None
        assert claimed.id == 'scheduled'
    
    def test_scheduled_with_priority(self, manager):
        """Scheduled jobs should respect priority when ready"""
        now = datetime.utcnow()
        
        # Two jobs ready now, different priorities
        manager.enqueue('low', 'echo', priority=3, run_at=now - timedelta(seconds=1))
        manager.enqueue('high', 'echo', priority=8, run_at=now - timedelta(seconds=1))
        
        # High priority should be claimed first
        first = manager.claim_job()
        assert first.id == 'high'
    
    def test_scheduled_with_metrics(self, manager):
        """Scheduled jobs should record metrics when claimed"""
        past = datetime.utcnow() - timedelta(seconds=1)
        manager.enqueue('scheduled', 'echo', run_at=past)
        
        claimed = manager.claim_job()
        assert claimed is not None
        
        # Metrics should show enqueued and started
        metrics = manager.get_metrics()
        assert metrics['event_counts']['enqueued'] == 1
        assert metrics['event_counts']['started'] == 1
    
    def test_priority_scheduled_with_metrics(self, manager):
        """Priority, scheduling, and metrics should work together"""
        now = datetime.utcnow()
        
        # High priority, scheduled in past (ready)
        job1 = manager.enqueue('urgent', 'echo', priority=10, run_at=now - timedelta(seconds=1))
        
        # Low priority, ready now
        job2 = manager.enqueue('normal', 'echo', priority=3, run_at=None)
        
        # High priority, scheduled in future (not ready)
        job3 = manager.enqueue('future', 'echo', priority=10, run_at=now + timedelta(hours=1))
        
        # Should claim urgent (high priority + ready)
        first = manager.claim_job()
        assert first.id == 'urgent'
        
        # Should claim normal (only remaining ready job)
        second = manager.claim_job()
        assert second.id == 'normal'
        
        # Future job not claimable yet
        third = manager.claim_job()
        assert third is None
        
        # Check metrics
        metrics = manager.get_metrics()
        assert metrics['event_counts']['enqueued'] == 3
        assert metrics['event_counts']['started'] == 2
