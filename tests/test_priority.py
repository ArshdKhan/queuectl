"""Tests for priority queue functionality"""

import pytest
import time
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


class TestPriorityQueues:
    """Tests for priority queue functionality"""
    
    def test_high_priority_claimed_first(self, manager):
        """High priority jobs should be claimed before low priority"""
        # Enqueue jobs in mixed priority order
        low = manager.enqueue('low', 'echo low', priority=2)
        high = manager.enqueue('high', 'echo high', priority=9)
        medium = manager.enqueue('medium', 'echo medium', priority=5)
        
        # Claim jobs - should get highest priority first
        first = manager.claim_job()
        assert first.id == 'high'
        assert first.priority == 9
        
        second = manager.claim_job()
        assert second.id == 'medium'
        assert second.priority == 5
        
        third = manager.claim_job()
        assert third.id == 'low'
        assert third.priority == 2
    
    def test_fifo_within_same_priority(self, manager):
        """Within same priority, FIFO order should be maintained"""
        job1 = manager.enqueue('first', 'echo 1', priority=5)
        time.sleep(0.01)  # Ensure different timestamps
        job2 = manager.enqueue('second', 'echo 2', priority=5)
        time.sleep(0.01)
        job3 = manager.enqueue('third', 'echo 3', priority=5)
        
        # All have same priority, should be FIFO
        assert manager.claim_job().id == 'first'
        assert manager.claim_job().id == 'second'
        assert manager.claim_job().id == 'third'
    
    def test_priority_bounds(self, manager):
        """Priority should be constrained between 1-10"""
        # Valid priorities
        job_min = manager.enqueue('min', 'echo', priority=1)
        assert job_min.priority == 1
        
        job_max = manager.enqueue('max', 'echo', priority=10)
        assert job_max.priority == 10
        
        # Default priority
        job_default = manager.enqueue('default', 'echo')
        assert job_default.priority == 5
    
    def test_priority_persists_across_restart(self, manager):
        """Priority should survive queue manager restart"""
        manager.enqueue('high', 'echo', priority=9)
        manager.enqueue('low', 'echo', priority=2)
        
        # Create new manager with same DB
        new_manager = QueueManager(manager.config)
        
        # Priority order should be maintained
        first = new_manager.claim_job()
        assert first.id == 'high'
        assert first.priority == 9
    
    def test_priority_with_metrics(self, manager):
        """Priority queues should work with metrics tracking"""
        manager.enqueue('low', 'echo', priority=2)
        manager.enqueue('high', 'echo', priority=9)
        
        # Claim high priority
        claimed = manager.claim_job()
        assert claimed.id == 'high'
        
        # Check metrics recorded
        metrics = manager.get_metrics()
        assert metrics['event_counts']['enqueued'] == 2
        assert metrics['event_counts']['started'] == 1
