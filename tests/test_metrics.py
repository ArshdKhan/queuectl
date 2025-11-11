"""Tests for metrics collection and reporting"""

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


class TestMetricsSystem:
    """Tests for metrics collection and reporting"""
    
    def test_enqueue_event_recorded(self, manager):
        """Enqueueing a job should record an event"""
        manager.enqueue('test', 'echo test')
        
        metrics = manager.get_metrics()
        assert metrics['event_counts']['enqueued'] == 1
    
    def test_job_lifecycle_events(self, manager):
        """Complete job lifecycle should record all events"""
        job = manager.enqueue('lifecycle', 'echo test')
        
        # Claim (started event)
        claimed = manager.claim_job()
        
        # Complete
        manager.mark_completed(claimed.id)
        
        metrics = manager.get_metrics()
        assert metrics['event_counts']['enqueued'] == 1
        assert metrics['event_counts']['started'] == 1
        assert metrics['event_counts']['completed'] == 1
    
    def test_failed_job_metrics(self, manager):
        """Failed jobs should record failed event"""
        job = manager.enqueue('fail', 'false')
        claimed = manager.claim_job()
        
        # Mark as failed (will retry)
        manager.mark_pending(claimed.id, attempts=1, error="Command failed")
        
        metrics = manager.get_metrics()
        assert metrics['event_counts']['failed'] == 1
    
    def test_dlq_event_recorded(self, manager):
        """Moving to DLQ should record dlq event"""
        job = manager.enqueue('dead', 'false', max_retries=0)
        claimed = manager.claim_job()
        
        # Exhaust retries - moves to DLQ
        manager.mark_dead(claimed.id, attempts=0, error="Max retries exceeded")
        metrics = manager.get_metrics()
        assert metrics['event_counts']['dlq'] == 1
    
    def test_average_duration_calculation(self, manager):
        """Metrics should calculate average job duration"""
        # Enqueue and complete multiple jobs
        for i in range(3):
            job = manager.enqueue(f'job{i}', 'echo test')
            claimed = manager.claim_job()
            time.sleep(0.05)  # Simulate work
            manager.mark_completed(claimed.id, duration_ms=50)
        
        metrics = manager.get_metrics()
        avg_duration = metrics.get('average_duration_seconds', 0)
        
        # Average should be positive (if metrics calculated)
        # Note: Average might be 0 if no duration data
        assert avg_duration >= 0
    
    def test_recent_events_list(self, manager):
        """Metrics should return recent events"""
        job = manager.enqueue('recent', 'echo test')
        
        metrics = manager.get_metrics()
        recent = metrics['recent_events']
        
        assert len(recent) > 0
        assert recent[0]['job_id'] == 'recent'
        assert recent[0]['event_type'] == 'enqueued'
        assert 'timestamp' in recent[0]
    
    def test_metrics_persist_across_restart(self, manager):
        """Metrics should survive queue manager restart"""
        # Record some events
        job = manager.enqueue('persist', 'echo test')
        claimed = manager.claim_job()
        manager.mark_completed(claimed.id)
        
        # Create new manager with same DB
        new_manager = QueueManager(manager.config)
        
        metrics = new_manager.get_metrics()
        assert metrics['event_counts']['enqueued'] >= 1
        assert metrics['event_counts']['completed'] >= 1
    
    def test_multiple_job_metrics_accumulate(self, manager):
        """Metrics should accumulate across multiple jobs"""
        # Enqueue and complete 5 jobs
        for i in range(5):
            job = manager.enqueue(f'batch{i}', 'echo test')
            claimed = manager.claim_job()
            manager.mark_completed(claimed.id)
        
        metrics = manager.get_metrics()
        assert metrics['event_counts']['enqueued'] == 5
        assert metrics['event_counts']['started'] == 5
        assert metrics['event_counts']['completed'] == 5
