"""
Tests for transaction isolation and concurrency
"""

import pytest
import threading
import time
from pathlib import Path

from queuectl.config import Config
from queuectl.queue import QueueManager
from queuectl.models import JobState


def test_concurrent_job_claiming(tmp_path):
    """Test that concurrent workers don't claim the same job"""
    db_path = tmp_path / "test.db"
    config = Config(db_path=str(db_path))
    manager = QueueManager(config)
    
    # Enqueue 5 jobs
    for i in range(5):
        manager.enqueue(f"job{i}", f"echo {i}")
    
    claimed_jobs = []
    lock = threading.Lock()
    
    def claim_worker():
        """Worker that tries to claim jobs"""
        for _ in range(3):
            job = manager.claim_job()
            if job:
                with lock:
                    claimed_jobs.append(job.id)
            time.sleep(0.01)
    
    # Start 3 concurrent workers
    threads = []
    for _ in range(3):
        t = threading.Thread(target=claim_worker)
        threads.append(t)
        t.start()
    
    # Wait for all threads
    for t in threads:
        t.join()
    
    # Verify no duplicate claims
    assert len(claimed_jobs) == len(set(claimed_jobs)), "Jobs were claimed multiple times"
    assert len(claimed_jobs) == 5, "Not all jobs were claimed"


def test_transaction_rollback_on_error(tmp_path):
    """Test that failed transactions roll back properly"""
    db_path = tmp_path / "test.db"
    config = Config(db_path=str(db_path))
    manager = QueueManager(config)
    
    manager.enqueue("job1", "echo test")
    
    # Get the storage layer
    storage = manager.storage
    
    # Try to cause an error during transaction
    try:
        with storage._transaction() as conn:
            conn.execute("UPDATE jobs SET state = 'processing' WHERE id = 'job1'")
            # Simulate error
            raise RuntimeError("Simulated error")
    except RuntimeError:
        pass
    
    # Verify job state wasn't changed
    job = manager.get_job("job1")
    assert job.state == JobState.PENDING, "Transaction did not roll back"


def test_immediate_lock_acquisition(tmp_path):
    """Test that BEGIN IMMEDIATE acquires write lock immediately"""
    db_path = tmp_path / "test.db"
    config = Config(db_path=str(db_path))
    manager = QueueManager(config)
    
    manager.enqueue("job1", "echo test")
    
    storage = manager.storage
    results = []
    
    def transaction_with_delay():
        """Hold transaction open for a while"""
        try:
            with storage._transaction() as conn:
                conn.execute("SELECT * FROM jobs WHERE id = 'job1'")
                time.sleep(0.2)  # Hold lock
                conn.execute("UPDATE jobs SET state = 'processing' WHERE id = 'job1'")
                results.append("first")
        except Exception as e:
            results.append(f"first_error: {e}")
    
    def competing_transaction():
        """Try to acquire lock while first transaction holds it"""
        time.sleep(0.05)  # Let first transaction start
        try:
            with storage._transaction() as conn:
                # This should block until first transaction commits
                conn.execute("SELECT * FROM jobs WHERE id = 'job1'")
                results.append("second")
        except Exception as e:
            results.append(f"second_error: {e}")
    
    t1 = threading.Thread(target=transaction_with_delay)
    t2 = threading.Thread(target=competing_transaction)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # Both should succeed, but first should complete before second
    assert "first" in results
    assert "second" in results
    assert results.index("first") < results.index("second"), "Transactions executed out of order"


def test_high_concurrency_stress(tmp_path):
    """Stress test with many concurrent workers"""
    db_path = tmp_path / "test.db"
    config = Config(db_path=str(db_path))
    manager = QueueManager(config)
    
    # Enqueue 20 jobs
    num_jobs = 20
    for i in range(num_jobs):
        manager.enqueue(f"job{i}", f"echo {i}")
    
    claimed = []
    lock = threading.Lock()
    
    def aggressive_worker():
        """Worker that aggressively claims jobs"""
        while True:
            job = manager.claim_job()
            if job:
                with lock:
                    claimed.append(job.id)
            else:
                break
    
    # Start 10 concurrent workers
    threads = []
    for _ in range(10):
        t = threading.Thread(target=aggressive_worker)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join(timeout=5)
    
    # Verify all jobs claimed exactly once
    assert len(claimed) == num_jobs
    assert len(set(claimed)) == num_jobs, "Duplicate job claims detected"
