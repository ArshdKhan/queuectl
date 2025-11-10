"""Worker pool with multiprocessing and health monitoring"""

import multiprocessing as mp
from multiprocessing.synchronize import Event as EventType
import os
import signal
import time
from pathlib import Path
from typing import List, Optional, Any
from datetime import datetime

from queuectl.config import Config
from queuectl.queue import QueueManager
from queuectl.utils import setup_logger
from queuectl.worker.executor import JobExecutor


def _worker_loop_func(worker_id: int, shutdown_event: EventType) -> None:
    """Module-level worker function to avoid pickle issues with bound methods"""
    # Recreate QueueManager and Config in worker process
    config = Config.load()
    queue_manager = QueueManager(config=config)
    
    logger = setup_logger(f"worker-{worker_id}")
    executor = JobExecutor(timeout=config.job_timeout)

    # Setup signal handlers
    signal.signal(signal.SIGINT, lambda s, f: shutdown_event.set())
    signal.signal(signal.SIGTERM, lambda s, f: shutdown_event.set())
    
    logger.info(f"Worker {worker_id} started (PID: {os.getpid()})")

    while not shutdown_event.is_set():
        try:
            job = queue_manager.claim_job()

            if not job:
                time.sleep(config.worker_poll_interval)
                continue

            logger.info(f"Processing job {job.id}: {job.command}")
            success, error = executor.execute(job.command)

            if success:
                queue_manager.mark_completed(job.id)
                logger.info(f"Job {job.id} completed successfully")
            else:
                job.attempts += 1
                logger.warning(f"Job {job.id} failed: {error} (attempt {job.attempts}/{job.max_retries})")

                if job.should_retry():
                    backoff = job.calculate_backoff(config.backoff_base)
                    logger.info(f"Retrying job {job.id} after {backoff}s backoff")
                    time.sleep(backoff)
                    queue_manager.mark_pending(job.id, job.attempts, error)
                else:
                    logger.error(f"Job {job.id} moved to DLQ after {job.attempts} failed attempts")
                    queue_manager.mark_dead(job.id, job.attempts, error)
                    
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
            break
        except Exception as e:
            logger.error(f"Unexpected worker error: {e}", exc_info=True)
            time.sleep(1)

    logger.info(f"Worker {worker_id} shutting down gracefully")


class WorkerHealthMonitor:
    """Monitors worker health with heartbeat tracking"""
    
    def __init__(self, worker_id: int, queue_manager: QueueManager):
        self.worker_id = worker_id
        self.queue_manager = queue_manager
        self.last_heartbeat = mp.Value('d', time.time())
        self.jobs_processed = mp.Value('i', 0)
    
    def heartbeat(self) -> None:
        """Update heartbeat timestamp"""
        with self.last_heartbeat.get_lock():
            self.last_heartbeat.value = time.time()
    
    def increment_jobs(self) -> None:
        """Increment processed jobs counter"""
        with self.jobs_processed.get_lock():
            self.jobs_processed.value += 1
    
    def get_stats(self) -> dict:
        """Get worker statistics"""
        return {
            'worker_id': self.worker_id,
            'last_heartbeat': self.last_heartbeat.value,
            'jobs_processed': self.jobs_processed.value,
            'alive': time.time() - self.last_heartbeat.value < 60
        }


class WorkerPool:
    """Manages multiple worker processes with health monitoring"""

    def __init__(self, queue_manager: QueueManager, config: Config, count: int = 1):
        self.queue_manager = queue_manager
        self.config = config
        self.count = count
        self.processes: List[mp.Process] = []
        self.health_monitors: List[WorkerHealthMonitor] = []
        self.shutdown_event = mp.Event()

    def start(self, daemon: bool = False) -> None:
        """Start worker processes with health monitoring"""
        if daemon:
            self._write_pid_file()

        for i in range(self.count):
            # Use module-level function to avoid pickling self
            p = mp.Process(target=_worker_loop_func, args=(i, self.shutdown_event), daemon=daemon)
            p.start()
            self.processes.append(p)

        if not daemon:
            self._wait_for_workers()

    def _write_pid_file(self) -> None:
        """Write worker PIDs to file for later shutdown"""
        pid_file = Path.home() / ".queuectl" / "workers.pid"
        pid_file.parent.mkdir(exist_ok=True)
        
        with open(pid_file, 'w') as f:
            f.write(f"{os.getpid()}\n")
            for p in self.processes:
                f.write(f"{p.pid}\n")

    def _wait_for_workers(self) -> None:
        """Wait for all workers to complete"""
        try:
            for p in self.processes:
                p.join()
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        """Signal all workers to stop gracefully"""
        logger = setup_logger("pool")
        logger.info(f"Stopping {len(self.processes)} worker(s)...")
        
        self.shutdown_event.set()
        
        for i, p in enumerate(self.processes):
            p.join(timeout=30)
            if p.is_alive():
                logger.warning(f"Worker {i} did not stop gracefully, terminating...")
                p.terminate()
                p.join(timeout=5)
                if p.is_alive():
                    logger.error(f"Worker {i} did not terminate, killing...")
                    p.kill()
        
        logger.info("All workers stopped")
    
    def get_health_status(self) -> List[dict]:
        """Get health status of all workers"""
        return [monitor.get_stats() for monitor in self.health_monitors]


