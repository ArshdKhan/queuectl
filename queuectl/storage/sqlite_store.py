"""SQLite storage implementation with atomic job claiming"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

from queuectl.models import Job, JobState
from queuectl.storage.base import StorageInterface


class SQLiteStorage(StorageInterface):
    """SQLite-based storage for jobs with atomic claiming"""

    def __init__(self, db_path: str = "queuectl.db") -> None:
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database schema with priority and scheduling support"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('pending','processing','completed','failed','dead')),
                    attempts INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    priority INTEGER DEFAULT 5 CHECK(priority BETWEEN 1 AND 10),
                    run_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    error_message TEXT,
                    last_executed_at TEXT
                )
            """)
            
            # Add new columns if they don't exist (migration)
            try:
                conn.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 5")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            try:
                conn.execute("ALTER TABLE jobs ADD COLUMN run_at TEXT")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_priority_created 
                ON jobs(state, priority DESC, created_at ASC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_runat 
                ON jobs(state, run_at)
            """)
            
            # Add metrics table for historical statistics
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    event_type TEXT NOT NULL CHECK(event_type IN ('enqueued','started','completed','failed','dlq')),
                    timestamp TEXT NOT NULL,
                    duration_ms INTEGER,
                    error_message TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_timestamp 
                ON job_metrics(timestamp DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_jobid 
                ON job_metrics(job_id)
            """)
            
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def _transaction(self):  # type: ignore[no-untyped-def]
        """
        Context manager for atomic transactions using BEGIN IMMEDIATE.
        
        Transaction Isolation:
        - Uses BEGIN IMMEDIATE to acquire write lock immediately
        - Prevents write-write conflicts between concurrent workers
        - ACID compliant: atomic claim_job() prevents duplicate processing
        
        SQLite Isolation Levels:
        - DEFERRED (default): Lock acquired on first write
        - IMMEDIATE (used here): Lock acquired immediately on BEGIN
        - EXCLUSIVE: Lock acquired for entire database
        
        For high-concurrency scenarios:
        - BEGIN IMMEDIATE is sufficient for single-machine queues
        - SERIALIZABLE isolation can be achieved with EXCLUSIVE locks
        - For distributed systems, use PostgreSQL or Redis
        """
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        """Convert database row to Job object"""
        try:
            priority = row['priority']
        except IndexError:
            priority = 5
        
        try:
            run_at = row['run_at']
        except IndexError:
            run_at = None
        
        return Job.from_dict({
            'id': row['id'],
            'command': row['command'],
            'state': row['state'],
            'attempts': row['attempts'],
            'max_retries': row['max_retries'],
            'priority': priority,
            'run_at': run_at,
            'created_at': row['created_at'],
            'updated_at': row['updated_at'],
            'error_message': row['error_message'],
            'last_executed_at': row['last_executed_at'],
        })

    def insert_job(self, job: Job) -> None:
        """Insert a new job into storage"""
        with self._transaction() as conn:
            conn.execute("""
                INSERT INTO jobs (id, command, state, attempts, max_retries, priority, run_at,
                                  created_at, updated_at, error_message, last_executed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.id,
                job.command,
                job.state.value,
                job.attempts,
                job.max_retries,
                job.priority,
                job.run_at.isoformat() if job.run_at else None,
                job.created_at.isoformat(),
                job.updated_at.isoformat(),
                job.error_message,
                job.last_executed_at.isoformat() if job.last_executed_at else None,
            ))
            # Record enqueue metric
            self._record_metric(conn, job.id, 'enqueued')

    def claim_job(self) -> Optional[Job]:
        """Atomically claim a pending job with priority and scheduling support"""
        with self._transaction() as conn:
            cursor = conn.execute("""
                SELECT * FROM jobs 
                WHERE state = 'pending' 
                AND (run_at IS NULL OR run_at <= ?)
                ORDER BY priority DESC, created_at ASC 
                LIMIT 1
            """, (datetime.utcnow().isoformat(),))
            row = cursor.fetchone()
            
            if row:
                job = self._row_to_job(row)
                conn.execute("""
                    UPDATE jobs 
                    SET state = 'processing', updated_at = ? 
                    WHERE id = ?
                """, (datetime.utcnow().isoformat(), job.id))
                job.state = JobState.PROCESSING
                job.updated_at = datetime.utcnow()
                # Record start metric
                self._record_metric(conn, job.id, 'started')
                return job
            
            return None
    
    def _record_metric(self, conn: Any, job_id: str, event_type: str, duration_ms: Optional[int] = None, error_message: Optional[str] = None) -> None:
        """Record a job metric event"""
        conn.execute("""
            INSERT INTO job_metrics (job_id, event_type, timestamp, duration_ms, error_message)
            VALUES (?, ?, ?, ?, ?)
        """, (job_id, event_type, datetime.utcnow().isoformat(), duration_ms, error_message))

    def update_job_state(self, job_id: str, state: JobState) -> None:
        """Update job state"""
        with self._transaction() as conn:
            conn.execute("""
                UPDATE jobs 
                SET state = ?, updated_at = ? 
                WHERE id = ?
            """, (state.value, datetime.utcnow().isoformat(), job_id))

    def update_job(self, job_id: str, updates: dict) -> None:
        """Update job with arbitrary fields"""
        if not updates:
            return

        # Build dynamic UPDATE query
        set_clauses = ', '.join(f"{key} = ?" for key in updates.keys())
        values = list(updates.values()) + [job_id]

        with self._transaction() as conn:
            conn.execute(f"""
                UPDATE jobs 
                SET {set_clauses}
                WHERE id = ?
            """, values)

    def get_job(self, job_id: str) -> Optional[Job]:
        """Retrieve a job by ID"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute("""
                SELECT * FROM jobs WHERE id = ?
            """, (job_id,))
            row = cursor.fetchone()
            return self._row_to_job(row) if row else None
        finally:
            conn.close()

    def list_jobs(self, state: Optional[JobState] = None) -> List[Job]:
        """List all jobs, optionally filtered by state"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            if state:
                cursor = conn.execute("""
                    SELECT * FROM jobs 
                    WHERE state = ? 
                    ORDER BY created_at DESC
                """, (state.value,))
            else:
                cursor = conn.execute("""
                    SELECT * FROM jobs 
                    ORDER BY created_at DESC
                """)
            
            return [self._row_to_job(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_job_counts(self) -> Dict[str, int]:
        """Get count of jobs by state"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("""
                SELECT state, COUNT(*) as count 
                FROM jobs 
                GROUP BY state
            """)
            
            counts = {state.value: 0 for state in JobState}
            for row in cursor.fetchall():
                counts[row[0]] = row[1]
            
            return counts
        finally:
            conn.close()
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary statistics from job metrics"""
        conn = sqlite3.connect(self.db_path)
        try:
            # Get total counts by event type
            cursor = conn.execute("""
                SELECT event_type, COUNT(*) as count 
                FROM job_metrics 
                GROUP BY event_type
            """)
            event_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get average execution time (start to completed)
            cursor = conn.execute("""
                SELECT AVG(c.timestamp - s.timestamp) as avg_duration
                FROM job_metrics s
                JOIN job_metrics c ON s.job_id = c.job_id
                WHERE s.event_type = 'started' AND c.event_type = 'completed'
            """)
            avg_duration = cursor.fetchone()[0]
            
            # Get recent metrics (last 100 events)
            cursor = conn.execute("""
                SELECT job_id, event_type, timestamp, duration_ms, error_message
                FROM job_metrics 
                ORDER BY timestamp DESC 
                LIMIT 100
            """)
            recent_events = [
                {
                    'job_id': row[0],
                    'event_type': row[1],
                    'timestamp': row[2],
                    'duration_ms': row[3],
                    'error_message': row[4]
                }
                for row in cursor.fetchall()
            ]
            
            return {
                'event_counts': event_counts,
                'avg_duration_seconds': avg_duration if avg_duration else 0,
                'recent_events': recent_events
            }
        finally:
            conn.close()
