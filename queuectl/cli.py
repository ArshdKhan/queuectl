"""CLI interface using Click"""

import json
import os
import signal
import time
from pathlib import Path
from typing import Optional, Any
from datetime import datetime, timedelta

import click

from queuectl.config import Config
from queuectl.models import JobState
from queuectl.queue import QueueManager
from queuectl.worker import WorkerPool
from queuectl.exceptions import (
    JobNotFoundException, 
    InvalidJobStateException,
    QueueCTLException
)


@click.group()
def cli() -> None:
    """QueueCTL - Background Job Queue System"""
    pass


@cli.command()
@click.argument('job_json')
def enqueue(job_json: str) -> None:
    """
    Enqueue a new job with optional priority and scheduling
    
    Example: queuectl enqueue '{"id":"job1","command":"sleep 2"}'
    Example: queuectl enqueue '{"id":"job2","command":"echo hi","priority":8}'
    Example: queuectl enqueue '{"id":"job3","command":"backup.sh","run_at":"2025-11-11T10:00:00"}'
    """
    try:
        from datetime import datetime
        
        data = json.loads(job_json)
        
        if 'id' not in data or 'command' not in data:
            click.echo("Error: job must contain 'id' and 'command' fields", err=True)
            return
        
        config = Config.load()
        manager = QueueManager(config)
        
        # Parse optional run_at datetime (accepts local time, converts to UTC)
        run_at = None
        if 'run_at' in data:
            local_dt = datetime.fromisoformat(data['run_at'])
            # Convert local time to UTC for storage
            utc_offset_seconds = -time.timezone if time.daylight == 0 else -time.altzone
            utc_offset = timedelta(seconds=utc_offset_seconds)
            run_at = local_dt - utc_offset
        
        job = manager.enqueue(
            data['id'],
            data['command'],
            data.get('max_retries'),
            data.get('priority'),
            run_at
        )
        
        if run_at:
            click.echo(f"Enqueued job {job.id} (priority={job.priority}, scheduled for {run_at.isoformat()})")
        elif job.priority != 5:
            click.echo(f"Enqueued job {job.id} (priority={job.priority})")
        else:
            click.echo(f"Enqueued job {job.id}")
    
    except json.JSONDecodeError:
        click.echo("Error: invalid JSON format", err=True)
    except ValueError as e:
        click.echo(f"Error: invalid datetime format - {e}", err=True)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@cli.command()
@click.option('--state', type=click.Choice(['pending', 'processing', 'completed', 'failed', 'dead']), help='Filter by state')
def list(state: Optional[str]) -> None:
    """
    List jobs
    
    Example: queuectl list --state pending
    """
    try:
        config = Config.load()
        manager = QueueManager(config)
        
        state_enum = JobState(state) if state else None
        jobs = manager.list_jobs(state_enum)
        
        if not jobs:
            click.echo("No jobs found")
            return
        
        for job in jobs:
            click.echo(f"[{job.id}] {job.command}")
            click.echo(f"  State: {job.state.value} | Attempts: {job.attempts}/{job.max_retries}")
            if job.error_message:
                click.echo(f"  Error: {job.error_message}")
            click.echo()
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@cli.command()
def status() -> None:
    """
    Show queue status
    
    Example: queuectl status
    """
    try:
        config = Config.load()
        manager = QueueManager(config)
        stats = manager.get_stats()
        
        click.echo("=== Queue Status ===")
        click.echo(f"Pending:    {stats['pending']}")
        click.echo(f"Processing: {stats['processing']}")
        click.echo(f"Completed:  {stats['completed']}")
        click.echo(f"Failed:     {stats['failed']}")
        click.echo(f"Dead (DLQ): {stats['dead']}")
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@cli.command()
@click.option('--recent', default=10, help='Number of recent events to show')
def metrics(recent: int) -> None:
    """
    Show job metrics and statistics
    
    Example: queuectl metrics
    Example: queuectl metrics --recent 20
    """
    try:
        config = Config.load()
        manager = QueueManager(config)
        metrics_data = manager.get_metrics()
        
        click.echo("=== Job Metrics ===")
        click.echo("\nEvent Counts:")
        for event_type, count in metrics_data.get('event_counts', {}).items():
            click.echo(f"  {event_type.capitalize():12} {count}")
        
        avg_duration = metrics_data.get('avg_duration_seconds', 0)
        click.echo(f"\nAverage Execution Time: {avg_duration:.2f}s")
        
        recent_events = metrics_data.get('recent_events', [])[:recent]
        if recent_events:
            click.echo(f"\nRecent Events (last {len(recent_events)}):")
            for event in recent_events:
                timestamp = event['timestamp'].split('T')[1][:8]  # Show time only
                job_id = event['job_id'][:20]  # Truncate long IDs
                event_type = event['event_type']
                click.echo(f"  [{timestamp}] {job_id:20} - {event_type}")
                if event.get('error_message'):
                    error = event['error_message'][:60]
                    click.echo(f"            Error: {error}")
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@cli.group()
def worker() -> None:
    """Worker management commands"""
    pass


@worker.command()
@click.option('--count', default=1, help='Number of workers to start')
def start(count: int) -> None:
    """
    Start worker processes
    
    Example: queuectl worker start --count 3
    """
    try:
        config = Config.load()
        manager = QueueManager(config)
        pool = WorkerPool(manager, config, count)
        
        click.echo(f"Starting {count} worker(s)... (Press Ctrl+C to stop)")
        pool.start()
    
    except KeyboardInterrupt:
        click.echo("\nStopping workers...")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@worker.command()
def stop() -> None:
    """
    Stop running workers gracefully
    
    Example: queuectl worker stop
    """
    try:
        pid_file = Path.home() / ".queuectl" / "workers.pid"
        
        if not pid_file.exists():
            click.echo("No workers running")
            return
        
        with open(pid_file) as f:
            pids = [int(line.strip()) for line in f if line.strip()]
        
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
                click.echo(f"Sent SIGTERM to worker {pid}")
            except ProcessLookupError:
                click.echo(f"Worker {pid} not found")
            except PermissionError:
                click.echo(f"Permission denied for worker {pid}")
        
        pid_file.unlink()
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@worker.command()
def health() -> None:
    """
    Show worker health status
    
    Example: queuectl worker health
    """
    try:
        config = Config.load()
        manager = QueueManager(config=config)
        stats = manager.get_stats()
        
        click.echo("=== Worker Status ===")
        click.echo(f"Jobs in queue: {stats['pending']} pending, {stats['processing']} processing")
        click.echo(f"\nWorker logs available at:")
        click.echo(f"  {Path.home() / '.queuectl' / 'logs'}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@cli.group()
def dlq() -> None:
    """Dead Letter Queue commands"""
    pass


@dlq.command('list')
def dlq_list() -> None:
    """
    List DLQ jobs
    
    Example: queuectl dlq list
    """
    try:
        config = Config.load()
        manager = QueueManager(config)
        
        jobs = manager.list_jobs(JobState.DEAD)
        
        if not jobs:
            click.echo("DLQ is empty")
            return
        
        click.echo(f"=== Dead Letter Queue ({len(jobs)} jobs) ===\n")
        
        for job in jobs:
            click.echo(f"[{job.id}] {job.command}")
            click.echo(f"  Attempts: {job.attempts}/{job.max_retries}")
            click.echo(f"  Error: {job.error_message}")
            click.echo()
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@dlq.command('retry')
@click.argument('job_id')
def dlq_retry(job_id: str) -> None:
    """
    Retry a DLQ job
    
    Example: queuectl dlq retry job1
    """
    try:
        config = Config.load()
        manager = QueueManager(config)
        
        manager.retry_dlq_job(job_id)
        click.echo(f"Job {job_id} reset to pending")
    
    except JobNotFoundException as e:
        click.echo(f"Error: {e}", err=True)
    except InvalidJobStateException as e:
        click.echo(f"Error: {e}", err=True)
    except QueueCTLException as e:
        click.echo(f"Error: {e}", err=True)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)


@cli.group()
def config() -> None:
    """Configuration management"""
    pass


@config.command('get')
@click.argument('key', required=False)
def config_get(key: Optional[str]) -> None:
    """
    Get configuration value(s)
    
    Example: queuectl config get max-retries
    """
    try:
        cfg = Config.load()
        
        if key:
            # Convert hyphen to underscore
            key = key.replace('-', '_')
            value = cfg.get(key)
            click.echo(f"{key.replace('_', '-')} = {value}")
        else:
            # Show all config
            from dataclasses import asdict
            data = asdict(cfg)
            for k, v in data.items():
                click.echo(f"{k.replace('_', '-')}: {v}")
    
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)

@config.command('set')
@click.argument('key')
@click.argument('value')
def config_set(key: str, value: str) -> None:
    """
    Set configuration value
    
    Example: queuectl config set max-retries 5
    """
    try:
        cfg = Config.load()
        
        # Convert hyphen to underscore
        key = key.replace('-', '_')
        
        # Type conversion
        value_typed: Any
        if key in ['max_retries', 'job_timeout']:
            value_typed = int(value)
        elif key in ['backoff_base', 'worker_poll_interval']:
            value_typed = float(value)
        else:
            value_typed = value
        
        cfg.set(key, value_typed)
        click.echo(f"Set {key.replace('_', '-')} = {value_typed}")
    
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)

@cli.command()
@click.option('--host', default='127.0.0.1', help='Host to bind to')
@click.option('--port', default=5000, help='Port to bind to')
@click.option('--debug', is_flag=True, help='Enable debug mode')
def web(host: str, port: int, debug: bool) -> None:
    """Start web dashboard for monitoring"""
    try:
        from queuectl.web.app import run_server
        
        click.echo(f"Starting QueueCTL web dashboard...")
        click.echo(f"Open http://{host}:{port} in your browser")
        click.echo("Press Ctrl+C to stop\n")
        
        run_server(host=host, port=port, debug=debug)
        
    except ImportError:
        click.echo("Error: Flask not installed. Install with: pip install flask", err=True)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


if __name__ == '__main__':
    cli()
