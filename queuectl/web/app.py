"""Flask web application for monitoring QueueCTL"""
from flask import Flask, render_template, jsonify, request
from queuectl.config import Config
from queuectl.queue import QueueManager
from queuectl.models import JobState
from datetime import datetime
from typing import Any, Optional


def create_app(config_path: Optional[str] = None) -> Flask:
    """Create and configure Flask application"""
    app = Flask(__name__)
    app.config['JSON_SORT_KEYS'] = False
    
    config = Config.load() if not config_path else Config.load()
    manager = QueueManager(config)
    
    @app.route('/')
    def index() -> str:
        """Main dashboard page"""
        return render_template('dashboard.html')
    
    @app.route('/api/stats')
    def api_stats() -> Any:
        """Get queue statistics"""
        stats = manager.get_stats()
        return jsonify(stats)
    
    @app.route('/api/jobs')
    def api_jobs() -> Any:
        """Get list of jobs with optional state filter"""
        state_str = request.args.get('state')
        state = JobState(state_str) if state_str else None
        jobs = manager.list_jobs(state)
        
        # Convert Job objects to dicts
        job_dicts = []
        for job in jobs:
            job_dict = {
                'id': job.id,
                'command': job.command,
                'state': job.state.value,
                'priority': job.priority,
                'attempts': job.attempts,
                'max_retries': job.max_retries,
                'created_at': job.created_at.isoformat() if job.created_at else None,
                'updated_at': job.updated_at.isoformat() if job.updated_at else None,
                'run_at': job.run_at.isoformat() if job.run_at else None,
                'error_message': job.error_message,
            }
            job_dicts.append(job_dict)
        
        return jsonify(job_dicts)
    
    @app.route('/api/metrics')
    def api_metrics() -> Any:
        """Get job metrics and statistics"""
        metrics = manager.get_metrics()
        return jsonify(metrics)
    
    @app.route('/api/enqueue', methods=['POST'])
    def api_enqueue() -> Any:
        """Enqueue a new job"""
        data: Any = request.json
        
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        job_id = data.get('id')
        command = data.get('command')
        priority = data.get('priority', 5)
        max_retries = data.get('max_retries', 3)
        run_at_str = data.get('run_at')
        
        if not job_id or not command:
            return jsonify({'error': 'id and command are required'}), 400
        
        run_at = None
        if run_at_str:
            try:
                run_at = datetime.fromisoformat(run_at_str)
            except ValueError:
                return jsonify({'error': 'Invalid run_at format. Use ISO8601'}), 400
        
        try:
            job = manager.enqueue(job_id, command, max_retries, priority, run_at)
            return jsonify({
                'success': True,
                'job_id': job.id,
                'message': f'Job {job.id} enqueued with priority {job.priority}'
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/retry/<job_id>', methods=['POST'])
    def api_retry(job_id: str) -> Any:
        """Retry a DLQ job"""
        try:
            manager.retry_dlq_job(job_id)
            return jsonify({'success': True, 'message': f'Job {job_id} moved back to pending'})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    return app


def run_server(host: str = '127.0.0.1', port: int = 5000, debug: bool = False) -> None:
    """Run the Flask development server"""
    app = create_app()
    app.run(host=host, port=port, debug=debug)
