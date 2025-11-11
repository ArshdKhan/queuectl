"""Unit tests for worker components"""

import pytest
from queuectl.worker.executor import JobExecutor


def test_executor_success():
    """Test successful command execution"""
    executor = JobExecutor()
    success, error = executor.execute("echo 'hello'")
    
    assert success is True
    assert error == ""


def test_executor_failure():
    """Test failed command execution"""
    executor = JobExecutor()
    success, error = executor.execute("exit 1")
    
    assert success is False
    assert "Exit code 1" in error


def test_executor_command_not_found():
    """Test handling of nonexistent command"""
    executor = JobExecutor()
    success, error = executor.execute("nonexistent_command_xyz_123")
    
    assert success is False
    assert error != ""


def test_executor_timeout():
    """Test command timeout handling"""
    executor = JobExecutor(timeout=1)
    success, error = executor.execute("sleep 5")
    
    assert success is False
    assert "timeout" in error.lower()


def test_executor_captures_stderr():
    """Test that stderr is captured in error message"""
    executor = JobExecutor()
    # PowerShell command to write to stderr
    success, error = executor.execute('powershell -Command "Write-Error \'test error\'"')
    
    assert success is False
    assert error != ""
