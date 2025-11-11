#!/usr/bin/env python3
"""
Validation script to verify all assignment requirements
Run: python tests/validate_requirements.py
"""

import subprocess
import time
import sys
import json
from pathlib import Path


def run_command(cmd, shell=True):
    """Run command and return result"""
    try:
        result = subprocess.run(
            cmd,
            shell=shell,
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"


def test_cli_commands():
    """Verify all CLI commands work"""
    print("Testing CLI commands...")
    
    tests = [
        ("queuectl --help", "help text"),
        ("queuectl status", "status display"),
        ("queuectl list", "list jobs"),
        ("queuectl config get", "get config"),
        ("queuectl dlq list", "DLQ list"),
    ]
    
    for cmd, desc in tests:
        code, out, err = run_command(cmd)
        if code == 0:
            print(f"  [PASS] {desc}")
        else:
            print(f"  [FAIL] {desc} - Exit code: {code}")
            print(f"    Error: {err}")
            return False
    
    return True


def test_job_enqueue():
    """Test job enqueue"""
    print("\nTesting job enqueue...")
    
    # Use subprocess.run with proper args instead of shell string
    job_data = {"id": "validate1", "command": "echo validation_test"}
    try:
        result = subprocess.run(
            ["queuectl", "enqueue", json.dumps(job_data)],
            capture_output=True,
            text=True,
            timeout=10
        )
        code, out, err = result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        code, out, err = -1, "", "Command timed out"
    except Exception as e:
        code, out, err = -1, "", str(e)
    if code == 0 and "Enqueued" in out:
        print("  [PASS] Job enqueue successful")
        return True
    else:
        print(f"  [FAIL] Job enqueue failed - Exit code: {code}")
        print(f"    Output: {out}")
        print(f"    Error: {err}")
        return False


def test_job_listing():
    """Test job listing"""
    print("\nTesting job listing...")
    
    code, out, err = run_command("queuectl list --state pending")
    if code == 0:
        print("  [PASS] Job listing successful")
        return True
    else:
        print(f"  [FAIL] Job listing failed - Exit code: {code}")
        return False


def test_configuration():
    """Test configuration management"""
    print("\nTesting configuration...")
    
    # Set config
    code, out, err = run_command("queuectl config set max-retries 5")
    if code != 0:
        print("  [FAIL] Config set failed")
        return False
    
    # Get config
    code, out, err = run_command("queuectl config get max-retries")
    if code == 0 and "5" in out:
        print("  [PASS] Configuration management working")
        # Reset to default
        run_command("queuectl config set max-retries 3")
        return True
    else:
        print("  [FAIL] Config get failed")
        return False


def test_status_command():
    """Test status command"""
    print("\nTesting status command...")
    
    code, out, err = run_command("queuectl status")
    if code == 0 and "Pending" in out and "Completed" in out:
        print("  [PASS] Status command working")
        return True
    else:
        print("  [FAIL] Status command failed")
        return False


def test_dlq_commands():
    """Test DLQ commands"""
    print("\nTesting DLQ commands...")
    
    # List DLQ
    code, out, err = run_command("queuectl dlq list")
    if code != 0:
        print("  [FAIL] DLQ list failed")
        return False
    
    print("  [PASS] DLQ commands working")
    return True


def main():
    """Run all validation tests"""
    print("=" * 50)
    print("QueueCTL Validation Script")
    print("=" * 50)
    
    # Check if queuectl is installed
    code, _, _ = run_command("queuectl --version", shell=True)
    if code != 0:
        # Try with python -m
        code, _, _ = run_command("python -m queuectl.cli --help", shell=True)
        if code != 0:
            print("\n[ERROR] queuectl command not found. Please install first:")
            print("  pip install -e .")
            sys.exit(1)
    
    tests = [
        test_cli_commands,
        test_job_enqueue,
        test_job_listing,
        test_configuration,
        test_status_command,
        test_dlq_commands,
    ]
    
    results = []
    for test in tests:
        try:
            results.append(test())
        except Exception as e:
            print(f"\n[ERROR] Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("Validation Summary")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Passed: {passed}/{total}")
    
    if all(results):
        print("\n[PASS] All requirements validated successfully!")
        sys.exit(0)
    else:
        print("\n[FAIL] Some tests failed. Please review above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
