#!/usr/bin/env python3
"""
CoreDB Test Runner - Run all integration tests

Usage:
    ./run_all_tests.py
    python run_all_tests.py

This script runs all test_*.py files in the tests directory.
"""

import os
import sys
import glob
import subprocess
import signal
import time
import re
from typing import Tuple


def signal_handler(sig, frame):
    """Handle interrupt signals."""
    print("\n\nInterrupted!")
    sys.exit(1)


def run_test(test_file: str, tests_dir: str) -> Tuple[bool, list]:
    """Run a single test file and return (passed, failed_tests)."""
    print("\n" + "=" * 60)
    print(f"Running: {test_file}")
    print("=" * 60)
    
    test_path = os.path.join(tests_dir, test_file)
    
    result = subprocess.run(
        [sys.executable, test_path],
        cwd=tests_dir,
        capture_output=True,
        text=True
    )
    
    if result.stdout:
        print(result.stdout, end='')
    if result.stderr:
        print(result.stderr, end='', file=sys.stderr)
    
    failed_tests = []
    if result.returncode != 0:
        lines = result.stdout.split('\n') if result.stdout else []
        current_test = None
        for line in lines:
            test_match = re.match(r'^Test:\s*(.+)$', line.strip())
            if test_match:
                current_test = test_match.group(1).strip()
            if current_test and 'FAILED' in line and 'PASSED' not in line:
                if current_test not in failed_tests:
                    failed_tests.append(current_test)
    
    return result.returncode == 0, failed_tests


def main():
    """Main entry point."""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    test_files = sorted(glob.glob(os.path.join(tests_dir, "test_*.py")))
    
    if not test_files:
        print("No test files found!")
        return 1
    
    print("CoreDB Integration Test Suite")
    print("=" * 60)
    print(f"Found {len(test_files)} test file(s):")
    for test_file in test_files:
        print(f"  - {os.path.basename(test_file)}")
    
    results = {}
    for i, test_path in enumerate(test_files):
        test_file = os.path.basename(test_path)
        
        if i > 0:
            print(f"\nWaiting 5 seconds before next test...")
            time.sleep(5)
        
        results[test_file] = run_test(test_file, tests_dir)
    
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed_count = sum(1 for passed, _ in results.values() if passed)
    failed_count = sum(1 for passed, _ in results.values() if not passed)
    
    for test_file, (passed, _) in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {status} - {test_file}")
    
    print("-" * 60)
    print(f"Results: {passed_count} passed, {failed_count} failed")
    print("=" * 60)
    
    if failed_count == 0:
        print("\n🎉 All tests passed!")
        return 0
    
    print(f"\n⚠️  {failed_count} test file(s) failed!")
    
    all_failed_tests = []
    for test_file, (passed, failed_tests) in results.items():
        if not passed and failed_tests:
            all_failed_tests.append((test_file, failed_tests))
    
    print("\nFailed test files:")
    for test_file, _ in all_failed_tests:
        print(f"  - {test_file}")
    
    print("\nFailed test cases:")
    for test_file, failed_tests in all_failed_tests:
        print(f"\n  [{test_file}]")
        for test_name in failed_tests:
            print(f"    - {test_name}")
    
    return 1


if __name__ == "__main__":
    sys.exit(main())
