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


def signal_handler(sig, frame):
    """Handle interrupt signals."""
    print("\n\nInterrupted!")
    sys.exit(1)


def run_test(test_file: str, tests_dir: str) -> bool:
    """Run a single test file.
    
    Args:
        test_file: Name of the test file (e.g., "test_cluster_string.py")
        tests_dir: Path to the tests directory
        
    Returns:
        True if test passed, False otherwise
    """
    print("\n" + "=" * 60)
    print(f"Running: {test_file}")
    print("=" * 60)
    
    test_path = os.path.join(tests_dir, test_file)
    
    result = subprocess.run(
        [sys.executable, test_path],
        cwd=tests_dir,
        capture_output=False,  # Allow output to be displayed in real-time
        text=True
    )
    
    return result.returncode == 0


def main():
    """Main entry point."""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Get the directory containing this script
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Find all test_*.py files
    test_files = sorted(glob.glob(os.path.join(tests_dir, "test_*.py")))
    
    if not test_files:
        print("No test files found (test_*.py)")
        sys.exit(0)
    
    print("=" * 60)
    print("CoreDB Integration Test Suite")
    print("=" * 60)
    print(f"Found {len(test_files)} test file(s):")
    for test_file in test_files:
        print(f"  - {os.path.basename(test_file)}")
    
    # Run each test
    results = {}
    for i, test_path in enumerate(test_files):
        test_file = os.path.basename(test_path)
        
        # Wait 5 seconds between test files (except before the first one)
        if i > 0:
            print(f"\nWaiting 5 seconds before next test...")
            time.sleep(5)
        
        results[test_file] = run_test(test_file, tests_dir)
    
    # Print summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(1 for result in results.values() if result)
    failed = sum(1 for result in results.values() if not result)
    
    for test_file, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"  {status} - {test_file}")
    
    print("-" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    if failed == 0:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {failed} test(s) failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
