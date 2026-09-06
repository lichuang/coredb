#!/usr/bin/env python3
"""
CoreDB Cluster Connection Command Integration Tests

This test suite covers connection-level commands (ECHO).

Usage:
    pip install -r requirements.txt
    python test_cluster_connection.py
"""

import sys
import os

import redis

from cluster_manager import ClusterManager
from base_test import TestClusterBase


class TestClusterConnection(TestClusterBase):
    """Connection command tests."""

    def test_echo_basic(self) -> bool:
        """Test ECHO returns the given message."""
        print("\nTest: ECHO basic message")

        node = self._get_random_node()
        result = node.echo("hello world")

        if result != "hello world":
            print(f"  FAILED: expected 'hello world', got {result}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_echo_empty_string(self) -> bool:
        """Test ECHO with an empty message."""
        print("\nTest: ECHO empty string")

        node = self._get_random_node()
        result = node.echo("")

        if result != "":
            print(f"  FAILED: expected '', got {result!r}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_echo_special_characters(self) -> bool:
        """Test ECHO with special characters."""
        print("\nTest: ECHO special characters")

        node = self._get_random_node()
        message = "line1\nline2\twith\ttabs\x01\x02"
        result = node.echo(message)

        if result != message:
            print(f"  FAILED: expected {message!r}, got {result!r}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_echo_number_string(self) -> bool:
        """Test ECHO with a numeric string."""
        print("\nTest: ECHO numeric string")

        node = self._get_random_node()
        result = node.echo("12345")

        if result != "12345":
            print(f"  FAILED: expected '12345', got {result}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_echo_consistent_across_nodes(self) -> bool:
        """Test ECHO returns the same result on all nodes."""
        print("\nTest: ECHO consistent across nodes")

        message = "consistent-message"
        for node in self.get_alive_nodes():
            result = node.conn.echo(message)
            if result != message:
                print(f"  FAILED on port {node.port}: expected {message!r}, got {result!r}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def run_all_tests(self) -> bool:
        """Run all connection command tests."""
        print("\n" + "=" * 50)
        print("Running Connection Command Tests")
        print("=" * 50)

        if not self.setup():
            return False

        tests = [
            self.test_echo_basic,
            self.test_echo_empty_string,
            self.test_echo_special_characters,
            self.test_echo_number_string,
            self.test_echo_consistent_across_nodes,
        ]

        passed = 0
        failed = 0
        for test in tests:
            try:
                if test():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"\033[31m  FAILED with exception: {e}\033[0m")
                failed += 1

        print(f"\n{'='*40}")
        print(f"Connection Command Tests: {passed} passed, {failed} failed")
        print(f"{'='*40}")

        return failed == 0


def main():
    """Main entry point."""
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    cluster = ClusterManager(tests_dir=tests_dir)

    # Clean up any existing cluster
    cluster.clean()

    # Build project
    if not cluster.build():
        print("Build failed, exiting")
        sys.exit(1)

    # Start cluster
    if not cluster.start():
        print("Failed to start cluster, exiting")
        cluster.stop()
        sys.exit(1)

    try:
        # Run tests
        tester = TestClusterConnection(cluster)
        success = tester.run_all_tests()

        if success:
            print("\n✅ All connection command tests passed!")
        else:
            print("\n❌ Some connection command tests failed!")

    finally:
        # Always stop cluster
        cluster.stop()
        cluster.clean()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
