#!/usr/bin/env python3
"""
CoreDB Cluster Key Command Integration Tests

This test suite covers key-level commands (KEYS) that operate across all data types.

Usage:
    pip install -r requirements.txt
    python test_cluster_key.py
"""

import time
import sys
import os

import redis

from cluster_manager import ClusterManager
from base_test import TestClusterBase


class TestClusterKey(TestClusterBase):
    """Key command tests."""

    def _cleanup_test_keys(self, prefix: str = "keys_test"):
        """Delete all keys with the given prefix."""
        node = self._get_random_node()
        keys = node.keys(f"{prefix}:*")
        if keys:
            node.delete(*keys)

    def test_keys_star_returns_all_keys(self) -> bool:
        """Test KEYS * returns all user-facing keys."""
        print("\nTest: KEYS * returns all keys")

        prefix = "keys_test_star"
        node = self._get_random_node()
        self._cleanup_test_keys(prefix)

        # Set up some string keys
        node.set(f"{prefix}:a", "1")
        node.set(f"{prefix}:b", "2")
        node.set(f"{prefix}:c", "3")

        # Query with KEYS *
        result = node.keys(f"{prefix}:*")
        result_set = set(result)

        expected = {f"{prefix}:a", f"{prefix}:b", f"{prefix}:c"}
        if result_set != expected:
            print(f"  FAILED: expected {expected}, got {result_set}")
            return False

        # Cleanup
        self._cleanup_test_keys(prefix)

        print("\033[32m  PASSED\033[0m")
        return True

    def test_keys_with_wildcard_prefix(self) -> bool:
        """Test KEYS with prefix:* pattern."""
        print("\nTest: KEYS with prefix:* pattern")

        prefix = "keys_test_wild"
        node = self._get_random_node()
        self._cleanup_test_keys(prefix)

        node.set(f"{prefix}:hello", "1")
        node.set(f"{prefix}:hallo", "2")
        node.set(f"{prefix}:hxllo", "3")
        node.set("other_key", "4")

        result = node.keys(f"{prefix}:h*llo")
        result_set = set(result)

        expected = {f"{prefix}:hello", f"{prefix}:hallo", f"{prefix}:hxllo"}
        if result_set != expected:
            print(f"  FAILED: expected {expected}, got {result_set}")
            return False

        # Cleanup
        node.delete("other_key")
        self._cleanup_test_keys(prefix)

        print("\033[32m  PASSED\033[0m")
        return True

    def test_keys_with_question_mark(self) -> bool:
        """Test KEYS with h?llo pattern (single character wildcard)."""
        print("\nTest: KEYS with ? wildcard")

        prefix = "keys_test_qmark"
        node = self._get_random_node()
        self._cleanup_test_keys(prefix)

        node.set(f"{prefix}:hello", "1")
        node.set(f"{prefix}:hallo", "2")
        node.set(f"{prefix}:hllo", "3")
        node.set(f"{prefix}:heeello", "4")

        result = node.keys(f"{prefix}:h?llo")
        result_set = set(result)

        expected = {f"{prefix}:hello", f"{prefix}:hallo"}
        if result_set != expected:
            print(f"  FAILED: expected {expected}, got {result_set}")
            return False

        # Cleanup
        self._cleanup_test_keys(prefix)

        print("\033[32m  PASSED\033[0m")
        return True

    def test_keys_with_charset(self) -> bool:
        """Test KEYS with [ae] pattern."""
        print("\nTest: KEYS with [ae] charset")

        prefix = "keys_test_charset"
        node = self._get_random_node()
        self._cleanup_test_keys(prefix)

        node.set(f"{prefix}:hello", "1")
        node.set(f"{prefix}:hallo", "2")
        node.set(f"{prefix}:hillo", "3")

        result = node.keys(f"{prefix}:h[ae]llo")
        result_set = set(result)

        expected = {f"{prefix}:hello", f"{prefix}:hallo"}
        if result_set != expected:
            print(f"  FAILED: expected {expected}, got {result_set}")
            return False

        # Cleanup
        self._cleanup_test_keys(prefix)

        print("\033[32m  PASSED\033[0m")
        return True

    def test_keys_no_match(self) -> bool:
        """Test KEYS returns empty list when no keys match."""
        print("\nTest: KEYS returns empty list for no match")

        prefix = "keys_test_nomatch"
        node = self._get_random_node()
        self._cleanup_test_keys(prefix)

        result = node.keys("nonexistent_pattern_xyz_12345")
        if result != []:
            print(f"  FAILED: expected [], got {result}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_keys_across_data_types(self) -> bool:
        """Test KEYS returns keys from different data types."""
        print("\nTest: KEYS across data types")

        prefix = "keys_test_types"
        node = self._get_random_node()
        self._cleanup_test_keys(prefix)

        # Create keys of different types
        node.set(f"{prefix}:string", "value")
        node.hset(f"{prefix}:hash", "field", "value")
        node.lpush(f"{prefix}:list", "element")
        node.sadd(f"{prefix}:set", "member")
        node.zadd(f"{prefix}:zset", {"member": 1.0})

        result = node.keys(f"{prefix}:*")
        result_set = set(result)

        expected = {
            f"{prefix}:string",
            f"{prefix}:hash",
            f"{prefix}:list",
            f"{prefix}:set",
            f"{prefix}:zset",
        }
        if result_set != expected:
            print(f"  FAILED: expected {expected}, got {result_set}")
            return False

        # Cleanup
        self._cleanup_test_keys(prefix)

        print("\033[32m  PASSED\033[0m")
        return True

    def test_keys_does_not_return_expired_keys(self) -> bool:
        """Test KEYS does not return expired keys."""
        print("\nTest: KEYS does not return expired keys")

        prefix = "keys_test_expire"
        node = self._get_random_node()
        self._cleanup_test_keys(prefix)

        node.set(f"{prefix}:permanent", "value")
        node.set(f"{prefix}:expiring", "value", px=200)

        # Wait for the key to expire
        time.sleep(0.3)

        result = node.keys(f"{prefix}:*")
        result_set = set(result)

        if f"{prefix}:expiring" in result_set:
            print(f"  FAILED: expired key should not appear, got {result_set}")
            return False

        if f"{prefix}:permanent" not in result_set:
            print(f"  FAILED: permanent key missing, got {result_set}")
            return False

        # Cleanup
        self._cleanup_test_keys(prefix)

        print("\033[32m  PASSED\033[0m")
        return True

    def test_keys_exact_match(self) -> bool:
        """Test KEYS with exact key name (no wildcards)."""
        print("\nTest: KEYS with exact key name")

        prefix = "keys_test_exact"
        node = self._get_random_node()
        self._cleanup_test_keys(prefix)

        node.set(f"{prefix}:mykey", "value")
        node.set(f"{prefix}:mykey2", "value2")

        result = node.keys(f"{prefix}:mykey")
        # Redis KEYS with exact match returns the exact key
        if f"{prefix}:mykey" not in result:
            print(f"  FAILED: expected exact key, got {result}")
            return False

        if f"{prefix}:mykey2" in result:
            print(f"  FAILED: should not match mykey2, got {result}")
            return False

        # Cleanup
        self._cleanup_test_keys(prefix)

        print("\033[32m  PASSED\033[0m")
        return True

    def run_all_tests(self) -> bool:
        """Run all key command tests."""
        print("\n" + "=" * 50)
        print("Running Key Command Tests")
        print("=" * 50)

        if not self.setup():
            return False

        tests = [
            self.test_keys_star_returns_all_keys,
            self.test_keys_with_wildcard_prefix,
            self.test_keys_with_question_mark,
            self.test_keys_with_charset,
            self.test_keys_no_match,
            self.test_keys_across_data_types,
            self.test_keys_does_not_return_expired_keys,
            self.test_keys_exact_match,
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
        print(f"Key Command Tests: {passed} passed, {failed} failed")
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
        tester = TestClusterKey(cluster)
        success = tester.run_all_tests()

        if success:
            print("\n✅ All key command tests passed!")
        else:
            print("\n❌ Some key command tests failed!")

    finally:
        # Always stop cluster
        cluster.stop()
        cluster.clean()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
