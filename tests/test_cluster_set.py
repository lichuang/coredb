#!/usr/bin/env python3
"""
CoreDB Cluster Set Integration Tests

This test suite verifies Set commands (SADD, etc.) against a running 3-node cluster.

Usage:
    pip install -r requirements.txt
    python test_cluster_set.py
"""

import sys
import os
import signal

import redis

from cluster_manager import ClusterManager
from base_test import TestClusterBase


class TestClusterSet(TestClusterBase):
    """Set command tests."""

    def test_sadd_single_member(self) -> bool:
        """Test SADD with a single member."""
        print("\nTest: SADD single member")

        key = "sadd_single"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  SADD '{key}' 'member1'...")
        try:
            result = write_node.sadd(key, "member1")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_multiple_members(self) -> bool:
        """Test SADD with multiple members."""
        print("\nTest: SADD multiple members")

        key = "sadd_multi"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  SADD '{key}' a b c...")
        try:
            result = write_node.sadd(key, "a", "b", "c")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD returned 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_duplicate_members(self) -> bool:
        """Test SADD with duplicate members returns 0 for existing."""
        print("\nTest: SADD duplicate members")

        key = "sadd_dup"
        write_node = self._get_random_node()

        write_node.delete(key)

        write_node.sadd(key, "a", "b")

        print(f"  SADD '{key}' a b c (a, b already exist)...")
        try:
            result = write_node.sadd(key, "a", "b", "c")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1 (only c is new), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD returned 1 (only 'c' added): OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_creates_key(self) -> bool:
        """Test that SADD creates the key."""
        print("\nTest: SADD creates key")

        key = "sadd_new_key"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  SADD '{key}' 'value' on non-existent key...")
        try:
            result = write_node.sadd(key, "value")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print(f"  SADD again to verify key exists...")
        try:
            result = write_node.sadd(key, "value2")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  Key created and verified: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_wrong_type(self) -> bool:
        """Test SADD on a key holding wrong type."""
        print("\nTest: SADD wrong type error")

        key = "sadd_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  SADD on string key '{key}'...")
        try:
            write_node.sadd(key, "member")
            print(f"\033[31m  FAILED: Expected WRONGTYPE error")
            return False
        except redis.ResponseError as e:
            error_msg = str(e)
            if "WRONGTYPE" not in error_msg:
                print(f"\033[31m  FAILED: Expected WRONGTYPE error, got: {e}")
                return False
            print(f"  Got expected WRONGTYPE error: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_empty_member(self) -> bool:
        """Test SADD with empty string member."""
        print("\nTest: SADD empty member")

        key = "sadd_empty"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  SADD '{key}' ''...")
        try:
            result = write_node.sadd(key, "")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD empty member returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_binary_member(self) -> bool:
        """Test SADD with binary data member."""
        print("\nTest: SADD binary member")

        key = "sadd_binary"
        write_node = self._get_random_node()

        write_node.delete(key)

        binary_data = bytes(range(256))
        print(f"  SADD '{key}' with 256-byte binary data...")
        try:
            result = write_node.sadd(key, binary_data)
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD binary member returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_replication(self) -> bool:
        """Test that SADD data replicates to all nodes."""
        print("\nTest: SADD replication")

        key = "sadd_repl"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  SADD '{key}' a b c on random node...")
        try:
            write_node.sadd(key, "a", "b", "c")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD from all nodes to verify replication (adding duplicate should return 0)...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.sadd(key, "a", "b", "c")
                if result != 0:
                    print(f"    Node {i}: FAILED (expected 0, got {result})")
                    return False
                print(f"    Node {i}: OK (sadd returned 0, all members exist)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_all_duplicates(self) -> bool:
        """Test SADD with all members already existing returns 0."""
        print("\nTest: SADD all duplicates returns 0")

        key = "sadd_all_dup"
        write_node = self._get_random_node()

        write_node.delete(key)

        write_node.sadd(key, "a", "b", "c")

        print(f"  SADD '{key}' a b c (all exist)...")
        try:
            result = write_node.sadd(key, "a", "b", "c")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_large_number_of_members(self) -> bool:
        """Test SADD with a large number of members."""
        print("\nTest: SADD large number of members")

        key = "sadd_large"
        write_node = self._get_random_node()

        write_node.delete(key)

        members = [f"member_{i}" for i in range(1000)]
        print(f"  SADD '{key}' with 1000 members...")
        try:
            result = write_node.sadd(key, *members)
            if result != 1000:
                print(f"\033[31m  FAILED: Expected 1000, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD returned 1000: OK")

        print("  SADD again with same members to verify (should return 0)...")
        try:
            result = write_node.sadd(key, *members)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_insufficient_args(self) -> bool:
        """Test SADD with insufficient arguments returns error."""
        print("\nTest: SADD insufficient arguments")

        key = "sadd_no_args"
        write_node = self._get_random_node()

        print(f"  SADD without members...")
        try:
            write_node.sadd(key)
            print(f"\033[31m  FAILED: Expected error")
            return False
        except redis.RedisError as e:
            error_msg = str(e)
            if "wrong number" not in error_msg.lower():
                print(f"\033[31m  FAILED: Expected wrong number error, got: {e}")
                return False
            print(f"  Got expected error: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_sadd_special_characters(self) -> bool:
        """Test SADD with special characters in members."""
        print("\nTest: SADD special characters")

        key = "sadd_special"
        write_node = self._get_random_node()

        write_node.delete(key)

        special_members = ["hello world", "key:value", "a\nb\tc", "日本語", "emoji🎉"]
        print(f"  SADD '{key}' with special characters...")
        try:
            result = write_node.sadd(key, *special_members)
            if result != len(special_members):
                print(f"\033[31m  FAILED: Expected {len(special_members)}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SADD failed - {e}")
            return False

        print("  SADD returned 5: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def run_all_tests(self) -> bool:
        """Run all set tests."""
        print("\n" + "=" * 50)
        print("Running Set Tests")
        print("=" * 50)

        if not self.setup():
            return False

        tests = [
            self.test_sadd_single_member,
            self.test_sadd_multiple_members,
            self.test_sadd_duplicate_members,
            self.test_sadd_creates_key,
            self.test_sadd_wrong_type,
            self.test_sadd_empty_member,
            self.test_sadd_binary_member,
            self.test_sadd_replication,
            self.test_sadd_all_duplicates,
            self.test_sadd_large_number_of_members,
            self.test_sadd_insufficient_args,
            self.test_sadd_special_characters,
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
                print(f"\033[31m  EXCEPTION: {test.__name__} - {e}\033[0m")
                failed += 1

        print(f"\n{'=' * 60}")
        print(f"Set Tests: {passed} passed, {failed} failed")
        print(f"{'=' * 60}")

        return failed == 0


def main():
    """Main entry point."""
    tests_dir = os.path.dirname(os.path.abspath(__file__))

    def signal_handler(sig, frame):
        print("\n\nInterrupted, cleaning up...")
        cluster.stop()
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    cluster = ClusterManager(tests_dir)

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
        tester = TestClusterSet(cluster)
        success = tester.run_all_tests()

        if success:
            print("\n✅ All set tests passed!")
        else:
            print("\n❌ Some set tests failed!")

    finally:
        cluster.stop()
        cluster.clean()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    sys.exit(main())
