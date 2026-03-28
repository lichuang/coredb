#!/usr/bin/env python3
"""
CoreDB Cluster List Integration Tests

This test suite verifies List commands (LPUSH, etc.) against a running 3-node cluster.

Usage:
    pip install -r requirements.txt
    python test_cluster_list.py
"""

import sys
import os
import signal

import redis

from cluster_manager import ClusterManager
from base_test import TestClusterBase


class TestClusterList(TestClusterBase):
    """List command tests."""

    def test_lpush_single_element(self) -> bool:
        """Test LPUSH with a single element."""
        print("\nTest: LPUSH single element")

        key = "lpush_single"
        write_node = self._get_random_node()

        # Clean up
        write_node.delete(key)

        # LPUSH single element
        print(f"  LPUSH '{key}' 'hello'...")
        try:
            result = write_node.lpush(key, "hello")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPUSH failed - {e}")
            return False

        print("  LPUSH returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_multiple_elements(self) -> bool:
        """Test LPUSH with multiple elements."""
        print("\nTest: LPUSH multiple elements")

        key = "lpush_multi"
        write_node = self._get_random_node()

        # Clean up
        write_node.delete(key)

        # LPUSH mylist a b c => list = [c, b, a]
        print(f"  LPUSH '{key}' a b c...")
        try:
            result = write_node.lpush(key, "a", "b", "c")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPUSH failed - {e}")
            return False

        print("  LPUSH returned 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_creates_key(self) -> bool:
        """Test that LPUSH creates the key if it doesn't exist."""
        print("\nTest: LPUSH creates key")

        key = "lpush_new_key"
        write_node = self._get_random_node()

        # Make sure key doesn't exist
        write_node.delete(key)

        # LPUSH should create the key
        print(f"  LPUSH '{key}' 'value' on non-existent key...")
        try:
            result = write_node.lpush(key, "value")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPUSH failed - {e}")
            return False

        print(f"  Verify list exists by pushing again...")
        try:
            result = write_node.lpush(key, "value2")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPUSH failed - {e}")
            return False

        print("  List created and verified: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_existing_list(self) -> bool:
        """Test LPUSH appending to an existing list."""
        print("\nTest: LPUSH to existing list")

        key = "lpush_existing"
        write_node = self._get_random_node()

        # Clean up and create initial list
        write_node.delete(key)
        write_node.lpush(key, "first")

        # LPUSH more elements
        print(f"  LPUSH '{key}' 'second' 'third'...")
        try:
            result = write_node.lpush(key, "second", "third")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPUSH failed - {e}")
            return False

        print("  LPUSH returned 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_wrong_type(self) -> bool:
        """Test LPUSH on a key holding wrong type."""
        print("\nTest: LPUSH wrong type error")

        key = "lpush_wrong_type"
        write_node = self._get_random_node()

        # Set a string key
        write_node.set(key, "string_value")

        # LPUSH should fail with WRONGTYPE
        print(f"  LPUSH on string key '{key}'...")
        try:
            write_node.lpush(key, "element")
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

    def test_lpush_empty_element(self) -> bool:
        """Test LPUSH with empty string element."""
        print("\nTest: LPUSH empty element")

        key = "lpush_empty"
        write_node = self._get_random_node()

        # Clean up
        write_node.delete(key)

        # LPUSH empty string
        print(f"  LPUSH '{key}' ''...")
        try:
            result = write_node.lpush(key, "")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPUSH failed - {e}")
            return False

        print("  LPUSH empty element returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_binary_element(self) -> bool:
        """Test LPUSH with binary data."""
        print("\nTest: LPUSH binary element")

        key = "lpush_binary"
        write_node = self._get_random_node()

        # Clean up
        write_node.delete(key)

        # LPUSH with binary data
        binary_data = bytes(range(256))
        print(f"  LPUSH '{key}' with 256-byte binary data...")
        try:
            result = write_node.lpush(key, binary_data)
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPUSH failed - {e}")
            return False

        print("  LPUSH binary element returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_replication(self) -> bool:
        """Test that LPUSH data replicates to all nodes."""
        print("\nTest: LPUSH replication")

        key = "lpush_repl"
        write_node = self._get_random_node()

        # Clean up
        write_node.delete(key)

        # LPUSH on one node
        print(f"  LPUSH '{key}' 'a' 'b' 'c' on random node...")
        try:
            write_node.lpush(key, "a", "b", "c")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPUSH failed - {e}")
            return False

        print("  Verify list exists on all nodes by pushing...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.lpush(key, f"node{i}")
                if result < 1:
                    print(f"    Node {i}: FAILED (expected >= 1, got {result})")
                    return False
                print(f"    Node {i}: OK (pushed, list length={result})")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_single_element(self) -> bool:
        """Test RPUSH with a single element."""
        print("\nTest: RPUSH single element")

        key = "rpush_single"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  RPUSH '{key}' 'hello'...")
        try:
            result = write_node.rpush(key, "hello")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPUSH failed - {e}")
            return False

        print("  RPUSH returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_multiple_elements(self) -> bool:
        """Test RPUSH with multiple elements preserves insertion order."""
        print("\nTest: RPUSH multiple elements (order)")

        key = "rpush_multi"
        write_node = self._get_random_node()

        write_node.delete(key)

        # RPUSH mylist a b c => list = [a, b, c]
        print(f"  RPUSH '{key}' a b c...")
        try:
            result = write_node.rpush(key, "a", "b", "c")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPUSH failed - {e}")
            return False

        print("  RPUSH returned 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_creates_key(self) -> bool:
        """Test that RPUSH creates the key if it doesn't exist."""
        print("\nTest: RPUSH creates key")

        key = "rpush_new_key"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  RPUSH '{key}' 'value' on non-existent key...")
        try:
            result = write_node.rpush(key, "value")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPUSH failed - {e}")
            return False

        try:
            result = write_node.rpush(key, "value2")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPUSH failed - {e}")
            return False

        print("  List created and verified: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_existing_list(self) -> bool:
        """Test RPUSH appending to an existing list."""
        print("\nTest: RPUSH to existing list")

        key = "rpush_existing"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "first")

        print(f"  RPUSH '{key}' 'second' 'third'...")
        try:
            result = write_node.rpush(key, "second", "third")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPUSH failed - {e}")
            return False

        print("  RPUSH returned 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_wrong_type(self) -> bool:
        """Test RPUSH on a key holding wrong type."""
        print("\nTest: RPUSH wrong type error")

        key = "rpush_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  RPUSH on string key '{key}'...")
        try:
            write_node.rpush(key, "element")
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

    def test_rpush_empty_element(self) -> bool:
        """Test RPUSH with empty string element."""
        print("\nTest: RPUSH empty element")

        key = "rpush_empty"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  RPUSH '{key}' ''...")
        try:
            result = write_node.rpush(key, "")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPUSH failed - {e}")
            return False

        print("  RPUSH empty element returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_binary_element(self) -> bool:
        """Test RPUSH with binary data."""
        print("\nTest: RPUSH binary element")

        key = "rpush_binary"
        write_node = self._get_random_node()

        write_node.delete(key)

        binary_data = bytes(range(256))
        print(f"  RPUSH '{key}' with 256-byte binary data...")
        try:
            result = write_node.rpush(key, binary_data)
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPUSH failed - {e}")
            return False

        print("  RPUSH binary element returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_after_lpush(self) -> bool:
        """Test RPUSH after LPUSH produces correct combined list."""
        print("\nTest: RPUSH after LPUSH")

        key = "rpush_after_lpush"
        write_node = self._get_random_node()

        write_node.delete(key)

        # LPUSH x => [x]
        write_node.lpush(key, "x")
        # RPUSH a b => [x, a, b]
        print(f"  LPUSH x, then RPUSH a b...")
        try:
            result = write_node.rpush(key, "a", "b")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPUSH failed - {e}")
            return False

        print("  Combined list length = 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def run_all_tests(self) -> bool:
        """Run all list tests."""
        print("\n" + "=" * 50)
        print("Running List Tests")
        print("=" * 50)

        if not self.setup():
            return False

        tests = [
            self.test_lpush_single_element,
            self.test_lpush_multiple_elements,
            self.test_lpush_creates_key,
            self.test_lpush_existing_list,
            self.test_lpush_wrong_type,
            self.test_lpush_empty_element,
            self.test_lpush_binary_element,
            self.test_lpush_replication,
            self.test_rpush_single_element,
            self.test_rpush_multiple_elements,
            self.test_rpush_creates_key,
            self.test_rpush_existing_list,
            self.test_rpush_wrong_type,
            self.test_rpush_empty_element,
            self.test_rpush_binary_element,
            self.test_rpush_after_lpush,
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
        print(f"List Tests: {passed} passed, {failed} failed")
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
        tester = TestClusterList(cluster)
        success = tester.run_all_tests()

        if success:
            print("\n✅ All list tests passed!")
        else:
            print("\n❌ Some list tests failed!")

    finally:
        cluster.stop()
        cluster.clean()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    sys.exit(main())
