#!/usr/bin/env python3
"""
CoreDB Cluster Bitmap Integration Tests

Usage:
    pip install -r requirements.txt
    python test_cluster_bitmap.py
"""

import sys
import os
import signal

import redis

from cluster_manager import ClusterManager
from base_test import TestClusterBase


class TestClusterBitmap(TestClusterBase):
    """Bitmap command tests."""

    def test_setbit_getbit_basic(self) -> bool:
        """Test SETBIT and GETBIT basic operation."""
        print("\nTest: SETBIT/GETBIT basic")

        key = "bmp_basic"
        node = self._get_random_node()
        node.delete(key)

        # Non-existent key returns 0
        try:
            result = node.getbit(key, 0)
            if result != 0:
                print(f"\033[31m  FAILED: GETBIT on non-existent key expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: GETBIT failed - {e}")
            return False

        # SETBIT returns old value (0)
        try:
            result = node.setbit(key, 0, True)
            if result != 0:
                print(f"\033[31m  FAILED: SETBIT expected 0 (old value), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SETBIT failed - {e}")
            return False

        # GETBIT returns 1
        try:
            result = node.getbit(key, 0)
            if result != 1:
                print(f"\033[31m  FAILED: GETBIT expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: GETBIT failed - {e}")
            return False

        # SETBIT again returns old value (1)
        try:
            result = node.setbit(key, 0, True)
            if result != 1:
                print(f"\033[31m  FAILED: SETBIT expected 1 (old value), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SETBIT failed - {e}")
            return False

        # Unset bit
        try:
            result = node.setbit(key, 0, False)
            if result != 1:
                print(f"\033[31m  FAILED: SETBIT expected 1 (old value), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SETBIT failed - {e}")
            return False

        try:
            result = node.getbit(key, 0)
            if result != 0:
                print(f"\033[31m  FAILED: GETBIT expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: GETBIT failed - {e}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_setbit_multiple_bits(self) -> bool:
        """Test SETBIT with multiple bit positions."""
        print("\nTest: SETBIT multiple bits")

        key = "bmp_multi"
        node = self._get_random_node()
        node.delete(key)

        for i in [0, 7, 8, 15, 100, 8191]:
            node.setbit(key, i, True)

        for i in [0, 7, 8, 15, 100, 8191]:
            try:
                result = node.getbit(key, i)
                if result != 1:
                    print(f"\033[31m  FAILED: bit {i} expected 1, got {result}")
                    return False
            except redis.RedisError as e:
                print(f"\033[31m  FAILED: GETBIT failed - {e}")
                return False

        # Unset bits should be 0
        for i in [1, 6, 9, 14, 50, 8190]:
            try:
                result = node.getbit(key, i)
                if result != 0:
                    print(f"\033[31m  FAILED: bit {i} expected 0, got {result}")
                    return False
            except redis.RedisError as e:
                print(f"\033[31m  FAILED: GETBIT failed - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_setbit_cross_fragment(self) -> bool:
        """Test SETBIT across fragment boundaries (8192 bits per fragment)."""
        print("\nTest: SETBIT cross fragment")

        key = "bmp_cross"
        node = self._get_random_node()
        node.delete(key)

        # Bit at end of fragment 0
        node.setbit(key, 8191, True)
        # Bit at start of fragment 1
        node.setbit(key, 8192, True)
        # Bit in middle of fragment 1
        node.setbit(key, 12288, True)

        try:
            assert node.getbit(key, 8191) == 1
            assert node.getbit(key, 8192) == 1
            assert node.getbit(key, 12288) == 1
            assert node.getbit(key, 8190) == 0
            assert node.getbit(key, 8193) == 0
        except AssertionError:
            print(f"\033[31m  FAILED: cross fragment bits incorrect")
            return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: GETBIT failed - {e}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_setbit_wrong_type(self) -> bool:
        """Test SETBIT on a key holding wrong type."""
        print("\nTest: SETBIT wrong type")

        key = "bmp_wrong"
        node = self._get_random_node()
        node.set(key, "string_value")

        try:
            node.setbit(key, 0, True)
            print(f"\033[31m  FAILED: Expected WRONGTYPE error")
            return False
        except redis.ResponseError as e:
            if "WRONGTYPE" not in str(e):
                print(f"\033[31m  FAILED: Expected WRONGTYPE, got: {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_getbit_wrong_type(self) -> bool:
        """Test GETBIT on a key holding wrong type."""
        print("\nTest: GETBIT wrong type")

        key = "bmp_wrong2"
        node = self._get_random_node()
        node.set(key, "string_value")

        try:
            node.getbit(key, 0)
            print(f"\033[31m  FAILED: Expected WRONGTYPE error")
            return False
        except redis.ResponseError as e:
            if "WRONGTYPE" not in str(e):
                print(f"\033[31m  FAILED: Expected WRONGTYPE, got: {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_getbit_nonexistent_key(self) -> bool:
        """Test GETBIT on non-existent key returns 0."""
        print("\nTest: GETBIT non-existent key")

        key = "bmp_noexist"
        node = self._get_random_node()
        node.delete(key)

        try:
            result = node.getbit(key, 99999)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: GETBIT failed - {e}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_setbit_getbit_large_offset(self) -> bool:
        """Test SETBIT/GETBIT with very large offset."""
        print("\nTest: SETBIT/GETBIT large offset")

        key = "bmp_large"
        node = self._get_random_node()
        node.delete(key)

        large_offset = 1000000
        try:
            result = node.setbit(key, large_offset, True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SETBIT failed - {e}")
            return False

        try:
            result = node.getbit(key, large_offset)
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: GETBIT failed - {e}")
            return False

        # Nearby bits should be 0
        try:
            assert node.getbit(key, large_offset - 1) == 0
            assert node.getbit(key, large_offset + 1) == 0
        except AssertionError:
            print(f"\033[31m  FAILED: nearby bits should be 0")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_setbit_replication(self) -> bool:
        """Test SETBIT data replicates to all nodes."""
        print("\nTest: SETBIT replication")

        key = "bmp_repl"
        node = self._get_random_node()
        node.delete(key)

        node.setbit(key, 42, True)

        for i, n in enumerate(self.nodes, 1):
            try:
                result = n.conn.getbit(key, 42)
                if result != 1:
                    print(f"    Node {i}: FAILED (expected 1, got {result})")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_setbit_invalid_args(self) -> bool:
        """Test SETBIT with invalid arguments."""
        print("\nTest: SETBIT invalid arguments")

        key = "bmp_inv"
        node = self._get_random_node()
        node.delete(key)

        # Invalid bit value (only 0 or 1)
        try:
            node.execute_command("SETBIT", key, "0", "2")
            print(f"\033[31m  FAILED: Expected error for bit=2")
            return False
        except redis.ResponseError as e:
            if "not an integer or out of range" not in str(e):
                print(f"\033[31m  FAILED: Expected out of range error, got: {e}")
                return False

        # Missing arguments
        try:
            node.execute_command("SETBIT", key)
            print(f"\033[31m  FAILED: Expected error for missing args")
            return False
        except redis.ResponseError:
            pass

        print("\033[32m  PASSED\033[0m")
        return True

    def test_getbit_invalid_args(self) -> bool:
        """Test GETBIT with invalid arguments."""
        print("\nTest: GETBIT invalid arguments")

        key = "bmp_inv2"
        node = self._get_random_node()
        node.delete(key)

        try:
            node.execute_command("GETBIT", key, "abc")
            print(f"\033[31m  FAILED: Expected error for non-integer offset")
            return False
        except redis.ResponseError as e:
            if "not an integer or out of range" not in str(e):
                print(f"\033[31m  FAILED: Expected out of range error, got: {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def run_all_tests(self) -> bool:
        """Run all bitmap tests."""
        print("\n" + "=" * 50)
        print("Running Bitmap Tests")
        print("=" * 50)

        if not self.setup():
            return False

        tests = [
            self.test_setbit_getbit_basic,
            self.test_setbit_multiple_bits,
            self.test_setbit_cross_fragment,
            self.test_setbit_wrong_type,
            self.test_getbit_wrong_type,
            self.test_getbit_nonexistent_key,
            self.test_setbit_getbit_large_offset,
            self.test_setbit_replication,
            self.test_setbit_invalid_args,
            self.test_getbit_invalid_args,
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
        print(f"Bitmap Tests: {passed} passed, {failed} failed")
        print(f"{'=' * 60}")

        return failed == 0


def main():
    tests_dir = os.path.dirname(os.path.abspath(__file__))

    def signal_handler(sig, frame):
        print("\n\nInterrupted, cleaning up...")
        cluster.stop()
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    cluster = ClusterManager(tests_dir)
    cluster.clean()

    if not cluster.build():
        print("Build failed, exiting")
        sys.exit(1)

    if not cluster.start():
        print("Failed to start cluster, exiting")
        cluster.stop()
        sys.exit(1)

    try:
        tester = TestClusterBitmap(cluster)
        success = tester.run_all_tests()

        if success:
            print("\n✅ All bitmap tests passed!")
        else:
            print("\n❌ Some bitmap tests failed!")

    finally:
        cluster.stop()
        cluster.clean()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    sys.exit(main())
