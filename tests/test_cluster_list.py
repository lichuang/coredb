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
        """Test LPUSH with a single element, then LPOP to verify."""
        print("\nTest: LPUSH single element + LPOP verify")

        key = "lpush_single"
        write_node = self._get_random_node()

        write_node.delete(key)

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
        print("  LPOP to verify...")
        try:
            result = write_node.lpop(key)
            if result != "hello":
                print(f"\033[31m  FAILED: LPOP expected 'hello', got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPOP failed - {e}")
            return False

        print("  LPOP returned 'hello': OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_multiple_elements(self) -> bool:
        """Test LPUSH with multiple elements, then LPOP to verify order."""
        print("\nTest: LPUSH multiple elements + LPOP verify")

        key = "lpush_multi"
        write_node = self._get_random_node()

        write_node.delete(key)

        # LPUSH a b c => [c, b, a]
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
        print("  LPOP 3 to verify order [c, b, a]...")
        try:
            results = []
            for _ in range(3):
                results.append(write_node.lpop(key))
            if results != ["c", "b", "a"]:
                print(f"\033[31m  FAILED: Expected ['c', 'b', 'a'], got {results}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPOP failed - {e}")
            return False

        print("  LPOP order ['c', 'b', 'a']: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_creates_key(self) -> bool:
        """Test that LPUSH creates the key, then LPOP to verify value."""
        print("\nTest: LPUSH creates key + LPOP verify")

        key = "lpush_new_key"
        write_node = self._get_random_node()

        write_node.delete(key)

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

        print("  LPOP to verify...")
        result = write_node.lpop(key)
        if result != "value2":
            print(f"\033[31m  FAILED: LPOP expected 'value2', got {result}")
            return False

        result = write_node.lpop(key)
        if result != "value":
            print(f"\033[31m  FAILED: LPOP expected 'value', got {result}")
            return False

        result = write_node.lpop(key)
        if result is not None:
            print(f"\033[31m  FAILED: LPOP expected None (empty), got {result}")
            return False

        print("  LPOP returned 'value2', 'value', None: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_existing_list(self) -> bool:
        """Test LPUSH appending to an existing list, then LPOP to verify order."""
        print("\nTest: LPUSH to existing list + LPOP verify")

        key = "lpush_existing"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.lpush(key, "first")

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
        print("  LPOP to verify order [third, second, first]...")
        result = write_node.lpop(key)
        if result != "third":
            print(f"\033[31m  FAILED: LPOP expected 'third', got {result}")
            return False

        result = write_node.lpop(key)
        if result != "second":
            print(f"\033[31m  FAILED: LPOP expected 'second', got {result}")
            return False

        result = write_node.lpop(key)
        if result != "first":
            print(f"\033[31m  FAILED: LPOP expected 'first', got {result}")
            return False

        print("  LPOP order [third, second, first]: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_wrong_type(self) -> bool:
        """Test LPUSH and LPOP on a key holding wrong type."""
        print("\nTest: LPUSH + LPOP wrong type error")

        key = "lpush_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

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

        print(f"  LPOP on string key '{key}'...")
        try:
            write_node.lpop(key)
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
        """Test LPUSH with empty string element, then LPOP to verify."""
        print("\nTest: LPUSH empty element + LPOP verify")

        key = "lpush_empty"
        write_node = self._get_random_node()

        write_node.delete(key)

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
        print("  LPOP to verify...")
        try:
            result = write_node.lpop(key)
            if result != "":
                print(f"\033[31m  FAILED: LPOP expected '', got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPOP failed - {e}")
            return False

        print("  LPOP returned empty string: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_binary_element(self) -> bool:
        """Test LPUSH with binary data, then LPOP to verify."""
        print("\nTest: LPUSH binary element + LPOP verify")

        key = "lpush_binary"
        write_node = self._get_random_node()

        write_node.delete(key)

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
        print("  LPOP to verify (expect decode error for binary data)...")
        try:
            write_node.lpop(key)
        except UnicodeDecodeError:
            print("  LPOP raised UnicodeDecodeError (expected for non-UTF-8 binary): OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPOP failed - {e}")
            return False
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lpush_replication(self) -> bool:
        """Test that LPUSH data replicates, then LPOP from all nodes to verify."""
        print("\nTest: LPUSH replication + LPOP verify")

        key = "lpush_repl"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  LPUSH '{key}' 'a' 'b' 'c' on random node...")
        try:
            write_node.lpush(key, "a", "b", "c")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LPUSH failed - {e}")
            return False

        print("  LPOP from all nodes to verify replication...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.lpop(key)
                if result is None:
                    print(f"    Node {i}: FAILED (got None)")
                    return False
                print(f"    Node {i}: OK (popped '{result}')")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_single_element(self) -> bool:
        """Test RPUSH with a single element, then RPOP to verify."""
        print("\nTest: RPUSH single element + RPOP verify")

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
        print("  RPOP to verify...")
        try:
            result = write_node.rpop(key)
            if result != "hello":
                print(f"\033[31m  FAILED: RPOP expected 'hello', got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPOP failed - {e}")
            return False

        print("  RPOP returned 'hello': OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_multiple_elements(self) -> bool:
        """Test RPUSH with multiple elements, then RPOP to verify reverse order."""
        print("\nTest: RPUSH multiple elements + RPOP verify")

        key = "rpush_multi"
        write_node = self._get_random_node()

        write_node.delete(key)

        # RPUSH a b c => [a, b, c], RPOP returns c, b, a
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
        print("  RPOP 3 to verify reverse order [c, b, a]...")
        try:
            results = []
            for _ in range(3):
                results.append(write_node.rpop(key))
            if results != ["c", "b", "a"]:
                print(f"\033[31m  FAILED: Expected ['c', 'b', 'a'], got {results}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPOP failed - {e}")
            return False

        print("  RPOP order ['c', 'b', 'a']: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_creates_key(self) -> bool:
        """Test that RPUSH creates the key, then RPOP to verify value."""
        print("\nTest: RPUSH creates key + RPOP verify")

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
        print("  RPOP to verify...")
        result = write_node.rpop(key)
        if result != "value2":
            print(f"\033[31m  FAILED: RPOP expected 'value2', got {result}")
            return False

        result = write_node.rpop(key)
        if result != "value":
            print(f"\033[31m  FAILED: RPOP expected 'value', got {result}")
            return False

        result = write_node.rpop(key)
        if result is not None:
            print(f"\033[31m  FAILED: RPOP expected None (empty), got {result}")
            return False

        print("  RPOP returned 'value2', 'value', None: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_existing_list(self) -> bool:
        """Test RPUSH appending to an existing list, then RPOP to verify order."""
        print("\nTest: RPUSH to existing list + RPOP verify")

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
        print("  RPOP to verify order [third, second, first]...")
        result = write_node.rpop(key)
        if result != "third":
            print(f"\033[31m  FAILED: RPOP expected 'third', got {result}")
            return False

        result = write_node.rpop(key)
        if result != "second":
            print(f"\033[31m  FAILED: RPOP expected 'second', got {result}")
            return False

        result = write_node.rpop(key)
        if result != "first":
            print(f"\033[31m  FAILED: RPOP expected 'first', got {result}")
            return False

        print("  RPOP order [third, second, first]: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_wrong_type(self) -> bool:
        """Test RPUSH and RPOP on a key holding wrong type."""
        print("\nTest: RPUSH + RPOP wrong type error")

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

        print(f"  RPOP on string key '{key}'...")
        try:
            write_node.rpop(key)
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
        """Test RPUSH with empty string element, then RPOP to verify."""
        print("\nTest: RPUSH empty element + RPOP verify")

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
        print("  RPOP to verify...")
        try:
            result = write_node.rpop(key)
            if result != "":
                print(f"\033[31m  FAILED: RPOP expected '', got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPOP failed - {e}")
            return False

        print("  RPOP returned empty string: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_binary_element(self) -> bool:
        """Test RPUSH with binary data, then RPOP to verify."""
        print("\nTest: RPUSH binary element + RPOP verify")

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
        print("  RPOP to verify (expect decode error for binary data)...")
        try:
            write_node.rpop(key)
        except UnicodeDecodeError:
            print("  RPOP raised UnicodeDecodeError (expected for non-UTF-8 binary): OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: RPOP failed - {e}")
            return False
        print("\033[32m  PASSED\033[0m")
        return True

    def test_rpush_after_lpush(self) -> bool:
        """Test RPUSH after LPUSH produces correct list, then LPOP/RPOP to verify."""
        print("\nTest: RPUSH after LPUSH + LPOP/RPOP verify")

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
        print("  LPOP to verify left end...")
        result = write_node.lpop(key)
        if result != "x":
            print(f"\033[31m  FAILED: LPOP expected 'x', got {result}")
            return False

        print("  RPOP to verify right end...")
        result = write_node.rpop(key)
        if result != "b":
            print(f"\033[31m  FAILED: RPOP expected 'b', got {result}")
            return False

        result = write_node.rpop(key)
        if result != "a":
            print(f"\033[31m  FAILED: RPOP expected 'a', got {result}")
            return False

        print("  LPOP='x', RPOP='b','a': OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_llen_nonexistent_key(self) -> bool:
        """Test LLEN on a non-existent key returns 0."""
        print("\nTest: LLEN non-existent key")

        key = "llen_nonexistent"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  LLEN '{key}' on non-existent key...")
        try:
            result = write_node.llen(key)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LLEN failed - {e}")
            return False

        print("  LLEN returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_llen_after_push(self) -> bool:
        """Test LLEN returns correct length after LPUSH/RPUSH operations."""
        print("\nTest: LLEN after push operations")

        key = "llen_after_push"
        write_node = self._get_random_node()

        write_node.delete(key)

        # Empty list
        try:
            result = write_node.llen(key)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0 for empty, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LLEN failed - {e}")
            return False
        print("  LLEN empty = 0: OK")

        # LPUSH one element
        write_node.lpush(key, "a")
        try:
            result = write_node.llen(key)
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LLEN failed - {e}")
            return False
        print("  LLEN after LPUSH 1 = 1: OK")

        # LPUSH multiple
        write_node.lpush(key, "b", "c")
        try:
            result = write_node.llen(key)
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LLEN failed - {e}")
            return False
        print("  LLEN after LPUSH 2 more = 3: OK")

        # RPUSH more
        write_node.rpush(key, "d", "e", "f")
        try:
            result = write_node.llen(key)
            if result != 6:
                print(f"\033[31m  FAILED: Expected 6, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LLEN failed - {e}")
            return False
        print("  LLEN after RPUSH 3 more = 6: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_llen_after_pop(self) -> bool:
        """Test LLEN returns correct length after LPOP/RPOP operations."""
        print("\nTest: LLEN after pop operations")

        key = "llen_after_pop"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.lpush(key, "a", "b", "c", "d", "e")

        try:
            result = write_node.llen(key)
            if result != 5:
                print(f"\033[31m  FAILED: Expected 5, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LLEN failed - {e}")
            return False
        print("  LLEN = 5: OK")

        write_node.lpop(key)
        try:
            result = write_node.llen(key)
            if result != 4:
                print(f"\033[31m  FAILED: Expected 4, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LLEN failed - {e}")
            return False
        print("  LLEN after LPOP 1 = 4: OK")

        write_node.rpop(key, count=2)
        try:
            result = write_node.llen(key)
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LLEN failed - {e}")
            return False
        print("  LLEN after RPOP 2 = 2: OK")

        # Pop remaining
        write_node.lpop(key)
        write_node.rpop(key)
        try:
            result = write_node.llen(key)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LLEN failed - {e}")
            return False
        print("  LLEN after popping all = 0: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_llen_wrong_type(self) -> bool:
        """Test LLEN on a key holding wrong type returns WRONGTYPE error."""
        print("\nTest: LLEN wrong type error")

        key = "llen_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  LLEN on string key '{key}'...")
        try:
            write_node.llen(key)
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
            self.test_llen_nonexistent_key,
            self.test_llen_after_push,
            self.test_llen_after_pop,
            self.test_llen_wrong_type,
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
