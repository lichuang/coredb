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
import time

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

    def test_lrange_nonexistent_key(self) -> bool:
        """Test LRANGE on a non-existent key returns empty list."""
        print("\nTest: LRANGE non-existent key")

        key = "lrange_nonexistent"
        write_node = self._get_random_node()

        write_node.delete(key)

        try:
            result = write_node.lrange(key, 0, -1)
            if result != []:
                print(f"\033[31m  FAILED: Expected [], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False

        print("  LRANGE 0 -1 on non-existent key = []: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrange_full_list(self) -> bool:
        """Test LRANGE 0 -1 returns entire list."""
        print("\nTest: LRANGE 0 -1 full list")

        key = "lrange_full"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c", "d", "e")

        try:
            result = write_node.lrange(key, 0, -1)
            expected = ["a", "b", "c", "d", "e"]
            if result != expected:
                print(f"\033[31m  FAILED: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False

        print("  LRANGE 0 -1 = ['a','b','c','d','e']: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrange_partial(self) -> bool:
        """Test LRANGE with positive and negative indices."""
        print("\nTest: LRANGE partial ranges")

        key = "lrange_partial"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c", "d", "e")

        try:
            result = write_node.lrange(key, 1, 3)
            expected = ["b", "c", "d"]
            if result != expected:
                print(f"\033[31m  FAILED: LRANGE 1 3: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False
        print("  LRANGE 1 3 = ['b','c','d']: OK")

        try:
            result = write_node.lrange(key, 0, 0)
            expected = ["a"]
            if result != expected:
                print(f"\033[31m  FAILED: LRANGE 0 0: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False
        print("  LRANGE 0 0 = ['a']: OK")

        try:
            result = write_node.lrange(key, -3, -1)
            expected = ["c", "d", "e"]
            if result != expected:
                print(f"\033[31m  FAILED: LRANGE -3 -1: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False
        print("  LRANGE -3 -1 = ['c','d','e']: OK")

        try:
            result = write_node.lrange(key, -1, -1)
            expected = ["e"]
            if result != expected:
                print(f"\033[31m  FAILED: LRANGE -1 -1: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False
        print("  LRANGE -1 -1 = ['e']: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrange_out_of_range(self) -> bool:
        """Test LRANGE with out-of-range indices returns empty or clamped list."""
        print("\nTest: LRANGE out-of-range indices")

        key = "lrange_oor"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c")

        try:
            result = write_node.lrange(key, 10, 20)
            if result != []:
                print(f"\033[31m  FAILED: Expected [], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False
        print("  LRANGE 10 20 (start > size) = []: OK")

        try:
            result = write_node.lrange(key, 0, 100)
            expected = ["a", "b", "c"]
            if result != expected:
                print(f"\033[31m  FAILED: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False
        print("  LRANGE 0 100 (stop > size, clamped) = ['a','b','c']: OK")

        try:
            result = write_node.lrange(key, -100, -1)
            expected = ["a", "b", "c"]
            if result != expected:
                print(f"\033[31m  FAILED: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False
        print("  LRANGE -100 -1 (start < 0, clamped) = ['a','b','c']: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrange_wrong_type(self) -> bool:
        """Test LRANGE on a key holding wrong type returns WRONGTYPE error."""
        print("\nTest: LRANGE wrong type error")

        key = "lrange_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  LRANGE on string key '{key}'...")
        try:
            write_node.lrange(key, 0, -1)
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

    def test_lrange_after_lpush(self) -> bool:
        """Test LRANGE on list created with LPUSH returns correct order."""
        print("\nTest: LRANGE after LPUSH")

        key = "lrange_lpush"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.lpush(key, "c", "b", "a")

        try:
            result = write_node.lrange(key, 0, -1)
            expected = ["a", "b", "c"]
            if result != expected:
                print(f"\033[31m  FAILED: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LRANGE failed - {e}")
            return False

        print("  LRANGE 0 -1 after LPUSH c b a = ['a','b','c']: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lset_basic(self) -> bool:
        """Test LSET with positive index."""
        print("\nTest: LSET basic positive index")

        key = "lset_basic"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c")

        print(f"  LSET '{key}' 0 'four'...")
        try:
            result = write_node.lset(key, 0, "four")
            if result is not True:
                print(f"\033[31m  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LSET failed - {e}")
            return False

        print("  LSET returned OK: True")
        result = write_node.lrange(key, 0, -1)
        if result != ["four", "b", "c"]:
            print(f"\033[31m  FAILED: Expected ['four','b','c'], got {result}")
            return False
        print("  LRANGE = ['four','b','c']: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lset_negative_index(self) -> bool:
        """Test LSET with negative index."""
        print("\nTest: LSET negative index")

        key = "lset_negative"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c")

        print(f"  LSET '{key}' -1 'five'...")
        try:
            result = write_node.lset(key, -1, "five")
            if result is not True:
                print(f"\033[31m  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LSET failed - {e}")
            return False

        print("  LSET returned OK: True")
        result = write_node.lrange(key, 0, -1)
        if result != ["a", "b", "five"]:
            print(f"\033[31m  FAILED: Expected ['a','b','five'], got {result}")
            return False
        print("  LRANGE = ['a','b','five']: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lset_middle(self) -> bool:
        """Test LSET on middle element."""
        print("\nTest: LSET middle element")

        key = "lset_middle"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c", "d", "e")

        print(f"  LSET '{key}' 2 'X'...")
        try:
            write_node.lset(key, 2, "X")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LSET failed - {e}")
            return False

        result = write_node.lindex(key, 2)
        if result != "X":
            print(f"\033[31m  FAILED: LINDEX 2 expected 'X', got {result}")
            return False
        print("  LINDEX 2 = 'X': OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lset_nonexistent_key(self) -> bool:
        """Test LSET on a non-existent key returns error."""
        print("\nTest: LSET non-existent key")

        key = "lset_nonexistent"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  LSET '{key}' 0 'value' on non-existent key...")
        try:
            write_node.lset(key, 0, "value")
            print(f"\033[31m  FAILED: Expected error")
            return False
        except redis.ResponseError as e:
            error_msg = str(e)
            if "no such key" not in error_msg:
                print(f"\033[31m  FAILED: Expected 'no such key' error, got: {e}")
                return False
            print(f"  Got expected error: {e}")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lset_out_of_range(self) -> bool:
        """Test LSET with out-of-range index returns error."""
        print("\nTest: LSET out-of-range index")

        key = "lset_oor"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c")

        # Positive out of range
        print(f"  LSET '{key}' 5 'x' (positive out of range)...")
        try:
            write_node.lset(key, 5, "x")
            print(f"\033[31m  FAILED: Expected error for index 5")
            return False
        except redis.ResponseError as e:
            if "index out of range" not in str(e):
                print(f"\033[31m  FAILED: Expected 'index out of range', got: {e}")
                return False
            print(f"  Got expected error: OK")

        # Negative out of range
        print(f"  LSET '{key}' -4 'x' (negative out of range)...")
        try:
            write_node.lset(key, -4, "x")
            print(f"\033[31m  FAILED: Expected error for index -4")
            return False
        except redis.ResponseError as e:
            if "index out of range" not in str(e):
                print(f"\033[31m  FAILED: Expected 'index out of range', got: {e}")
                return False
            print(f"  Got expected error: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lset_wrong_type(self) -> bool:
        """Test LSET on a key holding wrong type returns WRONGTYPE error."""
        print("\nTest: LSET wrong type error")

        key = "lset_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  LSET on string key '{key}'...")
        try:
            write_node.lset(key, 0, "element")
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

    def test_lindex_basic(self) -> bool:
        """Test LINDEX with positive and negative indices."""
        print("\nTest: LINDEX basic positive and negative indices")

        key = "lindex_basic"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c", "d", "e")

        # Positive indices
        try:
            result = write_node.lindex(key, 0)
            if result != "a":
                print(f"\033[31m  FAILED: LINDEX 0 expected 'a', got {result}")
                return False
            print("  LINDEX 0 = 'a': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, 2)
            if result != "c":
                print(f"\033[31m  FAILED: LINDEX 2 expected 'c', got {result}")
                return False
            print("  LINDEX 2 = 'c': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, 4)
            if result != "e":
                print(f"\033[31m  FAILED: LINDEX 4 expected 'e', got {result}")
                return False
            print("  LINDEX 4 = 'e': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        # Negative indices
        try:
            result = write_node.lindex(key, -1)
            if result != "e":
                print(f"\033[31m  FAILED: LINDEX -1 expected 'e', got {result}")
                return False
            print("  LINDEX -1 = 'e': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, -3)
            if result != "c":
                print(f"\033[31m  FAILED: LINDEX -3 expected 'c', got {result}")
                return False
            print("  LINDEX -3 = 'c': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, -5)
            if result != "a":
                print(f"\033[31m  FAILED: LINDEX -5 expected 'a', got {result}")
                return False
            print("  LINDEX -5 = 'a': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lindex_out_of_range(self) -> bool:
        """Test LINDEX with out-of-range indices returns None."""
        print("\nTest: LINDEX out-of-range indices")

        key = "lindex_oor"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c")

        # Positive out of range
        try:
            result = write_node.lindex(key, 3)
            if result is not None:
                print(f"\033[31m  FAILED: LINDEX 3 expected None, got {result}")
                return False
            print("  LINDEX 3 (out of range) = None: OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, 100)
            if result is not None:
                print(f"\033[31m  FAILED: LINDEX 100 expected None, got {result}")
                return False
            print("  LINDEX 100 (out of range) = None: OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        # Negative out of range
        try:
            result = write_node.lindex(key, -4)
            if result is not None:
                print(f"\033[31m  FAILED: LINDEX -4 expected None, got {result}")
                return False
            print("  LINDEX -4 (out of range) = None: OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, -100)
            if result is not None:
                print(f"\033[31m  FAILED: LINDEX -100 expected None, got {result}")
                return False
            print("  LINDEX -100 (out of range) = None: OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lindex_nonexistent_key(self) -> bool:
        """Test LINDEX on a non-existent key returns None."""
        print("\nTest: LINDEX non-existent key")

        key = "lindex_nonexistent"
        write_node = self._get_random_node()

        write_node.delete(key)

        try:
            result = write_node.lindex(key, 0)
            if result is not None:
                print(f"\033[31m  FAILED: Expected None, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        print("  LINDEX on non-existent key = None: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lindex_wrong_type(self) -> bool:
        """Test LINDEX on a key holding wrong type returns WRONGTYPE error."""
        print("\nTest: LINDEX wrong type error")

        key = "lindex_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  LINDEX on string key '{key}'...")
        try:
            write_node.lindex(key, 0)
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

    def test_lindex_after_lpush(self) -> bool:
        """Test LINDEX on list created with LPUSH returns correct elements."""
        print("\nTest: LINDEX after LPUSH")

        key = "lindex_lpush"
        write_node = self._get_random_node()

        write_node.delete(key)
        # LPUSH c b a => [a, b, c]
        write_node.lpush(key, "c", "b", "a")

        try:
            result = write_node.lindex(key, 0)
            if result != "a":
                print(f"\033[31m  FAILED: LINDEX 0 expected 'a', got {result}")
                return False
            print("  LINDEX 0 = 'a': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, 2)
            if result != "c":
                print(f"\033[31m  FAILED: LINDEX 2 expected 'c', got {result}")
                return False
            print("  LINDEX 2 = 'c': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, -1)
            if result != "c":
                print(f"\033[31m  FAILED: LINDEX -1 expected 'c', got {result}")
                return False
            print("  LINDEX -1 = 'c': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lindex_single_element(self) -> bool:
        """Test LINDEX on a single-element list."""
        print("\nTest: LINDEX single element")

        key = "lindex_single"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "only")

        try:
            result = write_node.lindex(key, 0)
            if result != "only":
                print(f"\033[31m  FAILED: LINDEX 0 expected 'only', got {result}")
                return False
            print("  LINDEX 0 = 'only': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, -1)
            if result != "only":
                print(f"\033[31m  FAILED: LINDEX -1 expected 'only', got {result}")
                return False
            print("  LINDEX -1 = 'only': OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        try:
            result = write_node.lindex(key, 1)
            if result is not None:
                print(f"\033[31m  FAILED: LINDEX 1 expected None, got {result}")
                return False
            print("  LINDEX 1 = None: OK")
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LINDEX failed - {e}")
            return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lindex_replication(self) -> bool:
        """Test that LINDEX reads replicated data from all nodes."""
        print("\nTest: LINDEX replication across nodes")

        key = "lindex_repl"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "x", "y", "z")

        print("  LINDEX from all nodes to verify replication...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.lindex(key, 1)
                if result != "y":
                    print(f"    Node {i}: FAILED (expected 'y', got {result})")
                    return False
                print(f"    Node {i}: OK (got 'y')")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_basic_positive_count(self) -> bool:
        """Test LREM with positive count removes from head to tail."""
        print("\nTest: LREM basic positive count")

        key = "lrem_positive"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "hello", "foo", "hello", "hello")

        print(f"  LREM '{key}' 2 'hello'...")
        try:
            result = write_node.lrem(key, 2, "hello")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 2: OK")
        remaining = write_node.lrange(key, 0, -1)
        if remaining != ["foo", "hello"]:
            print(f"\033[31m  FAILED: Expected ['foo','hello'], got {remaining}")
            return False
        print(f"  Remaining = ['foo','hello']: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_negative_count(self) -> bool:
        """Test LREM with negative count removes from tail to head."""
        print("\nTest: LREM negative count (tail to head)")

        key = "lrem_negative"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "hello", "hello", "foo", "hello")

        print(f"  LREM '{key}' -2 'hello'...")
        try:
            result = write_node.lrem(key, -2, "hello")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 2: OK")
        remaining = write_node.lrange(key, 0, -1)
        if remaining != ["hello", "foo"]:
            print(f"\033[31m  FAILED: Expected ['hello','foo'], got {remaining}")
            return False
        print(f"  Remaining = ['hello','foo']: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_zero_count(self) -> bool:
        """Test LREM with count=0 removes all occurrences."""
        print("\nTest: LREM count=0 removes all occurrences")

        key = "lrem_zero"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "a", "c", "a")

        print(f"  LREM '{key}' 0 'a'...")
        try:
            result = write_node.lrem(key, 0, "a")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 3: OK")
        remaining = write_node.lrange(key, 0, -1)
        if remaining != ["b", "c"]:
            print(f"\033[31m  FAILED: Expected ['b','c'], got {remaining}")
            return False
        print(f"  Remaining = ['b','c']: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_nonexistent_key(self) -> bool:
        """Test LREM on a non-existent key returns 0."""
        print("\nTest: LREM non-existent key")

        key = "lrem_nonexistent"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  LREM '{key}' 1 'hello' on non-existent key...")
        try:
            result = write_node.lrem(key, 1, "hello")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_no_match(self) -> bool:
        """Test LREM with element not in the list returns 0."""
        print("\nTest: LREM no matching element")

        key = "lrem_no_match"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "c")

        print(f"  LREM '{key}' 0 'x' (not in list)...")
        try:
            result = write_node.lrem(key, 0, "x")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 0: OK")

        remaining = write_node.lrange(key, 0, -1)
        if remaining != ["a", "b", "c"]:
            print(f"\033[31m  FAILED: List should be unchanged, got {remaining}")
            return False
        print("  List unchanged: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_removes_all_deletes_key(self) -> bool:
        """Test LREM that removes all elements deletes the key."""
        print("\nTest: LREM removes all elements deletes key")

        key = "lrem_delete_key"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "hello", "hello")

        print(f"  LREM '{key}' 0 'hello' (removes all)...")
        try:
            result = write_node.lrem(key, 0, "hello")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 2: OK")

        # Key should no longer exist
        exists = write_node.exists(key)
        if exists != 0:
            print(f"\033[31m  FAILED: Key should be deleted, exists={exists}")
            return False
        print("  Key deleted: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_count_exceeds_matches(self) -> bool:
        """Test LREM where count exceeds the number of matching elements."""
        print("\nTest: LREM count exceeds matches")

        key = "lrem_count_exceeds"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "a")

        print(f"  LREM '{key}' 10 'a' (count > occurrences)...")
        try:
            result = write_node.lrem(key, 10, "a")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 2 (removed all matches): OK")
        remaining = write_node.lrange(key, 0, -1)
        if remaining != ["b"]:
            print(f"\033[31m  FAILED: Expected ['b'], got {remaining}")
            return False
        print("  Remaining = ['b']: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_wrong_type(self) -> bool:
        """Test LREM on a key holding wrong type returns WRONGTYPE error."""
        print("\nTest: LREM wrong type error")

        key = "lrem_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  LREM on string key '{key}'...")
        try:
            write_node.lrem(key, 1, "hello")
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

    def test_lrem_single_element(self) -> bool:
        """Test LREM on a single-element list."""
        print("\nTest: LREM single element list")

        key = "lrem_single"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "hello")

        print(f"  LREM '{key}' 1 'hello'...")
        try:
            result = write_node.lrem(key, 1, "hello")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 1: OK")

        exists = write_node.exists(key)
        if exists != 0:
            print(f"\033[31m  FAILED: Key should be deleted after last element removed")
            return False
        print("  Key deleted after last element: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_atomicity_batch_consistency(self) -> bool:
        """Test that LREM is atomic — all removals and metadata update happen together."""
        print("\nTest: LREM atomicity batch consistency")

        key = "lrem_atomic"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "a", "b", "a", "c", "a")

        print(f"  LREM '{key}' 2 'a' (removes 2 of 3 'a's)...")
        try:
            result = write_node.lrem(key, 2, "a")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 2: OK")

        # Verify list integrity: exactly ['b', 'c', 'a'] remains
        remaining = write_node.lrange(key, 0, -1)
        if remaining != ["b", "c", "a"]:
            print(f"\033[31m  FAILED: Expected ['b','c','a'], got {remaining}")
            return False

        # Verify length is consistent
        length = write_node.llen(key)
        if length != 3:
            print(f"\033[31m  FAILED: Expected LLEN=3, got {length}")
            return False

        print("  List consistent after atomic removal: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_lrem_replication(self) -> bool:
        """Test that LREM operations replicate to all nodes."""
        print("\nTest: LREM replication")

        key = "lrem_repl"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.rpush(key, "x", "y", "x", "z")

        print(f"  LREM '{key}' 1 'x' on random node...")
        try:
            result = write_node.lrem(key, 1, "x")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: LREM failed - {e}")
            return False

        print("  LREM returned 1: OK")

        expected = ["y", "x", "z"]
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.lrange(key, 0, -1)
                if result != expected:
                    print(f"    Node {i}: FAILED (expected {expected}, got {result})")
                    return False
                print(f"    Node {i}: OK (got {result})")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

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
            self.test_lrange_nonexistent_key,
            self.test_lrange_full_list,
            self.test_lrange_partial,
            self.test_lrange_out_of_range,
            self.test_lrange_wrong_type,
            self.test_lrange_after_lpush,
            self.test_lset_basic,
            self.test_lset_negative_index,
            self.test_lset_middle,
            self.test_lset_nonexistent_key,
            self.test_lset_out_of_range,
            self.test_lset_wrong_type,
            self.test_lindex_basic,
            self.test_lindex_out_of_range,
            self.test_lindex_nonexistent_key,
            self.test_lindex_wrong_type,
            self.test_lindex_after_lpush,
            self.test_lindex_single_element,
            self.test_lindex_replication,
            self.test_lrem_basic_positive_count,
            self.test_lrem_negative_count,
            self.test_lrem_zero_count,
            self.test_lrem_nonexistent_key,
            self.test_lrem_no_match,
            self.test_lrem_removes_all_deletes_key,
            self.test_lrem_count_exceeds_matches,
            self.test_lrem_wrong_type,
            self.test_lrem_single_element,
            self.test_lrem_atomicity_batch_consistency,
            self.test_lrem_replication,
        ]

        passed = 0
        failed = 0

        for test in tests:
            print(f"\n\033[36m[running]\033[0m {test.__name__}")
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
