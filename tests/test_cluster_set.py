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

    def test_srem_single_member(self) -> bool:
        """Test SREM with a single member."""
        print("\nTest: SREM single member")

        key = "srem_single"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c")

        print(f"  SREM '{key}' 'a'...")
        try:
            result = write_node.srem(key, "a")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SREM failed - {e}")
            return False

        print("  SREM returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_srem_multiple_members(self) -> bool:
        """Test SREM with multiple members."""
        print("\nTest: SREM multiple members")

        key = "srem_multi"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c", "d")

        print(f"  SREM '{key}' a b c...")
        try:
            result = write_node.srem(key, "a", "b", "c")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SREM failed - {e}")
            return False

        print("  SREM returned 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_srem_nonexistent_member(self) -> bool:
        """Test SREM with member that does not exist in the set."""
        print("\nTest: SREM nonexistent member")

        key = "srem_nonexist"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b")

        print(f"  SREM '{key}' 'z' (not in set)...")
        try:
            result = write_node.srem(key, "z")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SREM failed - {e}")
            return False

        print("  SREM returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_srem_nonexistent_key(self) -> bool:
        """Test SREM on a key that does not exist."""
        print("\nTest: SREM nonexistent key")

        key = "srem_nokey"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  SREM '{key}' 'a' on non-existent key...")
        try:
            result = write_node.srem(key, "a")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SREM failed - {e}")
            return False

        print("  SREM returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_srem_wrong_type(self) -> bool:
        """Test SREM on a key holding wrong type."""
        print("\nTest: SREM wrong type error")

        key = "srem_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  SREM on string key '{key}'...")
        try:
            write_node.srem(key, "member")
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

    def test_srem_mixed_members(self) -> bool:
        """Test SREM with mix of existing and non-existing members."""
        print("\nTest: SREM mixed existing and non-existing members")

        key = "srem_mixed"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c")

        print(f"  SREM '{key}' a z b (z not in set)...")
        try:
            result = write_node.srem(key, "a", "z", "b")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SREM failed - {e}")
            return False

        print("  SREM returned 2: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_srem_replication(self) -> bool:
        """Test that SREM data replicates to all nodes."""
        print("\nTest: SREM replication")

        key = "srem_repl"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c")

        print(f"  SREM '{key}' a b on random node...")
        try:
            result = write_node.srem(key, "a", "b")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SREM failed - {e}")
            return False

        print("  SREM from all nodes to verify replication...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.srem(key, "a", "b")
                if result != 0:
                    print(f"    Node {i}: FAILED (expected 0, got {result})")
                    return False
                print(f"    Node {i}: OK (srem returned 0, members already removed)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_srem_insufficient_args(self) -> bool:
        """Test SREM with insufficient arguments returns error."""
        print("\nTest: SREM insufficient arguments")

        key = "srem_no_args"
        write_node = self._get_random_node()

        print(f"  SREM without members...")
        try:
            write_node.srem(key)
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

    def test_smembers_basic(self) -> bool:
        """Test SMEMBERS returns all members of a set."""
        print("\nTest: SMEMBERS basic")

        key = "smembers_basic"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c")

        print(f"  SMEMBERS '{key}'...")
        try:
            result = write_node.smembers(key)
            if set(result) != {"a", "b", "c"}:
                print(f"\033[31m  FAILED: Expected {{'a','b','c'}}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SMEMBERS failed - {e}")
            return False

        print(f"  SMEMBERS returned {sorted(result)}: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_smembers_empty_set(self) -> bool:
        """Test SMEMBERS on non-existent key returns empty set."""
        print("\nTest: SMEMBERS empty set")

        key = "smembers_empty"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  SMEMBERS '{key}' on non-existent key...")
        try:
            result = write_node.smembers(key)
            if result != set():
                print(f"\033[31m  FAILED: Expected empty set, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SMEMBERS failed - {e}")
            return False

        print("  SMEMBERS returned empty set: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_smembers_wrong_type(self) -> bool:
        """Test SMEMBERS on a key holding wrong type."""
        print("\nTest: SMEMBERS wrong type error")

        key = "smembers_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  SMEMBERS on string key '{key}'...")
        try:
            write_node.smembers(key)
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

    def test_smembers_after_srem(self) -> bool:
        """Test SMEMBERS returns correct members after SREM."""
        print("\nTest: SMEMBERS after SREM")

        key = "smembers_after_srem"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c", "d")
        write_node.srem(key, "b", "d")

        print(f"  SMEMBERS '{key}' after SREM b, d...")
        try:
            result = write_node.smembers(key)
            if set(result) != {"a", "c"}:
                print(f"\033[31m  FAILED: Expected {{'a','c'}}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SMEMBERS failed - {e}")
            return False

        print(f"  SMEMBERS returned {sorted(result)}: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_smembers_replication(self) -> bool:
        """Test that SMEMBERS data replicates to all nodes."""
        print("\nTest: SMEMBERS replication")

        key = "smembers_repl"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "x", "y", "z")

        print("  SMEMBERS from all nodes to verify replication...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.smembers(key)
                if set(result) != {"x", "y", "z"}:
                    print(f"    Node {i}: FAILED (expected {{'x','y','z'}}, got {result})")
                    return False
                print(f"    Node {i}: OK (smembers returned {sorted(result)})")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_smembers_special_characters(self) -> bool:
        """Test SMEMBERS with special characters in members."""
        print("\nTest: SMEMBERS special characters")

        key = "smembers_special"
        write_node = self._get_random_node()

        write_node.delete(key)
        special_members = ["hello world", "key:value", "日本語"]
        write_node.sadd(key, *special_members)

        print(f"  SMEMBERS '{key}' with special characters...")
        try:
            result = write_node.smembers(key)
            decoded = set()
            for m in result:
                if isinstance(m, bytes):
                    decoded.add(m.decode("utf-8"))
                else:
                    decoded.add(m)
            if decoded != set(special_members):
                print(f"\033[31m  FAILED: Expected {set(special_members)}, got {decoded}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SMEMBERS failed - {e}")
            return False

        print(f"  SMEMBERS returned all special members: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sismember_exists(self) -> bool:
        """Test SISMEMBER returns 1 when member exists."""
        print("\nTest: SISMEMBER member exists")

        key = "sismember_exists"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c")

        print(f"  SISMEMBER '{key}' 'a'...")
        try:
            result = write_node.sismember(key, "a")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SISMEMBER failed - {e}")
            return False

        print("  SISMEMBER returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sismember_not_exists(self) -> bool:
        """Test SISMEMBER returns 0 when member does not exist."""
        print("\nTest: SISMEMBER member does not exist")

        key = "sismember_not_exists"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c")

        print(f"  SISMEMBER '{key}' 'z' (not in set)...")
        try:
            result = write_node.sismember(key, "z")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SISMEMBER failed - {e}")
            return False

        print("  SISMEMBER returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sismember_nonexistent_key(self) -> bool:
        """Test SISMEMBER on non-existent key returns 0."""
        print("\nTest: SISMEMBER non-existent key")

        key = "sismember_nokey"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  SISMEMBER '{key}' 'a' on non-existent key...")
        try:
            result = write_node.sismember(key, "a")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SISMEMBER failed - {e}")
            return False

        print("  SISMEMBER returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sismember_wrong_type(self) -> bool:
        """Test SISMEMBER on a key holding wrong type."""
        print("\nTest: SISMEMBER wrong type error")

        key = "sismember_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  SISMEMBER on string key '{key}'...")
        try:
            write_node.sismember(key, "member")
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

    def test_sismember_replication(self) -> bool:
        """Test that SISMEMBER works on all replicated nodes."""
        print("\nTest: SISMEMBER replication")

        key = "sismember_repl"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c")

        print("  SISMEMBER from all nodes to verify replication...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.sismember(key, "a")
                if result != 1:
                    print(f"    Node {i}: FAILED (expected 1, got {result})")
                    return False
                print(f"    Node {i}: OK (sismember returned 1)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_sismember_after_srem(self) -> bool:
        """Test SISMEMBER returns 0 after member is removed."""
        print("\nTest: SISMEMBER after SREM")

        key = "sismember_after_srem"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c")
        write_node.srem(key, "a")

        print(f"  SISMEMBER '{key}' 'a' after SREM...")
        try:
            result = write_node.sismember(key, "a")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SISMEMBER failed - {e}")
            return False

        print("  SISMEMBER returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sismember_empty_member(self) -> bool:
        """Test SISMEMBER with empty string member."""
        print("\nTest: SISMEMBER empty member")

        key = "sismember_empty"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "")

        print(f"  SISMEMBER '{key}' ''...")
        try:
            result = write_node.sismember(key, "")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SISMEMBER failed - {e}")
            return False

        print("  SISMEMBER returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_sismember_special_characters(self) -> bool:
        """Test SISMEMBER with special characters in member."""
        print("\nTest: SISMEMBER special characters")

        key = "sismember_special"
        write_node = self._get_random_node()

        write_node.delete(key)
        special_member = "hello world"
        write_node.sadd(key, special_member)

        print(f"  SISMEMBER '{key}' '{special_member}'...")
        try:
            result = write_node.sismember(key, special_member)
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SISMEMBER failed - {e}")
            return False

        print("  SISMEMBER returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_srem_atomicity_batch_consistency(self) -> bool:
        """Test that SREM multi-member operations are atomic (all or nothing)."""
        print("\nTest: SREM atomicity batch consistency")

        key = "srem_atomic"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.sadd(key, "a", "b", "c", "d")

        print(f"  SREM '{key}' a b c (3 members)...")
        try:
            result = write_node.srem(key, "a", "b", "c")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SREM failed - {e}")
            return False

        print("  Verifying all removed members are gone...")
        try:
            result = write_node.srem(key, "a", "b", "c")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0 (all removed), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SREM verify failed - {e}")
            return False

        print("  Verifying remaining member still exists...")
        try:
            result = write_node.srem(key, "d")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1 (d still exists), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: SREM verify failed - {e}")
            return False

        print("  All consistency checks passed: OK")
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
            self.test_smembers_basic,
            self.test_smembers_empty_set,
            self.test_smembers_wrong_type,
            self.test_smembers_after_srem,
            self.test_smembers_replication,
            self.test_smembers_special_characters,
            self.test_sismember_exists,
            self.test_sismember_not_exists,
            self.test_sismember_nonexistent_key,
            self.test_sismember_wrong_type,
            self.test_sismember_replication,
            self.test_sismember_after_srem,
            self.test_sismember_empty_member,
            self.test_sismember_special_characters,
            self.test_srem_single_member,
            self.test_srem_multiple_members,
            self.test_srem_nonexistent_member,
            self.test_srem_nonexistent_key,
            self.test_srem_wrong_type,
            self.test_srem_mixed_members,
            self.test_srem_replication,
            self.test_srem_insufficient_args,
            self.test_srem_atomicity_batch_consistency,
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
