#!/usr/bin/env python3
"""
CoreDB Cluster ZSet Integration Tests

This test suite verifies ZSet commands (ZADD, etc.) against a running 3-node cluster.

Usage:
    pip install -r requirements.txt
    python test_cluster_zset.py
"""

import sys
import os
import signal

import redis

from cluster_manager import ClusterManager
from base_test import TestClusterBase


class TestClusterZSet(TestClusterBase):
    """ZSet command tests."""

    def test_zadd_single_member(self) -> bool:
        """Test ZADD with a single score-member pair."""
        print("\nTest: ZADD single member")

        key = "zadd_single"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  ZADD '{key}' 1.0 'member1'...")
        try:
            result = write_node.zadd(key, {"member1": 1.0})
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_multiple_members(self) -> bool:
        """Test ZADD with multiple score-member pairs."""
        print("\nTest: ZADD multiple members")

        key = "zadd_multi"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  ZADD '{key}' 1.0 a 2.0 b 3.0 c...")
        try:
            result = write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0})
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD returned 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_update_existing_score(self) -> bool:
        """Test ZADD updating an existing member's score returns 0."""
        print("\nTest: ZADD update existing member score")

        key = "zadd_update"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0})

        print(f"  ZADD '{key}' 10.0 'a' (update existing)...")
        try:
            result = write_node.zadd(key, {"a": 10.0})
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_mixed_new_and_existing(self) -> bool:
        """Test ZADD with mix of new and existing members."""
        print("\nTest: ZADD mixed new and existing")

        key = "zadd_mixed"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0})

        print(f"  ZADD '{key}' 10.0 a 3.0 c (a exists, c is new)...")
        try:
            result = write_node.zadd(key, {"a": 10.0, "c": 3.0})
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1 (only c is new), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_ch_flag(self) -> bool:
        """Test ZADD with CH flag returns changed count including updates."""
        print("\nTest: ZADD CH flag")

        key = "zadd_ch"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0})

        print(f"  ZADD '{key}' CH 10.0 a 3.0 c...")
        try:
            result = write_node.zadd(key, {"a": 10.0, "c": 3.0}, ch=True)
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2 (a updated + c added), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD CH returned 2: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_nx_flag(self) -> bool:
        """Test ZADD with NX flag only adds new members."""
        print("\nTest: ZADD NX flag")

        key = "zadd_nx"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0})

        print(f"  ZADD '{key}' NX 10.0 a 2.0 b...")
        try:
            result = write_node.zadd(key, {"a": 10.0, "b": 2.0}, nx=True)
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1 (only b added, a skipped), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        # Verify a was NOT updated by adding again (should return 0)
        try:
            result = write_node.zadd(key, {"a": 1.0, "b": 3.0}, nx=True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0 (both already exist), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD NX returned 1, then 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_xx_flag(self) -> bool:
        """Test ZADD with XX flag only updates existing members."""
        print("\nTest: ZADD XX flag")

        key = "zadd_xx"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0})

        print(f"  ZADD '{key}' XX 10.0 a 2.0 b...")
        try:
            result = write_node.zadd(key, {"a": 10.0, "b": 2.0}, xx=True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0 (b skipped, a updated doesn't count), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        # Verify a was updated: adding same score should return 0
        try:
            result = write_node.zadd(key, {"a": 10.0}, xx=True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0 (same score), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD XX returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_gt_flag(self) -> bool:
        """Test ZADD with GT flag only updates if new score is greater."""
        print("\nTest: ZADD GT flag")

        key = "zadd_gt"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 5.0})

        # GT with lower score: should not update
        print(f"  ZADD '{key}' GT 3.0 a (3.0 < 5.0, skip)...")
        try:
            result = write_node.zadd(key, {"a": 3.0}, gt=True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        # GT with higher score: should update (but return 0 without CH)
        print(f"  ZADD '{key}' GT 10.0 a (10.0 > 5.0, update)...")
        try:
            result = write_node.zadd(key, {"a": 10.0}, gt=True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0 (update doesn't count without CH), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        # GT with same score: should not update
        try:
            result = write_node.zadd(key, {"a": 10.0}, gt=True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  GT flag behavior correct: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_lt_flag(self) -> bool:
        """Test ZADD with LT flag only updates if new score is less."""
        print("\nTest: ZADD LT flag")

        key = "zadd_lt"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 5.0})

        # LT with higher score: should not update
        print(f"  ZADD '{key}' LT 10.0 a (10.0 > 5.0, skip)...")
        try:
            result = write_node.zadd(key, {"a": 10.0}, lt=True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        # LT with lower score: should update
        print(f"  ZADD '{key}' LT 2.0 a (2.0 < 5.0, update)...")
        try:
            result = write_node.zadd(key, {"a": 2.0}, lt=True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0 (update doesn't count without CH), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        # LT with same score: should not update
        try:
            result = write_node.zadd(key, {"a": 2.0}, lt=True)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  LT flag behavior correct: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_wrong_type(self) -> bool:
        """Test ZADD on a key holding wrong type."""
        print("\nTest: ZADD wrong type error")

        key = "zadd_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  ZADD on string key '{key}'...")
        try:
            write_node.zadd(key, {"member": 1.0})
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

    def test_zadd_negative_score(self) -> bool:
        """Test ZADD with negative scores."""
        print("\nTest: ZADD negative score")

        key = "zadd_neg"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  ZADD '{key}' -10.5 'member1'...")
        try:
            result = write_node.zadd(key, {"member1": -10.5})
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD negative score: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_replication(self) -> bool:
        """Test that ZADD data replicates to all nodes."""
        print("\nTest: ZADD replication")

        key = "zadd_repl"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  ZADD '{key}' 1.0 a 2.0 b 3.0 c on random node...")
        try:
            write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0})
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD from all nodes to verify replication (duplicates should return 0)...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0})
                if result != 0:
                    print(f"    Node {i}: FAILED (expected 0, got {result})")
                    return False
                print(f"    Node {i}: OK (zadd returned 0, all members exist)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_empty_member(self) -> bool:
        """Test ZADD with empty string member."""
        print("\nTest: ZADD empty member")

        key = "zadd_empty"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  ZADD '{key}' 1.0 ''...")
        try:
            result = write_node.zadd(key, {"": 1.0})
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD empty member returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_large_number_of_members(self) -> bool:
        """Test ZADD with a large number of members."""
        print("\nTest: ZADD large number of members")

        key = "zadd_large"
        write_node = self._get_random_node()

        write_node.delete(key)

        members = {f"member_{i}": float(i) for i in range(1000)}
        print(f"  ZADD '{key}' with 1000 members...")
        try:
            result = write_node.zadd(key, members)
            if result != 1000:
                print(f"\033[31m  FAILED: Expected 1000, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD returned 1000: OK")

        print("  ZADD again with same members (should return 0)...")
        try:
            result = write_node.zadd(key, members)
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_special_characters(self) -> bool:
        """Test ZADD with special characters in members."""
        print("\nTest: ZADD special characters")

        key = "zadd_special"
        write_node = self._get_random_node()

        write_node.delete(key)

        special_members = {"hello world": 1.0, "key:value": 2.0, "a\nb\tc": 3.0}
        print(f"  ZADD '{key}' with special characters...")
        try:
            result = write_node.zadd(key, special_members)
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD returned 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_zero_score(self) -> bool:
        """Test ZADD with zero score."""
        print("\nTest: ZADD zero score")

        key = "zadd_zero"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  ZADD '{key}' 0.0 'member1'...")
        try:
            result = write_node.zadd(key, {"member1": 0.0})
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD failed - {e}")
            return False

        print("  ZADD zero score: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zadd_nx_xx_conflict(self) -> bool:
        """Test ZADD with NX and XX flags returns error."""
        print("\nTest: ZADD NX XX conflict")

        key = "zadd_nx_xx"
        write_node = self._get_random_node()

        print(f"  ZADD '{key}' NX XX 1.0 member...")
        try:
            write_node.execute_command("ZADD", key, "NX", "XX", "1.0", "member")
            print(f"\033[31m  FAILED: Expected error")
            return False
        except redis.ResponseError as e:
            error_msg = str(e)
            if "not compatible" not in error_msg.lower():
                print(f"\033[31m  FAILED: Expected compatibility error, got: {e}")
                return False
            print(f"  Got expected error: OK")

        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrem_single_member(self) -> bool:
        """Test ZREM removes a single member."""
        print("\nTest: ZREM single member")

        key = "zrem_single"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0})

        print(f"  ZREM '{key}' 'a'...")
        try:
            result = write_node.zrem(key, "a")
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZREM failed - {e}")
            return False

        print("  ZREM returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrem_multiple_members(self) -> bool:
        """Test ZREM removes multiple members at once."""
        print("\nTest: ZREM multiple members")

        key = "zrem_multi"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})

        print(f"  ZREM '{key}' a b c...")
        try:
            result = write_node.zrem(key, "a", "b", "c")
            if result != 3:
                print(f"\033[31m  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZREM failed - {e}")
            return False

        print("  ZREM returned 3: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrem_nonexistent_member(self) -> bool:
        """Test ZREM with non-existent member returns 0."""
        print("\nTest: ZREM non-existent member")

        key = "zrem_nonexist"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0})

        print(f"  ZREM '{key}' 'nonexistent'...")
        try:
            result = write_node.zrem(key, "nonexistent")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZREM failed - {e}")
            return False

        print("  ZREM returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrem_nonexistent_key(self) -> bool:
        """Test ZREM on non-existent key returns 0."""
        print("\nTest: ZREM non-existent key")

        key = "zrem_nokey"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  ZREM '{key}' 'a'...")
        try:
            result = write_node.zrem(key, "a")
            if result != 0:
                print(f"\033[31m  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZREM failed - {e}")
            return False

        print("  ZREM returned 0: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrem_mixed_existing_and_nonexistent(self) -> bool:
        """Test ZREM with mix of existing and non-existing members."""
        print("\nTest: ZREM mixed existing and non-existing")

        key = "zrem_mixed"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0})

        print(f"  ZREM '{key}' a nonexistent b...")
        try:
            result = write_node.zrem(key, "a", "nonexistent", "b")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2 (a and b exist), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZREM failed - {e}")
            return False

        print("  ZREM returned 2: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrem_wrong_type(self) -> bool:
        """Test ZREM on a key holding wrong type."""
        print("\nTest: ZREM wrong type error")

        key = "zrem_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  ZREM on string key '{key}'...")
        try:
            write_node.zrem(key, "member")
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

    def test_zrem_replication(self) -> bool:
        """Test that ZREM replicates to all nodes."""
        print("\nTest: ZREM replication")

        key = "zrem_repl"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0})

        print(f"  ZREM '{key}' a b on random node...")
        try:
            result = write_node.zrem(key, "a", "b")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZREM failed - {e}")
            return False

        print("  ZADD c from all nodes to verify only c remains...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.zadd(key, {"c": 3.0})
                if result != 0:
                    print(f"    Node {i}: FAILED (expected 0, c already exists, got {result})")
                    return False
                print(f"    Node {i}: OK (c exists, a/b removed)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrem_all_members(self) -> bool:
        """Test ZREM removing all members empties the sorted set."""
        print("\nTest: ZREM all members")

        key = "zrem_all"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0})

        print(f"  ZREM '{key}' a b (all members)...")
        try:
            result = write_node.zrem(key, "a", "b")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZREM failed - {e}")
            return False

        print("  Verifying ZADD after removing all members...")
        try:
            result = write_node.zadd(key, {"c": 3.0})
            if result != 1:
                print(f"\033[31m  FAILED: Expected 1 (new member after empty), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD after ZREM all failed - {e}")
            return False

        print("  ZREM all + ZADD new returned 1: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrem_atomicity_batch_consistency(self) -> bool:
        """Test that ZREM is atomic - all or nothing."""
        print("\nTest: ZREM atomicity batch consistency")

        key = "zrem_atomic"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0})

        print(f"  ZREM '{key}' a nonexistent c...")
        try:
            result = write_node.zrem(key, "a", "nonexistent", "c")
            if result != 2:
                print(f"\033[31m  FAILED: Expected 2 (a and c exist), got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZREM failed - {e}")
            return False

        print("  Verifying b still exists...")
        try:
            result = write_node.zadd(key, {"b": 2.0})
            if result != 0:
                print(f"\033[31m  FAILED: b should still exist, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZADD check failed - {e}")
            return False

        print("  ZREM atomic: b preserved, a and c removed: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    # ==================== ZRANGE Tests ====================

    def test_zrange_basic(self) -> bool:
        """Test ZRANGE returns members in ascending score order."""
        print("\nTest: ZRANGE basic")

        key = "zrange_basic"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"c": 3.0, "a": 1.0, "b": 2.0})

        print(f"  ZRANGE '{key}' 0 -1...")
        try:
            result = write_node.zrange(key, 0, -1)
            if result != ["a", "b", "c"]:
                print(f"\033[31m  FAILED: Expected ['a', 'b', 'c'], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZRANGE failed - {e}")
            return False

        print("  ZRANGE returned ['a', 'b', 'c']: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrange_with_scores(self) -> bool:
        """Test ZRANGE WITHSCORES returns member-score pairs."""
        print("\nTest: ZRANGE WITHSCORES")

        key = "zrange_scores"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0})

        print(f"  ZRANGE '{key}' 0 -1 WITHSCORES...")
        try:
            result = write_node.zrange(key, 0, -1, withscores=True)
            expected = [("a", 1.0), ("b", 2.0), ("c", 3.0)]
            if result != expected:
                print(f"\033[31m  FAILED: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZRANGE failed - {e}")
            return False

        print("  ZRANGE WITHSCORES returned correct pairs: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrange_subset(self) -> bool:
        """Test ZRANGE with positive start and stop indices."""
        print("\nTest: ZRANGE subset")

        key = "zrange_subset"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})

        print(f"  ZRANGE '{key}' 1 3...")
        try:
            result = write_node.zrange(key, 1, 3)
            if result != ["b", "c", "d"]:
                print(f"\033[31m  FAILED: Expected ['b', 'c', 'd'], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZRANGE failed - {e}")
            return False

        print("  ZRANGE 1 3 returned ['b', 'c', 'd']: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrange_negative_indices(self) -> bool:
        """Test ZRANGE with negative start and stop indices."""
        print("\nTest: ZRANGE negative indices")

        key = "zrange_neg"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})

        print(f"  ZRANGE '{key}' -3 -1...")
        try:
            result = write_node.zrange(key, -3, -1)
            if result != ["b", "c", "d"]:
                print(f"\033[31m  FAILED: Expected ['b', 'c', 'd'], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZRANGE failed - {e}")
            return False

        print("  ZRANGE -3 -1 returned ['b', 'c', 'd']: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrange_start_greater_than_stop(self) -> bool:
        """Test ZRANGE returns empty when start > stop."""
        print("\nTest: ZRANGE start > stop")

        key = "zrange_empty"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0})

        print(f"  ZRANGE '{key}' 2 1...")
        try:
            result = write_node.zrange(key, 2, 1)
            if result != []:
                print(f"\033[31m  FAILED: Expected [], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZRANGE failed - {e}")
            return False

        print("  ZRANGE 2 1 returned []: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrange_out_of_range(self) -> bool:
        """Test ZRANGE with indices beyond the set size."""
        print("\nTest: ZRANGE out of range")

        key = "zrange_oor"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0})

        print(f"  ZRANGE '{key}' 0 100...")
        try:
            result = write_node.zrange(key, 0, 100)
            if result != ["a", "b"]:
                print(f"\033[31m  FAILED: Expected ['a', 'b'], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZRANGE failed - {e}")
            return False

        print("  ZRANGE 0 100 returned ['a', 'b']: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrange_nonexistent_key(self) -> bool:
        """Test ZRANGE on a non-existent key returns empty array."""
        print("\nTest: ZRANGE non-existent key")

        key = "zrange_noexist"
        write_node = self._get_random_node()

        write_node.delete(key)

        print(f"  ZRANGE '{key}' 0 -1...")
        try:
            result = write_node.zrange(key, 0, -1)
            if result != []:
                print(f"\033[31m  FAILED: Expected [], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZRANGE failed - {e}")
            return False

        print("  ZRANGE on non-existent key returned []: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrange_wrong_type(self) -> bool:
        """Test ZRANGE on a key holding wrong type."""
        print("\nTest: ZRANGE wrong type error")

        key = "zrange_wrong_type"
        write_node = self._get_random_node()

        write_node.set(key, "string_value")

        print(f"  ZRANGE on string key '{key}'...")
        try:
            write_node.zrange(key, 0, -1)
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

    def test_zrange_single_element(self) -> bool:
        """Test ZRANGE with a single element sorted set."""
        print("\nTest: ZRANGE single element")

        key = "zrange_single"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"only": 42.0})

        print(f"  ZRANGE '{key}' 0 -1...")
        try:
            result = write_node.zrange(key, 0, -1)
            if result != ["only"]:
                print(f"\033[31m  FAILED: Expected ['only'], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZRANGE failed - {e}")
            return False

        print("  ZRANGE returned ['only']: OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrange_equal_scores(self) -> bool:
        """Test ZRANGE with equal scores uses lexicographic member order."""
        print("\nTest: ZRANGE equal scores")

        key = "zrange_equal"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"c": 1.0, "a": 1.0, "b": 1.0})

        print(f"  ZRANGE '{key}' 0 -1...")
        try:
            result = write_node.zrange(key, 0, -1)
            if result != ["a", "b", "c"]:
                print(f"\033[31m  FAILED: Expected ['a', 'b', 'c'], got {result}")
                return False
        except redis.RedisError as e:
            print(f"\033[31m  FAILED: ZRANGE failed - {e}")
            return False

        print("  ZRANGE returned ['a', 'b', 'c'] (lexicographic): OK")
        print("\033[32m  PASSED\033[0m")
        return True

    def test_zrange_replication(self) -> bool:
        """Test that ZRANGE reads replicated data from all nodes."""
        print("\nTest: ZRANGE replication")

        key = "zrange_repl"
        write_node = self._get_random_node()

        write_node.delete(key)
        write_node.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0})

        print("  ZRANGE from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.zrange(key, 0, -1)
                if result != ["a", "b", "c"]:
                    print(f"    Node {i}: FAILED (expected ['a', 'b', 'c'], got {result})")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False

        print("\033[32m  PASSED\033[0m")
        return True

    def run_all_tests(self) -> bool:
        """Run all zset tests."""
        print("\n" + "=" * 50)
        print("Running ZSet Tests")
        print("=" * 50)

        if not self.setup():
            return False

        tests = [
            self.test_zadd_single_member,
            self.test_zadd_multiple_members,
            self.test_zadd_update_existing_score,
            self.test_zadd_mixed_new_and_existing,
            self.test_zadd_ch_flag,
            self.test_zadd_nx_flag,
            self.test_zadd_xx_flag,
            self.test_zadd_gt_flag,
            self.test_zadd_lt_flag,
            self.test_zadd_wrong_type,
            self.test_zadd_negative_score,
            self.test_zadd_replication,
            self.test_zadd_empty_member,
            self.test_zadd_large_number_of_members,
            self.test_zadd_special_characters,
            self.test_zadd_zero_score,
            self.test_zadd_nx_xx_conflict,
            self.test_zrem_single_member,
            self.test_zrem_multiple_members,
            self.test_zrem_nonexistent_member,
            self.test_zrem_nonexistent_key,
            self.test_zrem_mixed_existing_and_nonexistent,
            self.test_zrem_wrong_type,
            self.test_zrem_replication,
            self.test_zrem_all_members,
            self.test_zrem_atomicity_batch_consistency,
            self.test_zrange_basic,
            self.test_zrange_with_scores,
            self.test_zrange_subset,
            self.test_zrange_negative_indices,
            self.test_zrange_start_greater_than_stop,
            self.test_zrange_out_of_range,
            self.test_zrange_nonexistent_key,
            self.test_zrange_wrong_type,
            self.test_zrange_single_element,
            self.test_zrange_equal_scores,
            self.test_zrange_replication,
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
        print(f"ZSet Tests: {passed} passed, {failed} failed")
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

    cluster.clean()

    if not cluster.build():
        print("Build failed, exiting")
        sys.exit(1)

    if not cluster.start():
        print("Failed to start cluster, exiting")
        cluster.stop()
        sys.exit(1)

    try:
        tester = TestClusterZSet(cluster)
        success = tester.run_all_tests()

        if success:
            print("\n✅ All zset tests passed!")
        else:
            print("\n❌ Some zset tests failed!")

    finally:
        cluster.stop()
        cluster.clean()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    sys.exit(main())
