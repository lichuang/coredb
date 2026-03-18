#!/usr/bin/env python3
"""
CoreDB Cluster Hash Integration Tests

This test suite:
1. Starts a 3-node CoreDB cluster
2. Performs basic HSET/HGET operations
3. Verifies data replication across nodes
4. Tests multiple fields in a hash
5. Tests NX and XX options
6. Stops the cluster

Usage:
    pip install -r requirements.txt
    python test_cluster_hash.py
"""

import random
import sys
import os
import signal

import redis

from cluster_manager import ClusterManager
from base_test import TestClusterBase


class TestClusterHash(TestClusterBase):
    """Hash command tests."""
    
    def test_hset_and_hget(self) -> bool:
        """Test basic HSET and HGET operations."""
        print("\nTest: HSET and HGET")
        
        test_key = "test_hash"
        test_field = "field1"
        test_value = "value1"
        
        # HSET to a random node
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' '{test_field}' = '{test_value}' on a random node...")
        try:
            result = write_node.hset(test_key, test_field, test_value)
            if result != 1:
                print(f"  FAILED: Expected return 1 (new field), got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # HGET from all nodes
        print("  HGET from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.hget(test_key, test_field)
                if value == test_value:
                    print(f"    Node {i}: OK (got '{value}')")
                else:
                    print(f"    Node {i}: FAILED (expected '{test_value}', got '{value}')")
                    return False
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_hset_multiple_fields(self) -> bool:
        """Test HSET with multiple fields in one command."""
        print("\nTest: HSET Multiple Fields (Single Command)")
        
        test_key = "test_hash_multi_single"
        # Use mapping parameter for multiple fields in one HSET call
        fields = {
            "name": "John",
            "age": "30",
            "city": "New York"
        }
        
        # HSET multiple fields using mapping
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' with multiple fields in one command...")
        try:
            result = write_node.hset(test_key, mapping=fields)
            # Should return 3 (number of new fields added)
            if result != 3:
                print(f"  FAILED: Expected return 3 (3 new fields), got {result}")
                return False
            print(f"    HSET returned {result} (new fields)")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # HGET all fields from all nodes
        print("  HGET all fields from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                for field, expected_value in fields.items():
                    value = node.conn.hget(test_key, field)
                    if value != expected_value:
                        print(f"    Node {i}, field '{field}': FAILED (expected '{expected_value}', got '{value}')")
                        return False
                print(f"    Node {i}: OK (all fields match)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_hset_multiple_fields_mixed(self) -> bool:
        """Test HSET with multiple fields where some exist and some don't."""
        print("\nTest: HSET Multiple Fields Mixed (Existing + New)")
        
        test_key = "test_hash_multi_mixed"
        
        # First, set one field
        write_node = self._get_random_node()
        write_node.hset(test_key, "existing_field", "existing_value")
        
        # Now set multiple fields including the existing one
        # mapping = {"existing_field": "updated_value", "new_field1": "new1", "new_field2": "new2"}
        # redis-py doesn't support NX/XX with mapping, so we use individual calls for this test
        # But we can test the return value: should return 2 (2 new fields)
        
        print(f"  HSET '{test_key}' with existing + new fields...")
        try:
            # First update the existing field
            result = write_node.hset(test_key, "existing_field", "updated_value")
            if result != 0:
                print(f"  FAILED: Expected return 0 for existing field update, got {result}")
                return False
            
            # Then add new fields one by one to test return values
            result = write_node.hset(test_key, "new_field1", "new1")
            if result != 1:
                print(f"  FAILED: Expected return 1 for new field, got {result}")
                return False
            
            result = write_node.hset(test_key, "new_field2", "new2")
            if result != 1:
                print(f"  FAILED: Expected return 1 for new field, got {result}")
                return False
            
            print(f"    Individual HSET calls succeeded with correct return values")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # Verify all values
        print("  Verify all values from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                assert node.conn.hget(test_key, "existing_field") == "updated_value"
                assert node.conn.hget(test_key, "new_field1") == "new1"
                assert node.conn.hget(test_key, "new_field2") == "new2"
                print(f"    Node {i}: OK")
            except (redis.RedisError, AssertionError) as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True

    def test_hset_atomicity_batch_consistency(self) -> bool:
        """Test HSET batch write atomicity - all fields written together.
        
        This test verifies that when HSET is called with multiple fields,
        either all fields are written or none are (atomicity guarantee).
        Since we cannot easily simulate failures, we verify consistency:
        - After HSET, all fields should be readable from all nodes
        - Metadata (HLEN) should be consistent with actual field count
        """
        print("\nTest: HSET Batch Atomicity - Consistency Check")
        
        test_key = "test_hash_atomicity_batch"
        # Use many fields to increase chance of catching any inconsistency
        fields = {f"field_{i}": f"value_{i}" for i in range(50)}
        
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' with {len(fields)} fields atomically...")
        
        try:
            result = write_node.hset(test_key, mapping=fields)
            if result != len(fields):
                print(f"  FAILED: Expected return {len(fields)}, got {result}")
                return False
            print(f"    HSET returned {result}")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # Verify from all nodes: check HLEN and all fields
        print("  Verify atomicity from all nodes (HLEN consistency)...")
        for i, node in enumerate(self.nodes, 1):
            try:
                # Check HLEN matches expected
                hlen = node.conn.hlen(test_key)
                if hlen != len(fields):
                    print(f"    Node {i}: FAILED (HLEN expected {len(fields)}, got {hlen})")
                    return False
                
                # Check all fields exist (verifies batch was atomic)
                for field, expected_value in fields.items():
                    actual_value = node.conn.hget(test_key, field)
                    if actual_value != expected_value:
                        print(f"    Node {i}: FAILED (field '{field}' expected '{expected_value}', got '{actual_value}')")
                        return False
                
                print(f"    Node {i}: OK (HLEN={hlen}, all {len(fields)} fields verified)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True

    def test_hdel_atomicity_batch_consistency(self) -> bool:
        """Test HDEL batch write atomicity - all fields deleted together.
        
        This test verifies that when HDEL is called with multiple fields,
        either all specified fields are deleted or none are (atomicity guarantee).
        We verify consistency: after HDEL, metadata should match actual field count.
        """
        print("\nTest: HDEL Batch Atomicity - Consistency Check")
        
        test_key = "test_hash_hdel_atomicity"
        # Create hash with many fields
        all_fields = {f"field_{i}": f"value_{i}" for i in range(30)}
        fields_to_delete = [f"field_{i}" for i in range(0, 20, 2)]  # 10 fields to delete
        remaining_fields = {k: v for k, v in all_fields.items() if k not in fields_to_delete}
        
        write_node = self._get_random_node()
        print(f"  Setup: HSET '{test_key}' with {len(all_fields)} fields...")
        
        try:
            result = write_node.hset(test_key, mapping=all_fields)
            if result != len(all_fields):
                print(f"  FAILED: Setup HSET expected {len(all_fields)}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: Setup HSET failed - {e}")
            return False
        
        # Record state before HDEL
        hlen_before = write_node.hlen(test_key)
        print(f"    HLEN before HDEL: {hlen_before}")
        
        # Delete multiple fields atomically
        print(f"  HDEL {len(fields_to_delete)} fields atomically...")
        try:
            result = write_node.hdel(test_key, *fields_to_delete)
            if result != len(fields_to_delete):
                print(f"  FAILED: HDEL expected {len(fields_to_delete)}, got {result}")
                return False
            print(f"    HDEL returned {result}")
        except redis.RedisError as e:
            print(f"  FAILED: HDEL failed - {e}")
            return False
        
        # Verify from all nodes: check consistency
        print("  Verify atomicity from all nodes (consistency check)...")
        for i, node in enumerate(self.nodes, 1):
            try:
                # Check HLEN is correct
                hlen = node.conn.hlen(test_key)
                expected_len = len(remaining_fields)
                if hlen != expected_len:
                    print(f"    Node {i}: FAILED (HLEN expected {expected_len}, got {hlen})")
                    return False
                
                # Verify deleted fields are gone
                for field in fields_to_delete:
                    if node.conn.hexists(test_key, field):
                        print(f"    Node {i}: FAILED (deleted field '{field}' still exists)")
                        return False
                
                # Verify remaining fields still exist
                for field, expected_value in remaining_fields.items():
                    actual_value = node.conn.hget(test_key, field)
                    if actual_value != expected_value:
                        print(f"    Node {i}: FAILED (field '{field}' expected '{expected_value}', got '{actual_value}')")
                        return False
                
                print(f"    Node {i}: OK (HLEN={hlen}, {len(fields_to_delete)} deleted, {len(remaining_fields)} remain)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True

    def test_hsetnx_atomicity_field_creation(self) -> bool:
        """Test HSETNX atomicity - field and metadata updated together.
        
        HSETNX should atomically check field existence and create it with metadata update.
        If field exists, nothing should change. If not, both field and metadata updated.
        """
        print("\nTest: HSETNX Atomicity - Field Creation Consistency")
        
        test_key = "test_hash_hsetnx_atomicity"
        
        write_node = self._get_random_node()
        
        # First HSETNX should create field and update metadata
        print("  First HSETNX (field does not exist)...")
        try:
            result = write_node.hsetnx(test_key, "field1", "value1")
            if result != 1:
                print(f"  FAILED: First HSETNX expected 1, got {result}")
                return False
            
            hlen = write_node.hlen(test_key)
            if hlen != 1:
                print(f"  FAILED: HLEN expected 1 after first HSETNX, got {hlen}")
                return False
            print(f"    OK: HSETNX returned 1, HLEN={hlen}")
        except redis.RedisError as e:
            print(f"  FAILED: First HSETNX failed - {e}")
            return False
        
        # Second HSETNX on same field should not change anything
        print("  Second HSETNX on same field (should not change)...")
        try:
            result = write_node.hsetnx(test_key, "field1", "new_value")
            if result != 0:
                print(f"  FAILED: Second HSETNX expected 0, got {result}")
                return False
            
            # Verify value not changed
            value = write_node.hget(test_key, "field1")
            if value != "value1":
                print(f"  FAILED: Value was changed to '{value}', expected 'value1'")
                return False
            
            # Verify HLEN not changed
            hlen = write_node.hlen(test_key)
            if hlen != 1:
                print(f"  FAILED: HLEN changed to {hlen}, expected 1")
                return False
            
            print(f"    OK: HSETNX returned 0, value unchanged, HLEN={hlen}")
        except redis.RedisError as e:
            print(f"  FAILED: Second HSETNX failed - {e}")
            return False
        
        # Verify from all nodes
        print("  Verify consistency from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.hget(test_key, "field1")
                hlen = node.conn.hlen(test_key)
                if value != "value1" or hlen != 1:
                    print(f"    Node {i}: FAILED (value='{value}', HLEN={hlen})")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_hset_update_existing(self) -> bool:
        """Test HSET updating existing field returns 0."""
        print("\nTest: HSET Update Existing Field")
        
        test_key = "test_hash_update"
        test_field = "field1"
        initial_value = "initial"
        updated_value = "updated"
        
        # First HSET (new field)
        write_node = self._get_random_node()
        print(f"  First HSET '{test_key}' '{test_field}' = '{initial_value}'...")
        try:
            result = write_node.hset(test_key, test_field, initial_value)
            if result != 1:
                print(f"  FAILED: Expected return 1 for new field, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # Second HSET (update existing field)
        print(f"  Update HSET '{test_key}' '{test_field}' = '{updated_value}'...")
        try:
            result = write_node.hset(test_key, test_field, updated_value)
            if result != 0:
                print(f"  FAILED: Expected return 0 for existing field, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # Verify updated value from all nodes
        print("  Verify updated value from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.hget(test_key, test_field)
                if value == updated_value:
                    print(f"    Node {i}: OK (got '{value}')")
                else:
                    print(f"    Node {i}: FAILED (expected '{updated_value}', got '{value}')")
                    return False
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_hget_nonexistent(self) -> bool:
        """Test HGET on non-existent key or field returns nil."""
        print("\nTest: HGET Non-existent Key/Field")
        
        # Test non-existent key
        print("  HGET non-existent key...")
        node = self._get_random_node()
        try:
            value = node.hget("nonexistent_key", "field")
            if value is not None:
                print(f"  FAILED: Expected nil for non-existent key, got '{value}'")
                return False
            print("    Non-existent key: OK (returned nil)")
        except redis.RedisError as e:
            print(f"  FAILED: HGET failed - {e}")
            return False
        
        # Test non-existent field on existing hash
        print("  HGET non-existent field on existing hash...")
        test_key = "test_hash_exist"
        test_field = "existing_field"
        test_value = "value"
        
        try:
            node.hset(test_key, test_field, test_value)
            value = node.hget(test_key, "nonexistent_field")
            if value is not None:
                print(f"  FAILED: Expected nil for non-existent field, got '{value}'")
                return False
            print("    Non-existent field: OK (returned nil)")
        except redis.RedisError as e:
            print(f"  FAILED: HGET failed - {e}")
            return False
        
        print("  PASSED")
        return True
    
    def test_hgetall_basic(self) -> bool:
        """Test basic HGETALL operation."""
        print("\nTest: HGETALL Basic")
        
        test_key = "test_hash_hgetall"
        fields = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Set multiple fields using HSET
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' with multiple fields...")
        try:
            result = write_node.hset(test_key, mapping=fields)
            if result != 3:
                print(f"  FAILED: Expected return 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # HGETALL from all nodes
        print("  HGETALL from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.hgetall(test_key)
                # redis-py returns a dict
                if not isinstance(result, dict):
                    print(f"    Node {i}: FAILED (expected dict, got {type(result).__name__})")
                    return False
                
                # Check all fields are present
                for field, expected_value in fields.items():
                    if field not in result:
                        print(f"    Node {i}: FAILED (field '{field}' missing)")
                        return False
                    if result[field] != expected_value:
                        print(f"    Node {i}: FAILED (field '{field}' expected '{expected_value}', got '{result[field]}')")
                        return False
                
                # Check no extra fields
                if len(result) != len(fields):
                    print(f"    Node {i}: FAILED (expected {len(fields)} fields, got {len(result)})")
                    return False
                
                print(f"    Node {i}: OK (all {len(fields)} fields match)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_hgetall_empty_hash(self) -> bool:
        """Test HGETALL on non-existent key returns empty dict."""
        print("\nTest: HGETALL Non-existent Key")
        
        test_key = "nonexistent_hash_for_hgetall"
        
        node = self._get_random_node()
        try:
            result = node.hgetall(test_key)
            if result != {}:
                print(f"  FAILED: Expected empty dict, got {result}")
                return False
            print("  HGETALL on non-existent key returned empty dict: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HGETALL failed - {e}")
            return False
        
        print("  PASSED")
        return True
    
    def test_hgetall_after_hdel(self) -> bool:
        """Test HGETALL after deleting some fields."""
        print("\nTest: HGETALL After HDEL")
        
        test_key = "test_hash_hgetall_after_hdel"
        fields = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Set multiple fields
        write_node = self._get_random_node()
        write_node.hset(test_key, mapping=fields)
        
        # Delete one field
        print("  HDEL field2...")
        try:
            result = write_node.hdel(test_key, "field2")
            if result != 1:
                print(f"  FAILED: HDEL expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HDEL failed - {e}")
            return False
        
        # HGETALL should return remaining fields
        print("  HGETALL after HDEL...")
        try:
            result = write_node.hgetall(test_key)
            expected = {"field1": "value1", "field3": "value3"}
            if result != expected:
                print(f"  FAILED: Expected {expected}, got {result}")
                return False
            print(f"  HGETALL returned {len(result)} fields: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HGETALL failed - {e}")
            return False
        
        print("  PASSED")
        return True
    
    def test_hgetall_after_hset_update(self) -> bool:
        """Test HGETALL returns updated values after HSET update."""
        print("\nTest: HGETALL After HSET Update")
        
        test_key = "test_hash_hgetall_update"
        
        # Set initial fields
        write_node = self._get_random_node()
        write_node.hset(test_key, "field1", "initial_value")
        write_node.hset(test_key, "field2", "value2")
        
        # Update one field
        print("  Update field1 with new value...")
        write_node.hset(test_key, "field1", "updated_value")
        
        # HGETALL should return updated value
        print("  HGETALL after update...")
        try:
            result = write_node.hgetall(test_key)
            if result.get("field1") != "updated_value":
                print(f"  FAILED: field1 expected 'updated_value', got '{result.get('field1')}'")
                return False
            if result.get("field2") != "value2":
                print(f"  FAILED: field2 expected 'value2', got '{result.get('field2')}'")
                return False
            print("  HGETALL returned updated values: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HGETALL failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hkeys_basic(self) -> bool:
        """Test basic HKEYS operation."""
        print("\nTest: HKEYS Basic")
        
        test_key = "test_hash_hkeys"
        fields = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Set multiple fields using HSET
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' with multiple fields...")
        try:
            result = write_node.hset(test_key, mapping=fields)
            if result != 3:
                print(f"  FAILED: Expected return 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # HKEYS from all nodes
        print("  HKEYS from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.hkeys(test_key)
                # redis-py returns a list
                if not isinstance(result, list):
                    print(f"    Node {i}: FAILED (expected list, got {type(result).__name__})")
                    return False
                
                # Check all fields are present (order may vary)
                result_set = set(result)
                expected_set = set(fields.keys())
                if result_set != expected_set:
                    print(f"    Node {i}: FAILED (expected {expected_set}, got {result_set})")
                    return False
                
                # Check no extra fields
                if len(result) != len(fields):
                    print(f"    Node {i}: FAILED (expected {len(fields)} fields, got {len(result)})")
                    return False
                
                print(f"    Node {i}: OK (all {len(fields)} fields match)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True

    def test_hkeys_empty_hash(self) -> bool:
        """Test HKEYS on non-existent key returns empty list."""
        print("\nTest: HKEYS Non-existent Key")
        
        test_key = "nonexistent_hash_for_hkeys"
        
        node = self._get_random_node()
        try:
            result = node.hkeys(test_key)
            if result != []:
                print(f"  FAILED: Expected empty list, got {result}")
                return False
            print("  HKEYS on non-existent key returned empty list: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HKEYS failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hkeys_after_hdel(self) -> bool:
        """Test HKEYS after deleting some fields."""
        print("\nTest: HKEYS After HDEL")
        
        test_key = "test_hash_hkeys_after_hdel"
        fields = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Set multiple fields
        write_node = self._get_random_node()
        write_node.hset(test_key, mapping=fields)
        
        # Delete one field
        print("  HDEL field2...")
        try:
            result = write_node.hdel(test_key, "field2")
            if result != 1:
                print(f"  FAILED: HDEL expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HDEL failed - {e}")
            return False
        
        # HKEYS should return remaining fields
        print("  HKEYS after HDEL...")
        try:
            result = write_node.hkeys(test_key)
            expected = {"field1", "field3"}
            result_set = set(result)
            if result_set != expected:
                print(f"  FAILED: Expected {expected}, got {result_set}")
                return False
            print(f"  HKEYS returned {len(result)} fields: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HKEYS failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hkeys_after_hset_update(self) -> bool:
        """Test HKEYS returns same fields after HSET update."""
        print("\nTest: HKEYS After HSET Update")
        
        test_key = "test_hash_hkeys_update"
        
        # Set initial fields
        write_node = self._get_random_node()
        write_node.hset(test_key, "field1", "initial_value")
        write_node.hset(test_key, "field2", "value2")
        
        # Update one field
        print("  Update field1 with new value...")
        write_node.hset(test_key, "field1", "updated_value")
        
        # HKEYS should return same fields
        print("  HKEYS after update...")
        try:
            result = write_node.hkeys(test_key)
            result_set = set(result)
            expected_set = {"field1", "field2"}
            if result_set != expected_set:
                print(f"  FAILED: expected {expected_set}, got {result_set}")
                return False
            print("  HKEYS returned correct fields: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HKEYS failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hlen_basic(self) -> bool:
        """Test basic HLEN operation."""
        print("\nTest: HLEN Basic")
        
        test_key = "test_hash_hlen"
        fields = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Set multiple fields using HSET
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' with {len(fields)} fields...")
        try:
            result = write_node.hset(test_key, mapping=fields)
            if result != 3:
                print(f"  FAILED: Expected return 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # HLEN from all nodes
        print("  HLEN from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.hlen(test_key)
                if result != len(fields):
                    print(f"    Node {i}: FAILED (expected {len(fields)}, got {result})")
                    return False
                print(f"    Node {i}: OK (got {result})")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True

    def test_hlen_empty_hash(self) -> bool:
        """Test HLEN on non-existent key returns 0."""
        print("\nTest: HLEN Non-existent Key")
        
        test_key = "nonexistent_hash_for_hlen"
        
        node = self._get_random_node()
        try:
            result = node.hlen(test_key)
            if result != 0:
                print(f"  FAILED: Expected 0, got {result}")
                return False
            print("  HLEN on non-existent key returned 0: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HLEN failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hlen_after_hdel(self) -> bool:
        """Test HLEN after deleting some fields."""
        print("\nTest: HLEN After HDEL")
        
        test_key = "test_hash_hlen_after_hdel"
        fields = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Set multiple fields
        write_node = self._get_random_node()
        write_node.hset(test_key, mapping=fields)
        
        # Delete one field
        print("  HDEL field2...")
        try:
            result = write_node.hdel(test_key, "field2")
            if result != 1:
                print(f"  FAILED: HDEL expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HDEL failed - {e}")
            return False
        
        # HLEN should return 2
        print("  HLEN after HDEL...")
        try:
            result = write_node.hlen(test_key)
            if result != 2:
                print(f"  FAILED: Expected 2, got {result}")
                return False
            print(f"  HLEN returned {result}: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HLEN failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hlen_after_hset_update(self) -> bool:
        """Test HLEN returns same count after HSET update."""
        print("\nTest: HLEN After HSET Update")
        
        test_key = "test_hash_hlen_update"
        
        # Set initial fields
        write_node = self._get_random_node()
        write_node.hset(test_key, "field1", "initial_value")
        write_node.hset(test_key, "field2", "value2")
        
        # Update one field (should not change count)
        print("  Update field1 with new value...")
        write_node.hset(test_key, "field1", "updated_value")
        
        # HLEN should still return 2
        print("  HLEN after update...")
        try:
            result = write_node.hlen(test_key)
            if result != 2:
                print(f"  FAILED: expected 2, got {result}")
                return False
            print("  HLEN returned correct count: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HLEN failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hmget_basic(self) -> bool:
        """Test basic HMGET operation with multiple fields."""
        print("\nTest: HMGET Basic")
        
        test_key = "test_hash_hmget"
        fields = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Set multiple fields using HSET
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' with multiple fields...")
        try:
            result = write_node.hset(test_key, mapping=fields)
            if result != 3:
                print(f"  FAILED: Expected return 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # HMGET from all nodes
        print("  HMGET field1, field2, field3 from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.hmget(test_key, ["field1", "field2", "field3"])
                # redis-py returns a list
                if not isinstance(result, list):
                    print(f"    Node {i}: FAILED (expected list, got {type(result).__name__})")
                    return False
                
                # Check all values are correct
                expected = ["value1", "value2", "value3"]
                if result != expected:
                    print(f"    Node {i}: FAILED (expected {expected}, got {result})")
                    return False
                
                print(f"    Node {i}: OK (all values match)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True

    def test_hmget_partial_fields(self) -> bool:
        """Test HMGET with some non-existent fields."""
        print("\nTest: HMGET Partial Fields")
        
        test_key = "test_hash_hmget_partial"
        
        # Set only some fields
        write_node = self._get_random_node()
        write_node.hset(test_key, "field1", "value1")
        write_node.hset(test_key, "field3", "value3")
        
        # HMGET including non-existent field2
        print("  HMGET field1, field2, field3 (field2 does not exist)...")
        try:
            result = write_node.hmget(test_key, ["field1", "field2", "field3"])
            expected = ["value1", None, "value3"]
            if result != expected:
                print(f"  FAILED: Expected {expected}, got {result}")
                return False
            print(f"  OK: Got {result}")
        except redis.RedisError as e:
            print(f"  FAILED: HMGET failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hmget_nonexistent_key(self) -> bool:
        """Test HMGET on non-existent key returns array of nils."""
        print("\nTest: HMGET Non-existent Key")
        
        test_key = "nonexistent_hash_for_hmget"
        
        node = self._get_random_node()
        try:
            result = node.hmget(test_key, ["field1", "field2", "field3"])
            expected = [None, None, None]
            if result != expected:
                print(f"  FAILED: Expected {expected}, got {result}")
                return False
            print("  HMGET on non-existent key returned [nil, nil, nil]: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HMGET failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hmget_single_field(self) -> bool:
        """Test HMGET with single field (should work like HGET)."""
        print("\nTest: HMGET Single Field")
        
        test_key = "test_hash_hmget_single"
        
        write_node = self._get_random_node()
        write_node.hset(test_key, "field1", "value1")
        
        print("  HMGET field1 (single field)...")
        try:
            result = write_node.hmget(test_key, ["field1"])
            expected = ["value1"]
            if result != expected:
                print(f"  FAILED: Expected {expected}, got {result}")
                return False
            print(f"  OK: Got {result}")
        except redis.RedisError as e:
            print(f"  FAILED: HMGET failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hexists_basic(self) -> bool:
        """Test basic HEXISTS operation."""
        print("\nTest: HEXISTS Basic")
        
        test_key = "test_hash_hexists"
        test_field = "field1"
        test_value = "value1"
        
        # Set a field
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' '{test_field}' = '{test_value}'...")
        try:
            result = write_node.hset(test_key, test_field, test_value)
            if result != 1:
                print(f"  FAILED: Expected return 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # HEXISTS should return 1 for existing field
        print("  HEXISTS from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.hexists(test_key, test_field)
                if result != 1:
                    print(f"    Node {i}: FAILED (expected 1, got {result})")
                    return False
                print(f"    Node {i}: OK (field exists)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True

    def test_hexists_nonexistent_field(self) -> bool:
        """Test HEXISTS on non-existent field returns 0."""
        print("\nTest: HEXISTS Non-existent Field")
        
        test_key = "test_hash_hexists_no_field"
        
        # Create a hash with one field
        write_node = self._get_random_node()
        write_node.hset(test_key, "existing_field", "value")
        
        # HEXISTS on non-existent field should return 0
        print("  HEXISTS on non-existent field...")
        try:
            result = write_node.hexists(test_key, "nonexistent_field")
            if result != 0:
                print(f"  FAILED: Expected 0, got {result}")
                return False
            print("  OK: HEXISTS returned 0 for non-existent field")
        except redis.RedisError as e:
            print(f"  FAILED: HEXISTS failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hexists_nonexistent_key(self) -> bool:
        """Test HEXISTS on non-existent key returns 0."""
        print("\nTest: HEXISTS Non-existent Key")
        
        test_key = "nonexistent_hash_for_hexists"
        
        node = self._get_random_node()
        try:
            result = node.hexists(test_key, "field1")
            if result != 0:
                print(f"  FAILED: Expected 0, got {result}")
                return False
            print("  OK: HEXISTS returned 0 for non-existent key")
        except redis.RedisError as e:
            print(f"  FAILED: HEXISTS failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hexists_after_hdel(self) -> bool:
        """Test HEXISTS returns 0 after field is deleted."""
        print("\nTest: HEXISTS After HDEL")
        
        test_key = "test_hash_hexists_after_hdel"
        test_field = "field_to_delete"
        
        # Set a field
        write_node = self._get_random_node()
        write_node.hset(test_key, test_field, "value")
        
        # Verify field exists
        if write_node.hexists(test_key, test_field) != 1:
            print("  FAILED: Field should exist before HDEL")
            return False
        
        # Delete the field
        print("  HDEL the field...")
        try:
            result = write_node.hdel(test_key, test_field)
            if result != 1:
                print(f"  FAILED: HDEL expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HDEL failed - {e}")
            return False
        
        # HEXISTS should now return 0
        print("  HEXISTS after HDEL...")
        try:
            result = write_node.hexists(test_key, test_field)
            if result != 0:
                print(f"  FAILED: Expected 0 after HDEL, got {result}")
                return False
            print("  OK: HEXISTS returned 0 after HDEL")
        except redis.RedisError as e:
            print(f"  FAILED: HEXISTS failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hexists_after_hset_update(self) -> bool:
        """Test HEXISTS still returns 1 after field is updated."""
        print("\nTest: HEXISTS After HSET Update")
        
        test_key = "test_hash_hexists_update"
        test_field = "field1"
        
        # Set initial value
        write_node = self._get_random_node()
        write_node.hset(test_key, test_field, "initial_value")
        
        # Update the field
        print("  Update field with new value...")
        write_node.hset(test_key, test_field, "updated_value")
        
        # HEXISTS should still return 1
        print("  HEXISTS after update...")
        try:
            result = write_node.hexists(test_key, test_field)
            if result != 1:
                print(f"  FAILED: Expected 1, got {result}")
                return False
            print("  OK: HEXISTS returned 1 after update")
        except redis.RedisError as e:
            print(f"  FAILED: HEXISTS failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hsetnx_new_field(self) -> bool:
        """Test HSETNX on new field sets the value and returns 1."""
        print("\nTest: HSETNX New Field")
        
        test_key = "test_hash_hsetnx_new"
        test_field = "field1"
        test_value = "value1"
        
        write_node = self._get_random_node()
        print(f"  HSETNX '{test_key}' '{test_field}' = '{test_value}'...")
        try:
            result = write_node.hsetnx(test_key, test_field, test_value)
            if result != 1:
                print(f"  FAILED: Expected return 1 (field set), got {result}")
                return False
            print(f"  OK: HSETNX returned 1")
        except redis.RedisError as e:
            print(f"  FAILED: HSETNX failed - {e}")
            return False
        
        # Verify the field was set
        value = write_node.hget(test_key, test_field)
        if value != test_value:
            print(f"  FAILED: Field not set correctly, expected '{test_value}', got '{value}'")
            return False
        
        print("  PASSED")
        return True

    def test_hsetnx_existing_field(self) -> bool:
        """Test HSETNX on existing field does not change value and returns 0."""
        print("\nTest: HSETNX Existing Field")
        
        test_key = "test_hash_hsetnx_existing"
        test_field = "field1"
        initial_value = "initial_value"
        new_value = "new_value"
        
        write_node = self._get_random_node()
        
        # First set the field
        print(f"  HSET '{test_key}' '{test_field}' = '{initial_value}'...")
        write_node.hset(test_key, test_field, initial_value)
        
        # Try HSETNX on existing field
        print(f"  HSETNX on existing field with '{new_value}'...")
        try:
            result = write_node.hsetnx(test_key, test_field, new_value)
            if result != 0:
                print(f"  FAILED: Expected return 0 (field exists), got {result}")
                return False
            print(f"  OK: HSETNX returned 0")
        except redis.RedisError as e:
            print(f"  FAILED: HSETNX failed - {e}")
            return False
        
        # Verify the field was NOT changed
        value = write_node.hget(test_key, test_field)
        if value != initial_value:
            print(f"  FAILED: Value was changed despite HSETNX! Expected '{initial_value}', got '{value}'")
            return False
        
        print("  PASSED")
        return True

    def test_hsetnx_replication(self) -> bool:
        """Test HSETNX replicates to all nodes."""
        print("\nTest: HSETNX Replication")
        
        test_key = "test_hash_hsetnx_repl"
        test_field = "field1"
        test_value = "value1"
        
        write_node = self._get_random_node()
        print(f"  HSETNX '{test_key}' '{test_field}' = '{test_value}' on random node...")
        try:
            result = write_node.hsetnx(test_key, test_field, test_value)
            if result != 1:
                print(f"  FAILED: Expected return 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSETNX failed - {e}")
            return False
        
        # Verify from all nodes
        print("  Verify HGET from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.hget(test_key, test_field)
                if value == test_value:
                    print(f"    Node {i}: OK (got '{value}')")
                else:
                    print(f"    Node {i}: FAILED (expected '{test_value}', got '{value}')")
                    return False
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True

    def test_hsetnx_empty_value(self) -> bool:
        """Test HSETNX with empty value."""
        print("\nTest: HSETNX Empty Value")
        
        test_key = "test_hash_hsetnx_empty"
        test_field = "field1"
        test_value = ""
        
        write_node = self._get_random_node()
        print(f"  HSETNX '{test_key}' '{test_field}' = '' (empty)...")
        try:
            result = write_node.hsetnx(test_key, test_field, test_value)
            if result != 1:
                print(f"  FAILED: Expected return 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSETNX failed - {e}")
            return False
        
        # Verify the empty value was set
        value = write_node.hget(test_key, test_field)
        if value != test_value:
            print(f"  FAILED: Expected empty string, got '{value}'")
            return False
        
        print("  PASSED")
        return True

    def test_hvals_basic(self) -> bool:
        """Test basic HVALS operation."""
        print("\nTest: HVALS Basic")
        
        test_key = "test_hash_hvals"
        fields = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Set multiple fields using HSET
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' with multiple fields...")
        try:
            result = write_node.hset(test_key, mapping=fields)
            if result != 3:
                print(f"  FAILED: Expected return 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # HVALS from all nodes
        print("  HVALS from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.hvals(test_key)
                # redis-py returns a list
                if not isinstance(result, list):
                    print(f"    Node {i}: FAILED (expected list, got {type(result).__name__})")
                    return False
                
                # Check all values are present (order may vary)
                result_set = set(result)
                expected_set = set(fields.values())
                if result_set != expected_set:
                    print(f"    Node {i}: FAILED (expected {expected_set}, got {result_set})")
                    return False
                
                # Check no extra values
                if len(result) != len(fields):
                    print(f"    Node {i}: FAILED (expected {len(fields)} values, got {len(result)})")
                    return False
                
                print(f"    Node {i}: OK (all {len(fields)} values match)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True

    def test_hvals_empty_hash(self) -> bool:
        """Test HVALS on non-existent key returns empty list."""
        print("\nTest: HVALS Non-existent Key")
        
        test_key = "nonexistent_hash_for_hvals"
        
        node = self._get_random_node()
        try:
            result = node.hvals(test_key)
            if result != []:
                print(f"  FAILED: Expected empty list, got {result}")
                return False
            print("  HVALS on non-existent key returned empty list: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HVALS failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hvals_after_hdel(self) -> bool:
        """Test HVALS after deleting some fields."""
        print("\nTest: HVALS After HDEL")
        
        test_key = "test_hash_hvals_after_hdel"
        fields = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Set multiple fields
        write_node = self._get_random_node()
        write_node.hset(test_key, mapping=fields)
        
        # Delete one field
        print("  HDEL field2...")
        try:
            result = write_node.hdel(test_key, "field2")
            if result != 1:
                print(f"  FAILED: HDEL expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: HDEL failed - {e}")
            return False
        
        # HVALS should return remaining values
        print("  HVALS after HDEL...")
        try:
            result = write_node.hvals(test_key)
            expected = {"value1", "value3"}
            result_set = set(result)
            if result_set != expected:
                print(f"  FAILED: Expected {expected}, got {result_set}")
                return False
            print(f"  HVALS returned {len(result)} values: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HVALS failed - {e}")
            return False
        
        print("  PASSED")
        return True

    def test_hvals_after_hset_update(self) -> bool:
        """Test HVALS returns updated values after HSET update."""
        print("\nTest: HVALS After HSET Update")
        
        test_key = "test_hash_hvals_update"
        
        # Set initial fields
        write_node = self._get_random_node()
        write_node.hset(test_key, "field1", "initial_value")
        write_node.hset(test_key, "field2", "value2")
        
        # Update one field
        print("  Update field1 with new value...")
        write_node.hset(test_key, "field1", "updated_value")
        
        # HVALS should return updated values
        print("  HVALS after update...")
        try:
            result = write_node.hvals(test_key)
            result_set = set(result)
            expected_set = {"updated_value", "value2"}
            if result_set != expected_set:
                print(f"  FAILED: expected {expected_set}, got {result_set}")
                return False
            print("  HVALS returned correct values: OK")
        except redis.RedisError as e:
            print(f"  FAILED: HVALS failed - {e}")
            return False
        
        print("  PASSED")
        return True
    
    def test_chaos_hset_hget(self) -> bool:
        """Test HSET/HGET operations with one random node killed, then verify recovery."""
        print("\nTest: Chaos - HSET/HGET with one node down + recovery verification")
        
        test_key = "chaos_test_hash"
        test_field = "chaos_field"
        test_value = "chaos_value"
        killed_node = None
        
        # Use auto_recover=True so nodes are recovered when context exits
        with self.chaos_context(kill_count=1, auto_recover=True) as killed_nodes:
            print(f"  [Chaos] Running test with 1 node(s) down...")
            
            # Record the killed node (before it's recovered)
            if killed_nodes:
                killed_node = killed_nodes[0]
                print(f"  [Chaos] Killed: {killed_node}")
            
            # Get alive nodes for write/read
            alive = self.get_alive_nodes()
            if len(alive) < 2:
                print("  FAILED: Not enough alive nodes for test")
                return False
            
            write_node = random.choice(alive)
            read_node = random.choice([n for n in alive if n != write_node])
            
            # Write to an alive node
            print(f"  HSET '{test_key}' '{test_field}' = '{test_value}' on {write_node}...")
            try:
                result = write_node.conn.hset(test_key, test_field, test_value)
                if result != 1:
                    print(f"  FAILED: Expected return 1, got {result}")
                    return False
            except redis.RedisError as e:
                print(f"  FAILED: HSET failed - {e}")
                return False
            
            # Read from another alive node
            print(f"  HGET from {read_node}...")
            try:
                value = read_node.conn.hget(test_key, test_field)
                if value != test_value:
                    print(f"  FAILED: Expected '{test_value}', got '{value}'")
                    return False
                print(f"  OK: Read '{value}' from surviving node")
            except redis.RedisError as e:
                print(f"  FAILED: HGET failed - {e}")
                return False
        
        # After context exit, nodes are recovered. Verify killed node has the data.
        if killed_node and killed_node.alive:
            print(f"  Verifying recovered {killed_node} has the data...")
            try:
                value = killed_node.conn.hget(test_key, test_field)
                if value == test_value:
                    print(f"  OK: Recovered node has '{value}'")
                    return True
                else:
                    print(f"  FAILED: Recovered node has '{value}', expected '{test_value}'")
                    return False
            except redis.RedisError as e:
                print(f"  FAILED: HGET from recovered node failed - {e}")
                return False
        
        return True
    
    def run_all_tests(self) -> bool:
        """Run all tests."""
        print("\n" + "="*50)
        print("Running Hash Tests")
        print("="*50)
        
        if not self.setup():
            return False
        
        tests = [
            self.test_hset_and_hget,
            self.test_hset_multiple_fields,
            self.test_hset_multiple_fields_mixed,
            self.test_hset_atomicity_batch_consistency,
            self.test_hset_update_existing,
            self.test_hget_nonexistent,
            self.test_hgetall_basic,
            self.test_hgetall_empty_hash,
            self.test_hgetall_after_hdel,
            self.test_hdel_atomicity_batch_consistency,
            self.test_hgetall_after_hset_update,
            self.test_hkeys_basic,
            self.test_hkeys_empty_hash,
            self.test_hkeys_after_hdel,
            self.test_hkeys_after_hset_update,
            self.test_hlen_basic,
            self.test_hlen_empty_hash,
            self.test_hlen_after_hdel,
            self.test_hlen_after_hset_update,
            self.test_hexists_basic,
            self.test_hexists_nonexistent_field,
            self.test_hexists_nonexistent_key,
            self.test_hexists_after_hdel,
            self.test_hexists_after_hset_update,
            self.test_hsetnx_new_field,
            self.test_hsetnx_existing_field,
            self.test_hsetnx_replication,
            self.test_hsetnx_atomicity_field_creation,
            self.test_hsetnx_empty_value,
            self.test_hvals_basic,
            self.test_hvals_empty_hash,
            self.test_hvals_after_hdel,
            self.test_hvals_after_hset_update,
            self.test_hmget_basic,
            self.test_hmget_partial_fields,
            self.test_hmget_nonexistent_key,
            self.test_hmget_single_field,
            self.test_chaos_hset_hget,
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
                print(f"  EXCEPTION: {e}")
                import traceback
                traceback.print_exc()
                failed += 1
        
        print("\n" + "="*50)
        print(f"Results: {passed} passed, {failed} failed")
        print("="*50)
        
        return failed == 0


def main():
    """Main entry point."""
    # Get the directory containing this script
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Handle signal for clean shutdown
    def signal_handler(sig, frame):
        print("\n\nInterrupted, cleaning up...")
        cluster.stop()
        sys.exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create cluster manager
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
        # Run tests
        tester = TestClusterHash(cluster)
        success = tester.run_all_tests()
        
        if success:
            print("\n✅ All hash tests passed!")
        else:
            print("\n❌ Some hash tests failed!")
            
    finally:
        # Always stop cluster
        cluster.stop()
        cluster.clean()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
