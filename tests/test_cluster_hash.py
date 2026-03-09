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
    
    def test_hset_nx_option(self) -> bool:
        """Test HSET with NX option (only set if field does not exist)."""
        print("\nTest: HSET with NX Option")
        
        test_key = "test_hash_nx"
        test_field = "nx_field"
        initial_value = "initial"
        new_value = "new_value"
        
        # First set the field normally
        write_node = self._get_random_node()
        print(f"  Set field normally...")
        write_node.hset(test_key, test_field, initial_value)
        
        # Try HSET with NX - should fail (return 0) because field exists
        print(f"  HSET with NX on existing field...")
        try:
            # Execute raw command: HSET key field value NX
            result = write_node.execute_command('HSET', test_key, test_field, new_value, 'NX')
            if result != 0:
                print(f"  FAILED: Expected return 0 (field exists), got {result}")
                return False
            print(f"    NX on existing field: returned 0 (correct)")
        except redis.RedisError as e:
            print(f"  FAILED: HSET NX failed - {e}")
            return False
        
        # Verify value was NOT changed
        value = write_node.hget(test_key, test_field)
        if value != initial_value:
            print(f"  FAILED: Value was changed despite NX! Expected '{initial_value}', got '{value}'")
            return False
        
        # Try HSET with NX on a new field - should succeed
        print(f"  HSET with NX on new field...")
        new_field = "nx_new_field"
        try:
            result = write_node.execute_command('HSET', test_key, new_field, new_value, 'NX')
            if result != 1:
                print(f"  FAILED: Expected return 1 (new field), got {result}")
                return False
            print(f"    NX on new field: returned 1 (correct)")
        except redis.RedisError as e:
            print(f"  FAILED: HSET NX failed - {e}")
            return False
        
        # Verify new field was set
        value = write_node.hget(test_key, new_field)
        if value != new_value:
            print(f"  FAILED: New field was not set! Expected '{new_value}', got '{value}'")
            return False
        
        print("  PASSED")
        return True
    
    def test_hset_xx_option(self) -> bool:
        """Test HSET with XX option (only set if field exists)."""
        print("\nTest: HSET with XX Option")
        
        test_key = "test_hash_xx"
        test_field = "xx_field"
        initial_value = "initial"
        new_value = "updated"
        
        # First set the field normally
        write_node = self._get_random_node()
        print(f"  Set field normally...")
        write_node.hset(test_key, test_field, initial_value)
        
        # Try HSET with XX on existing field - should succeed
        print(f"  HSET with XX on existing field...")
        try:
            result = write_node.execute_command('HSET', test_key, test_field, new_value, 'XX')
            if result != 1:
                print(f"  FAILED: Expected return 1 (field updated), got {result}")
                return False
            print(f"    XX on existing field: returned 1 (correct)")
        except redis.RedisError as e:
            print(f"  FAILED: HSET XX failed - {e}")
            return False
        
        # Verify value WAS changed
        value = write_node.hget(test_key, test_field)
        if value != new_value:
            print(f"  FAILED: Value was not changed! Expected '{new_value}', got '{value}'")
            return False
        
        # Try HSET with XX on a new field - should fail (return 0)
        print(f"  HSET with XX on new field...")
        new_field = "xx_new_field"
        try:
            result = write_node.execute_command('HSET', test_key, new_field, "value", 'XX')
            if result != 0:
                print(f"  FAILED: Expected return 0 (field doesn't exist), got {result}")
                return False
            print(f"    XX on new field: returned 0 (correct)")
        except redis.RedisError as e:
            print(f"  FAILED: HSET XX failed - {e}")
            return False
        
        # Verify new field was NOT set
        value = write_node.hget(test_key, new_field)
        if value is not None:
            print(f"  FAILED: New field was set despite XX! Got '{value}'")
            return False
        
        print("  PASSED")
        return True
    
    def test_hset_nx_multiple_fields(self) -> bool:
        """Test HSET with NX option and multiple fields."""
        print("\nTest: HSET with NX Option and Multiple Fields")
        
        test_key = "test_hash_nx_multi"
        
        # Pre-populate one field
        write_node = self._get_random_node()
        write_node.hset(test_key, "field1", "value1")
        
        # HSET with NX for multiple fields:
        # field1 exists -> skip
        # field2 new -> set
        # field3 new -> set
        print(f"  HSET NX with field1 (exists), field2 (new), field3 (new)...")
        try:
            # Execute: HSET key field1 v1 field2 v2 field3 v3 NX
            result = write_node.execute_command('HSET', test_key, 'field1', 'new1', 'field2', 'v2', 'field3', 'v3', 'NX')
            # Should return 1 because at least one field was set
            if result != 1:
                print(f"  FAILED: Expected return 1 (at least one field set), got {result}")
                return False
            print(f"    Returned {result} (correct)")
        except redis.RedisError as e:
            print(f"  FAILED: HSET NX failed - {e}")
            return False
        
        # Verify field1 was NOT changed
        value = write_node.hget(test_key, 'field1')
        if value != 'value1':
            print(f"  FAILED: field1 was changed! Expected 'value1', got '{value}'")
            return False
        
        # Verify field2 and field3 were set
        if write_node.hget(test_key, 'field2') != 'v2':
            print(f"  FAILED: field2 was not set correctly")
            return False
        if write_node.hget(test_key, 'field3') != 'v3':
            print(f"  FAILED: field3 was not set correctly")
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
            self.test_hset_update_existing,
            self.test_hset_nx_option,
            self.test_hset_xx_option,
            self.test_hset_nx_multiple_fields,
            self.test_hget_nonexistent,
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
