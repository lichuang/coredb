#!/usr/bin/env python3
"""
CoreDB Cluster Hash Integration Tests

This test suite:
1. Starts a 3-node CoreDB cluster
2. Performs basic HSET/HGET operations
3. Verifies data replication across nodes
4. Tests multiple fields in a hash
5. Stops the cluster

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
    
    def _verify_connection(self, node: redis.Redis) -> None:
        """Verify connection using HSET/HGET."""
        node.hset('_test_conn', 'field', 'ok')
    
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
        """Test HSET with multiple fields."""
        print("\nTest: HSET Multiple Fields")
        
        test_key = "test_hash_multi"
        fields = {
            "name": "John",
            "age": "30",
            "city": "New York"
        }
        
        # HSET multiple fields
        write_node = self._get_random_node()
        print(f"  HSET '{test_key}' with multiple fields...")
        try:
            for field, value in fields.items():
                result = write_node.hset(test_key, field, value)
                if result != 1:
                    print(f"  FAILED: Expected return 1 for new field '{field}', got {result}")
                    return False
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
            self.test_hset_update_existing,
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
