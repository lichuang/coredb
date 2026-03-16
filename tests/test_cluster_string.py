#!/usr/bin/env python3
"""
CoreDB Cluster String Integration Tests

This test suite:
1. Starts a 3-node CoreDB cluster
2. Performs basic SET/GET operations
3. Verifies data replication across nodes
4. Tests persistence after restart
5. Stops the cluster

Usage:
    pip install -r requirements.txt
    python test_cluster_string.py
"""

import random
import time
import sys
import os
import signal

import redis

from cluster_manager import ClusterManager
from base_test import TestClusterBase


class TestClusterString(TestClusterBase):
    """String command tests."""
    
    def test_set_and_get(self) -> bool:
        """Test basic SET and GET operations."""
        print("\nTest: SET and GET")
        
        test_key = "test_key"
        test_value = "test_value_123"
        
        # SET to a random node
        write_node = self._get_random_node()
        print(f"  SET '{test_key}' = '{test_value}' on a random node...")
        try:
            write_node.set(test_key, test_value)
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        # GET from all nodes
        print("  GET from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
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
    
    def test_set_with_expiration(self) -> bool:
        """Test SET with TTL."""
        print("\nTest: SET with expiration (PX)")
        
        test_key = "expiring_key"
        test_value = "will_expire"
        
        # SET with 500ms expiration on a random node
        write_node = self._get_random_node()
        print(f"  SET with 500ms TTL on a random node...")
        try:
            write_node.set(test_key, test_value, px=500)
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        # Verify it's readable immediately from the same node
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key not readable immediately after write")
            return False
        print("  Key readable immediately: OK")
        
        # Wait for expiration
        print("  Waiting for expiration...")
        time.sleep(1)
        
        # Verify it's expired (returns None)
        value = self.nodes[0].conn.get(test_key)
        if value is not None:
            print(f"  FAILED: Key should have expired but got '{value}'")
            return False
        print("  Key expired correctly: OK")
        
        print("  PASSED")
        return True
    
    def test_set_with_keepttl(self) -> bool:
        """Test SET with KEEPTTL option."""
        print("\nTest: SET with KEEPTTL")
        
        test_key = "keepttl_key"
        initial_value = "initial_value"
        new_value = "new_value_with_keepttl"
        
        # SET with 2 seconds expiration on a random node
        write_node = self._get_random_node()
        print(f"  SET with 2000ms TTL...")
        try:
            write_node.set(test_key, initial_value, px=2000)
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        # Verify initial value is readable
        value = write_node.get(test_key)
        if value != initial_value:
            print(f"  FAILED: Initial value not readable")
            return False
        print("  Initial value readable: OK")
        
        # Wait a bit (500ms) then update with KEEPTTL
        print("  Waiting 500ms...")
        time.sleep(0.5)
        
        # SET with KEEPTTL - should preserve the original expiration
        print("  SET with KEEPTTL...")
        try:
            # Use execute_command to send raw KEEPTTL option
            write_node.execute_command('SET', test_key, new_value, 'KEEPTTL')
        except redis.RedisError as e:
            print(f"  FAILED: SET KEEPTTL failed - {e}")
            return False
        
        # Verify new value is readable
        value = write_node.get(test_key)
        if value != new_value:
            print(f"  FAILED: New value not readable, got '{value}'")
            return False
        print("  New value readable: OK")
        
        # Wait for the original expiration to pass (remaining ~1.5s + buffer)
        print("  Waiting for original expiration (1.5s)...")
        time.sleep(1.6)
        
        # Verify it's expired - if KEEPTTL worked, it should be gone
        value = self._get_random_node().get(test_key)
        if value is not None:
            print(f"  FAILED: Key should have expired but got '{value}'")
            return False
        print("  Key expired with original TTL: OK")
        
        print("  PASSED")
        return True
    
    def _wait_for_ports_free(self, timeout: int = 30) -> bool:
        """Wait for all ports to be free."""
        import socket
        ports = [6379, 6380, 6381, 7001, 7002, 7003]
        start_time = time.time()
        while time.time() - start_time < timeout:
            all_free = True
            for port in ports:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(('localhost', port))
                sock.close()
                if result == 0:  # Port is in use
                    all_free = False
                    break
            if all_free:
                return True
            time.sleep(0.5)
        return False
    
    def _close_all_connections(self):
        """Close all Redis connections."""
        for node in self.nodes:
            try:
                node.conn.close()
            except:
                pass
    
    def test_restart_persistence(self) -> bool:
        """Test that data persists after cluster restart."""
        print("\nTest: Restart Persistence")
        
        # Step 1: Write some data
        test_key_1 = "persistent_key_1"
        test_value_1 = "persistent_value_1"
        test_key_2 = "persistent_key_2"
        test_value_2 = "persistent_value_2"
        
        print(f"  Writing initial data...")
        try:
            write_node = self._get_random_node()
            write_node.set(test_key_1, test_value_1)
            write_node.set(test_key_2, test_value_2)
            print(f"    SET '{test_key_1}' = '{test_value_1}'")
            print(f"    SET '{test_key_2}' = '{test_value_2}'")
        except redis.RedisError as e:
            print(f"  FAILED: Failed to write initial data - {e}")
            return False
        
        # Verify data is written
        verify_node = self._get_random_node()
        value = verify_node.get(test_key_1)
        if value != test_value_1:
            print(f"  FAILED: Initial write verification failed")
            return False
        print("  Initial data written successfully")
        
        # Remember the write node for later verification
        initial_write_node = write_node
        
        # Step 2: Close all connections before stopping
        print("  Closing connections...")
        self._close_all_connections()
        time.sleep(1)
        
        # Step 3: Stop the cluster (without cleaning data)
        print("  Stopping cluster...")
        self.cluster.stop()
        
        # Wait longer for complete shutdown
        print("  Waiting for complete shutdown...")
        time.sleep(5)
        
        # Step 4: Restart the cluster
        print("  Restarting cluster...")
        if not self.cluster.start():
            print("  FAILED: Failed to restart cluster")
            return False
        
        # Step 5: Reconnect to nodes
        print("  Reconnecting to nodes...")
        if not self.setup():
            print("  FAILED: Failed to reconnect to nodes")
            return False
        
        # Step 5: Verify old data is still there
        print("  Verifying old data persisted...")
        try:
            verify_node = self._get_random_node()
            value_1 = verify_node.get(test_key_1)
            value_2 = verify_node.get(test_key_2)
            
            if value_1 != test_value_1:
                print(f"    FAILED: Key '{test_key_1}' should be '{test_value_1}' but got '{value_1}'")
                return False
            print(f"    '{test_key_1}': OK (got '{value_1}')")
            
            if value_2 != test_value_2:
                print(f"    FAILED: Key '{test_key_2}' should be '{test_value_2}' but got '{value_2}'")
                return False
            print(f"    '{test_key_2}': OK (got '{value_2}')")
            
        except redis.RedisError as e:
            print(f"  FAILED: Error reading persisted data - {e}")
            return False
        
        # Step 6: Write new data after restart
        print("  Writing new data after restart...")
        new_key = "new_key_after_restart"
        new_value = "new_value_after_restart"
        
        try:
            write_node = self._get_random_node()
            write_node.set(new_key, new_value)
            value = write_node.get(new_key)
            if value != new_value:
                print(f"  FAILED: New data write failed")
                return False
            print(f"    SET '{new_key}' = '{new_value}': OK")
        except redis.RedisError as e:
            print(f"  FAILED: Error writing new data - {e}")
            return False
        
        # Step 7: Verify all data (old + new) is readable from all nodes
        print("  Verifying all data on all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                v1 = node.conn.get(test_key_1)
                v2 = node.conn.get(test_key_2)
                v3 = node.conn.get(new_key)
                
                if v1 != test_value_1 or v2 != test_value_2 or v3 != new_value:
                    print(f"    Node {i}: FAILED (data mismatch)")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_set_nx_new_key(self) -> bool:
        """Test SET with NX option on a new key (should succeed)."""
        print("\nTest: SET with NX on new key")
        
        test_key = "nx_new_key"
        test_value = "nx_value"
        
        write_node = self._get_random_node()
        print(f"  SET '{test_key}' = '{test_value}' NX on a random node...")
        try:
            result = write_node.execute_command('SET', test_key, test_value, 'NX')
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET NX failed - {e}")
            return False
        
        # Verify value was set
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key not set, got '{value}'")
            return False
        print("  Key set successfully: OK")
        
        print("  PASSED")
        return True
    
    def test_set_nx_existing_key(self) -> bool:
        """Test SET with NX option on existing key (should fail)."""
        print("\nTest: SET with NX on existing key")
        
        test_key = "nx_existing_key"
        initial_value = "initial_value"
        new_value = "new_value"
        
        write_node = self._get_random_node()
        
        # First set the key
        print(f"  Initial SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        # Try SET NX on existing key
        print(f"  SET '{test_key}' = '{new_value}' NX...")
        try:
            result = write_node.execute_command('SET', test_key, new_value, 'NX')
            if result is not None:
                print(f"  FAILED: Expected None (nil), got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET NX failed - {e}")
            return False
        
        # Verify value was NOT changed
        value = write_node.get(test_key)
        if value != initial_value:
            print(f"  FAILED: Value was changed to '{value}', expected '{initial_value}'")
            return False
        print("  Value unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_set_xx_existing_key(self) -> bool:
        """Test SET with XX option on existing key (should succeed)."""
        print("\nTest: SET with XX on existing key")
        
        test_key = "xx_existing_key"
        initial_value = "initial_value"
        new_value = "xx_new_value"
        
        write_node = self._get_random_node()
        
        # First set the key
        print(f"  Initial SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        # SET XX on existing key
        print(f"  SET '{test_key}' = '{new_value}' XX...")
        try:
            result = write_node.execute_command('SET', test_key, new_value, 'XX')
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET XX failed - {e}")
            return False
        
        # Verify value WAS changed
        value = write_node.get(test_key)
        if value != new_value:
            print(f"  FAILED: Value not changed, got '{value}', expected '{new_value}'")
            return False
        print("  Value changed: OK")
        
        print("  PASSED")
        return True
    
    def test_set_xx_new_key(self) -> bool:
        """Test SET with XX option on non-existent key (should fail)."""
        print("\nTest: SET with XX on non-existent key")
        
        test_key = "xx_new_key"
        test_value = "xx_value"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # SET XX on non-existent key
        print(f"  SET '{test_key}' = '{test_value}' XX...")
        try:
            result = write_node.execute_command('SET', test_key, test_value, 'XX')
            if result is not None:
                print(f"  FAILED: Expected None (nil), got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET XX failed - {e}")
            return False
        
        # Verify key was NOT created
        value = write_node.get(test_key)
        if value is not None:
            print(f"  FAILED: Key was created with value '{value}'")
            return False
        print("  Key not created: OK")
        
        print("  PASSED")
        return True
    
    def test_set_get_new_key(self) -> bool:
        """Test SET with GET option on new key (should return nil)."""
        print("\nTest: SET with GET on new key")
        
        test_key = "get_new_key"
        test_value = "get_value"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # SET GET on new key
        print(f"  SET '{test_key}' = '{test_value}' GET...")
        try:
            result = write_node.execute_command('SET', test_key, test_value, 'GET')
            # Should return nil since key didn't exist
            if result is not None:
                print(f"  FAILED: Expected None, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET GET failed - {e}")
            return False
        
        # Verify key was set
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key not set, got '{value}'")
            return False
        print("  Key set, previous value was nil: OK")
        
        print("  PASSED")
        return True
    
    def test_set_get_existing_key(self) -> bool:
        """Test SET with GET option on existing key (should return old value)."""
        print("\nTest: SET with GET on existing key")
        
        test_key = "get_existing_key"
        initial_value = "initial_value"
        new_value = "get_new_value"
        
        write_node = self._get_random_node()
        
        # First set the key
        print(f"  Initial SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        # SET GET on existing key
        print(f"  SET '{test_key}' = '{new_value}' GET...")
        try:
            result = write_node.execute_command('SET', test_key, new_value, 'GET')
            if result != initial_value:
                print(f"  FAILED: Expected '{initial_value}', got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET GET failed - {e}")
            return False
        
        # Verify value was updated
        value = write_node.get(test_key)
        if value != new_value:
            print(f"  FAILED: Key not updated, got '{value}', expected '{new_value}'")
            return False
        print(f"  Previous value '{result}' returned, key updated: OK")
        
        print("  PASSED")
        return True
    
    def test_set_nx_get_new_key(self) -> bool:
        """Test SET with NX GET on new key (should set and return nil)."""
        print("\nTest: SET with NX GET on new key")
        
        test_key = "nx_get_new_key"
        test_value = "nx_get_value"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # SET NX GET on new key
        print(f"  SET '{test_key}' = '{test_value}' NX GET...")
        try:
            result = write_node.execute_command('SET', test_key, test_value, 'NX', 'GET')
            # Should return nil since key didn't exist
            if result is not None:
                print(f"  FAILED: Expected None, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET NX GET failed - {e}")
            return False
        
        # Verify key was set
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key not set, got '{value}'")
            return False
        print("  Key set, previous value was nil: OK")
        
        print("  PASSED")
        return True
    
    def test_set_nx_get_existing_key(self) -> bool:
        """Test SET with NX GET on existing key (should return current value, not set)."""
        print("\nTest: SET with NX GET on existing key")
        
        test_key = "nx_get_existing_key"
        initial_value = "initial_value"
        new_value = "nx_get_new_value"
        
        write_node = self._get_random_node()
        
        # First set the key
        print(f"  Initial SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        # SET NX GET on existing key
        print(f"  SET '{test_key}' = '{new_value}' NX GET...")
        try:
            result = write_node.execute_command('SET', test_key, new_value, 'NX', 'GET')
            # Should return current value since key exists (and not set new value)
            if result != initial_value:
                print(f"  FAILED: Expected '{initial_value}', got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET NX GET failed - {e}")
            return False
        
        # Verify value was NOT changed
        value = write_node.get(test_key)
        if value != initial_value:
            print(f"  FAILED: Value was changed to '{value}', expected '{initial_value}'")
            return False
        print(f"  Current value '{result}' returned, key unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_set_xx_get_existing_key(self) -> bool:
        """Test SET with XX GET on existing key (should set and return old value)."""
        print("\nTest: SET with XX GET on existing key")
        
        test_key = "xx_get_existing_key"
        initial_value = "initial_value"
        new_value = "xx_get_new_value"
        
        write_node = self._get_random_node()
        
        # First set the key
        print(f"  Initial SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        # SET XX GET on existing key
        print(f"  SET '{test_key}' = '{new_value}' XX GET...")
        try:
            result = write_node.execute_command('SET', test_key, new_value, 'XX', 'GET')
            if result != initial_value:
                print(f"  FAILED: Expected '{initial_value}', got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET XX GET failed - {e}")
            return False
        
        # Verify value was updated
        value = write_node.get(test_key)
        if value != new_value:
            print(f"  FAILED: Key not updated, got '{value}', expected '{new_value}'")
            return False
        print(f"  Previous value '{result}' returned, key updated: OK")
        
        print("  PASSED")
        return True
    
    def test_set_xx_get_new_key(self) -> bool:
        """Test SET with XX GET on non-existent key (should return nil, not set)."""
        print("\nTest: SET with XX GET on non-existent key")
        
        test_key = "xx_get_new_key"
        test_value = "xx_get_value"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # SET XX GET on non-existent key
        print(f"  SET '{test_key}' = '{test_value}' XX GET...")
        try:
            result = write_node.execute_command('SET', test_key, test_value, 'XX', 'GET')
            # Should return nil since key doesn't exist (and not set)
            if result is not None:
                print(f"  FAILED: Expected None, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET XX GET failed - {e}")
            return False
        
        # Verify key was NOT created
        value = write_node.get(test_key)
        if value is not None:
            print(f"  FAILED: Key was created with value '{value}'")
            return False
        print("  Key not created: OK")
        
        print("  PASSED")
        return True
    
    def test_chaos_set_get(self) -> bool:
        """Test SET/GET operations with one random node killed, then verify recovery."""
        print("\nTest: Chaos - SET/GET with one node down + recovery verification")
        
        test_key = "chaos_test_key"
        test_value = "chaos_test_value"
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
            print(f"  SET '{test_key}' = '{test_value}' on {write_node}...")
            try:
                write_node.conn.set(test_key, test_value)
            except redis.RedisError as e:
                print(f"  FAILED: SET failed - {e}")
                return False
            
            # Read from another alive node
            print(f"  GET from {read_node}...")
            try:
                value = read_node.conn.get(test_key)
                if value != test_value:
                    print(f"  FAILED: Expected '{test_value}', got '{value}'")
                    return False
                print(f"  OK: Read '{value}' from surviving node")
            except redis.RedisError as e:
                print(f"  FAILED: GET failed - {e}")
                return False
        
        # After context exit, nodes are recovered. Verify killed node has the data.
        if killed_node and killed_node.alive:
            print(f"  Verifying recovered {killed_node} has the data...")
            try:
                value = killed_node.conn.get(test_key)
                if value == test_value:
                    print(f"  OK: Recovered node has '{value}'")
                    return True
                else:
                    print(f"  FAILED: Recovered node has '{value}', expected '{test_value}'")
                    return False
            except redis.RedisError as e:
                print(f"  FAILED: GET from recovered node failed - {e}")
                return False
        
        return True
    
    def run_all_tests(self) -> bool:
        """Run all tests."""
        print("\n" + "="*50)
        print("Running String Tests")
        print("="*50)
        
        if not self.setup():
            return False
        
        tests = [
            self.test_set_and_get,
            self.test_set_with_expiration,
            self.test_set_with_keepttl,
            self.test_set_nx_new_key,
            self.test_set_nx_existing_key,
            self.test_set_xx_existing_key,
            self.test_set_xx_new_key,
            self.test_set_get_new_key,
            self.test_set_get_existing_key,
            self.test_set_nx_get_new_key,
            self.test_set_nx_get_existing_key,
            self.test_set_xx_get_existing_key,
            self.test_set_xx_get_new_key,
            self.test_restart_persistence,
            self.test_chaos_set_get,  # Chaos test enabled
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
        tester = TestClusterString(cluster)
        success = tester.run_all_tests()
        
        if success:
            print("\n✅ All string tests passed!")
        else:
            print("\n❌ Some string tests failed!")
            
    finally:
        # Always stop cluster
        cluster.stop()
        cluster.clean()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
