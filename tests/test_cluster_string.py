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
            write_node.set(test_key, new_value, keepttl=True)
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
            result = write_node.set(test_key, test_value, nx=True)
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
            result = write_node.set(test_key, new_value, nx=True)
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
            result = write_node.set(test_key, new_value, xx=True)
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
            result = write_node.set(test_key, test_value, xx=True)
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
            result = write_node.set(test_key, test_value, get=True)
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
            result = write_node.set(test_key, new_value, get=True)
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
            result = write_node.set(test_key, test_value, nx=True, get=True)
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
            result = write_node.set(test_key, new_value, nx=True, get=True)
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
            result = write_node.set(test_key, new_value, xx=True, get=True)
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
            result = write_node.set(test_key, test_value, xx=True, get=True)
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
    
    def test_del_single_key(self) -> bool:
        """Test deleting a single key."""
        print("\nTest: DEL single key")
        
        test_key = "del_single_key"
        test_value = "del_single_value"
        
        write_node = self._get_random_node()
        
        # First set the key
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        # Verify key exists
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key not set properly")
            return False
        print("  Key set: OK")
        
        # Delete the key
        print(f"  DEL '{test_key}'...")
        try:
            result = write_node.delete(test_key)
            if result != 1:
                print(f"  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DEL failed - {e}")
            return False
        print("  Deleted 1 key: OK")
        
        # Verify key is gone
        value = write_node.get(test_key)
        if value is not None:
            print(f"  FAILED: Key still exists after DEL")
            return False
        print("  Key no longer exists: OK")
        
        print("  PASSED")
        return True
    
    def test_del_multiple_keys(self) -> bool:
        """Test deleting multiple keys at once."""
        print("\nTest: DEL multiple keys")
        
        keys = ["del_multi_key1", "del_multi_key2", "del_multi_key3"]
        
        write_node = self._get_random_node()
        
        # Set multiple keys
        print("  Setting 3 keys...")
        for i, key in enumerate(keys):
            write_node.set(key, f"value{i}")
        
        # Delete all keys at once
        print("  DEL all 3 keys...")
        try:
            result = write_node.delete(*keys)
            if result != 3:
                print(f"  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DEL failed - {e}")
            return False
        print("  Deleted 3 keys: OK")
        
        # Verify all keys are gone
        print("  Verifying all keys deleted...")
        for key in keys:
            value = write_node.get(key)
            if value is not None:
                print(f"  FAILED: Key '{key}' still exists")
                return False
        print("  All keys deleted: OK")
        
        print("  PASSED")
        return True
    
    def test_del_nonexistent_key(self) -> bool:
        """Test deleting a non-existent key."""
        print("\nTest: DEL non-existent key")
        
        test_key = "del_nonexistent_key"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # Try to delete non-existent key
        print(f"  DEL non-existent '{test_key}'...")
        try:
            result = write_node.delete(test_key)
            if result != 0:
                print(f"  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DEL failed - {e}")
            return False
        print("  Deleted 0 keys: OK")
        
        print("  PASSED")
        return True
    
    def test_del_mixed_keys(self) -> bool:
        """Test deleting mix of existing and non-existing keys."""
        print("\nTest: DEL mixed existing/non-existing keys")
        
        existing_keys = ["del_mixed_key1", "del_mixed_key2"]
        non_existing_keys = ["del_mixed_key3", "del_mixed_key4"]
        
        write_node = self._get_random_node()
        
        # Set only some keys
        print("  Setting 2 keys...")
        for key in existing_keys:
            write_node.set(key, "value")
        
        # Make sure non-existing keys don't exist
        for key in non_existing_keys:
            write_node.delete(key)
        
        # Delete mix of keys
        all_keys = existing_keys + non_existing_keys
        print(f"  DEL 2 existing + 2 non-existing keys...")
        try:
            result = write_node.delete(*all_keys)
            if result != 2:
                print(f"  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DEL failed - {e}")
            return False
        print("  Deleted 2 keys: OK")
        
        # Verify existing keys are gone
        for key in existing_keys:
            value = write_node.get(key)
            if value is not None:
                print(f"  FAILED: Key '{key}' still exists")
                return False
        print("  Existing keys deleted: OK")
        
        print("  PASSED")
        return True
    
    def test_del_replication(self) -> bool:
        """Test that DEL replicates to all nodes."""
        print("\nTest: DEL replication")
        
        test_key = "del_repl_key"
        test_value = "del_repl_value"
        
        write_node = self._get_random_node()
        
        # Set the key
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        # Delete the key
        print(f"  DEL '{test_key}'...")
        try:
            result = write_node.delete(test_key)
            if result != 1:
                print(f"  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DEL failed - {e}")
            return False
        
        # Verify key is gone from all nodes
        print("  Verifying deletion on all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
                if value is not None:
                    print(f"    Node {i}: FAILED (key still exists)")
                    return False
                print(f"    Node {i}: OK (key deleted)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_exists_single_key(self) -> bool:
        """Test EXISTS with a single existing key."""
        print("\nTest: EXISTS single existing key")
        
        test_key = "exists_single_key"
        test_value = "exists_single_value"
        
        write_node = self._get_random_node()
        
        # Set the key
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        # EXISTS should return 1
        print(f"  EXISTS '{test_key}'...")
        try:
            result = write_node.exists(test_key)
            if result != 1:
                print(f"  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXISTS failed - {e}")
            return False
        print("  EXISTS returned 1: OK")
        
        print("  PASSED")
        return True
    
    def test_exists_nonexistent_key(self) -> bool:
        """Test EXISTS with a non-existent key."""
        print("\nTest: EXISTS non-existent key")
        
        test_key = "exists_nonexistent_key"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # EXISTS should return 0
        print(f"  EXISTS '{test_key}'...")
        try:
            result = write_node.exists(test_key)
            if result != 0:
                print(f"  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXISTS failed - {e}")
            return False
        print("  EXISTS returned 0: OK")
        
        print("  PASSED")
        return True
    
    def test_exists_multiple_keys(self) -> bool:
        """Test EXISTS with multiple keys."""
        print("\nTest: EXISTS multiple keys")
        
        keys = ["exists_multi_key1", "exists_multi_key2", "exists_multi_key3"]
        
        write_node = self._get_random_node()
        
        # Set all keys
        print(f"  Setting 3 keys...")
        for i, key in enumerate(keys):
            write_node.set(key, f"value{i}")
        
        # EXISTS should return 3
        print(f"  EXISTS {keys}...")
        try:
            result = write_node.exists(*keys)
            if result != 3:
                print(f"  FAILED: Expected 3, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXISTS failed - {e}")
            return False
        print("  EXISTS returned 3: OK")
        
        print("  PASSED")
        return True
    
    def test_exists_mixed_keys(self) -> bool:
        """Test EXISTS with mix of existing and non-existing keys."""
        print("\nTest: EXISTS mixed existing/non-existing keys")
        
        existing_keys = ["exists_mixed1", "exists_mixed3"]
        non_existing_keys = ["exists_mixed2", "exists_mixed4"]
        all_keys = existing_keys + non_existing_keys
        
        write_node = self._get_random_node()
        
        # Set only some keys
        print(f"  Setting 2 keys...")
        for key in existing_keys:
            write_node.set(key, "value")
        
        # Make sure non-existing keys don't exist
        for key in non_existing_keys:
            write_node.delete(key)
        
        # EXISTS should return 2 (only existing keys)
        print(f"  EXISTS {all_keys}...")
        try:
            result = write_node.exists(*all_keys)
            if result != 2:
                print(f"  FAILED: Expected 2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXISTS failed - {e}")
            return False
        print("  EXISTS returned 2: OK")
        
        print("  PASSED")
        return True
    
    def test_exists_duplicate_keys(self) -> bool:
        """Test EXISTS with duplicate keys (should count multiple times)."""
        print("\nTest: EXISTS duplicate keys")
        
        test_key = "exists_duplicate_key"
        
        write_node = self._get_random_node()
        
        # Set the key
        print(f"  SET '{test_key}'...")
        write_node.set(test_key, "value")
        
        # EXISTS with same key twice should return 2
        print(f"  EXISTS '{test_key}' '{test_key}' (duplicate)...")
        try:
            result = write_node.exists(test_key, test_key)
            if result != 2:
                print(f"  FAILED: Expected 2 (duplicate counted twice), got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXISTS failed - {e}")
            return False
        print("  EXISTS returned 2 (duplicate counted): OK")
        
        print("  PASSED")
        return True
    
    def test_exists_after_del(self) -> bool:
        """Test EXISTS returns 0 after DEL."""
        print("\nTest: EXISTS after DEL")
        
        test_key = "exists_after_del_key"
        
        write_node = self._get_random_node()
        
        # Set and verify exists
        print(f"  SET '{test_key}'...")
        write_node.set(test_key, "value")
        
        result = write_node.exists(test_key)
        if result != 1:
            print(f"  FAILED: Key should exist before DEL")
            return False
        
        # Delete the key
        print(f"  DEL '{test_key}'...")
        write_node.delete(test_key)
        
        # EXISTS should return 0
        print(f"  EXISTS '{test_key}' after DEL...")
        try:
            result = write_node.exists(test_key)
            if result != 0:
                print(f"  FAILED: Expected 0 after DEL, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXISTS failed - {e}")
            return False
        print("  EXISTS returned 0 after DEL: OK")
        
        print("  PASSED")
        return True
    
    def test_exists_replication(self) -> bool:
        """Test that EXISTS works on all nodes."""
        print("\nTest: EXISTS replication")
        
        test_key = "exists_repl_key"
        test_value = "exists_repl_value"
        
        write_node = self._get_random_node()
        
        # Set the key
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        # EXISTS should return 1 on all nodes
        print("  EXISTS on all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.exists(test_key)
                if result != 1:
                    print(f"    Node {i}: FAILED (expected 1, got {result})")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_mget_single_key(self) -> bool:
        """Test MGET with a single key."""
        print("\nTest: MGET single key")
        
        test_key = "mget_single_key"
        test_value = "mget_single_value"
        
        write_node = self._get_random_node()
        
        # Set the key
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        # MGET the single key
        print(f"  MGET '{test_key}'...")
        try:
            result = write_node.mget(test_key)
            if result != [test_value]:
                print(f"  FAILED: Expected ['{test_value}'], got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: MGET failed - {e}")
            return False
        print(f"  Got expected value: OK")
        
        print("  PASSED")
        return True
    
    def test_mget_multiple_keys(self) -> bool:
        """Test MGET with multiple keys."""
        print("\nTest: MGET multiple keys")
        
        keys = ["mget_key1", "mget_key2", "mget_key3"]
        values = ["mget_value1", "mget_value2", "mget_value3"]
        
        write_node = self._get_random_node()
        
        # Set multiple keys
        print(f"  Setting 3 keys...")
        for key, value in zip(keys, values):
            write_node.set(key, value)
        
        # MGET all keys
        print(f"  MGET {keys}...")
        try:
            result = write_node.mget(*keys)
            if result != values:
                print(f"  FAILED: Expected {values}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: MGET failed - {e}")
            return False
        print(f"  Got expected values: OK")
        
        print("  PASSED")
        return True
    
    def test_mget_nonexistent_keys(self) -> bool:
        """Test MGET with non-existent keys returns None for each."""
        print("\nTest: MGET non-existent keys")
        
        keys = ["mget_nonexistent1", "mget_nonexistent2"]
        
        write_node = self._get_random_node()
        
        # Make sure keys don't exist
        for key in keys:
            write_node.delete(key)
        
        # MGET non-existent keys
        print(f"  MGET {keys}...")
        try:
            result = write_node.mget(*keys)
            if result != [None, None]:
                print(f"  FAILED: Expected [None, None], got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: MGET failed - {e}")
            return False
        print(f"  Got expected [None, None]: OK")
        
        print("  PASSED")
        return True
    
    def test_mget_mixed_keys(self) -> bool:
        """Test MGET with mix of existing and non-existing keys."""
        print("\nTest: MGET mixed existing/non-existing keys")
        
        existing_keys = ["mget_mixed1", "mget_mixed3"]
        non_existing_keys = ["mget_mixed2", "mget_mixed4"]
        all_keys = ["mget_mixed1", "mget_mixed2", "mget_mixed3", "mget_mixed4"]
        expected = ["value1", None, "value3", None]
        
        write_node = self._get_random_node()
        
        # Set only some keys
        print(f"  Setting 2 keys...")
        write_node.set("mget_mixed1", "value1")
        write_node.set("mget_mixed3", "value3")
        
        # Make sure non-existing keys don't exist
        for key in non_existing_keys:
            write_node.delete(key)
        
        # MGET mix of keys
        print(f"  MGET {all_keys}...")
        try:
            result = write_node.mget(*all_keys)
            if result != expected:
                print(f"  FAILED: Expected {expected}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: MGET failed - {e}")
            return False
        print(f"  Got expected mix: OK")
        
        print("  PASSED")
        return True
    
    def test_mget_replication(self) -> bool:
        """Test that MGET works on all nodes after write."""
        print("\nTest: MGET replication")
        
        keys = ["mget_repl1", "mget_repl2"]
        values = ["repl_value1", "repl_value2"]
        
        write_node = self._get_random_node()
        
        # Set keys
        print(f"  Setting 2 keys...")
        for key, value in zip(keys, values):
            write_node.set(key, value)
        
        # MGET from all nodes
        print("  MGET from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.mget(*keys)
                if result != values:
                    print(f"    Node {i}: FAILED (expected {values}, got {result})")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_mset_single_pair(self) -> bool:
        """Test MSET with a single key-value pair."""
        print("\nTest: MSET single pair")
        
        test_key = "mset_single_key"
        test_value = "mset_single_value"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # MSET the single pair
        print(f"  MSET '{test_key}' = '{test_value}'...")
        try:
            result = write_node.mset({test_key: test_value})
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: MSET failed - {e}")
            return False
        
        # Verify value was set
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key not set correctly, got '{value}'")
            return False
        print(f"  Key set successfully: OK")
        
        print("  PASSED")
        return True
    
    def test_mset_multiple_pairs(self) -> bool:
        """Test MSET with multiple key-value pairs."""
        print("\nTest: MSET multiple pairs")
        
        pairs = {
            "mset_key1": "mset_value1",
            "mset_key2": "mset_value2",
            "mset_key3": "mset_value3",
        }
        
        write_node = self._get_random_node()
        
        # Make sure keys don't exist
        for key in pairs.keys():
            write_node.delete(key)
        
        # MSET multiple pairs
        print(f"  MSET {len(pairs)} key-value pairs...")
        try:
            result = write_node.mset(pairs)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: MSET failed - {e}")
            return False
        
        # Verify all values were set
        print("  Verifying all keys...")
        for key, expected_value in pairs.items():
            value = write_node.get(key)
            if value != expected_value:
                print(f"  FAILED: Key '{key}' expected '{expected_value}', got '{value}'")
                return False
        print(f"  All keys set correctly: OK")
        
        print("  PASSED")
        return True
    
    def test_mset_overwrite(self) -> bool:
        """Test MSET overwrites existing values."""
        print("\nTest: MSET overwrites existing values")
        
        test_key = "mset_overwrite_key"
        initial_value = "initial_value"
        new_value = "new_value"
        
        write_node = self._get_random_node()
        
        # Set initial value
        print(f"  Initial SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        # Verify initial value
        value = write_node.get(test_key)
        if value != initial_value:
            print(f"  FAILED: Initial value not set correctly")
            return False
        
        # MSET to overwrite
        print(f"  MSET '{test_key}' = '{new_value}'...")
        try:
            result = write_node.mset({test_key: new_value})
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: MSET failed - {e}")
            return False
        
        # Verify value was overwritten
        value = write_node.get(test_key)
        if value != new_value:
            print(f"  FAILED: Value not overwritten, got '{value}'")
            return False
        print(f"  Value overwritten successfully: OK")
        
        print("  PASSED")
        return True
    
    def test_mset_replication(self) -> bool:
        """Test that MSET data is replicated to all nodes."""
        print("\nTest: MSET replication")
        
        pairs = {
            "mset_repl1": "repl_value1",
            "mset_repl2": "repl_value2",
        }
        
        write_node = self._get_random_node()
        
        # MSET on random node
        print(f"  MSET {len(pairs)} pairs on random node...")
        try:
            result = write_node.mset(pairs)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: MSET failed - {e}")
            return False
        
        # Verify all nodes have the data
        print("  Verifying all nodes...")
        for i, node in enumerate(self.nodes, 1):
            for key, expected_value in pairs.items():
                try:
                    value = node.conn.get(key)
                    if value != expected_value:
                        print(f"    Node {i}: FAILED (key '{key}' expected '{expected_value}', got '{value}')")
                        return False
                except redis.RedisError as e:
                    print(f"    Node {i}: FAILED - {e}")
                    return False
            print(f"    Node {i}: OK")
        
        print("  PASSED")
        return True
    
    def test_mset_atomicity_batch_consistency(self) -> bool:
        """Test that MSET is atomic (all or nothing).
        
        Verifies that when MSET modifies multiple keys,
        either all modifications are applied, or none are.
        """
        print("\nTest: MSET atomicity (all or nothing)")
        
        # Setup: Set initial values for all keys
        keys = ["mset_atomic1", "mset_atomic2", "mset_atomic3"]
        initial_values = {k: f"initial_{k}" for k in keys}
        new_values = {k: f"new_{k}" for k in keys}
        
        write_node = self._get_random_node()
        
        # Set initial values
        print("  Setting initial values...")
        for key, value in initial_values.items():
            write_node.set(key, value)
        
        # Verify initial state
        for key, expected in initial_values.items():
            value = write_node.get(key)
            if value != expected:
                print(f"  FAILED: Initial setup failed for '{key}'")
                return False
        
        # MSET all keys at once
        print("  MSET all keys...")
        try:
            result = write_node.mset(new_values)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: MSET failed - {e}")
            return False
        
        # Verify ALL keys were updated (not partial)
        print("  Verifying all keys updated...")
        all_updated = True
        for key, expected in new_values.items():
            value = write_node.get(key)
            if value != expected:
                print(f"    FAILED: Key '{key}' expected '{expected}', got '{value}'")
                all_updated = False
        
        if not all_updated:
            print("  FAILED: Not all keys were updated (partial update detected)")
            return False
        
        print("  All keys updated atomically: OK")
        print("  PASSED")
        return True
    
    def test_incr_invalid_value(self) -> bool:
        print("\nTest: INCR on non-integer string value")
        
        test_key = "incr_invalid_string_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = 'not_an_integer'...")
        try:
            write_node.set(test_key, "not_an_integer")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  INCR '{test_key}' (should fail)...")
        try:
            result = write_node.incr(test_key)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "not an integer" not in error_msg and "out of range" not in error_msg:
                print(f"  FAILED: Expected 'not an integer' error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        value = write_node.get(test_key)
        if value != "not_an_integer":
            print(f"  FAILED: Value was modified to '{value}', expected 'not_an_integer'")
            return False
        print("  Value unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_incr_wrong_type(self) -> bool:
        print("\nTest: INCR on hash key (wrong type)")
        
        test_key = "incr_wrong_type_key"
        write_node = self._get_random_node()
        
        print(f"  HSET '{test_key}' field 'value'...")
        try:
            write_node.hset(test_key, "field", "value")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        print(f"  INCR '{test_key}' (should fail with WRONGTYPE)...")
        try:
            result = write_node.incr(test_key)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).upper()
            if "WRONGTYPE" not in error_msg:
                print(f"  FAILED: Expected WRONGTYPE error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        result = write_node.hget(test_key, "field")
        if result != "value":
            print(f"  FAILED: Hash was modified, field value is '{result}'")
            return False
        print("  Hash unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_incrby_basic(self) -> bool:
        print("\nTest: INCRBY basic operation")
        
        test_key = "incrby_basic_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  INCRBY '{test_key}' 10 (non-existent key)...")
        try:
            result = write_node.incrby(test_key, 10)
            if result != 10:
                print(f"  FAILED: Expected 10, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: INCRBY failed - {e}")
            return False
        print("  Result: 10 (OK)")
        
        print(f"  INCRBY '{test_key}' 5...")
        try:
            result = write_node.incrby(test_key, 5)
            if result != 15:
                print(f"  FAILED: Expected 15, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: INCRBY failed - {e}")
            return False
        print("  Result: 15 (OK)")
        
        print("  PASSED")
        return True
    
    def test_incrby_nonexistent_key(self) -> bool:
        print("\nTest: INCRBY on non-existent key")
        
        test_key = "incrby_nonexistent_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  INCRBY '{test_key}' 100 (non-existent key)...")
        try:
            result = write_node.incrby(test_key, 100)
            if result != 100:
                print(f"  FAILED: Expected 100, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: INCRBY failed - {e}")
            return False
        print("  Result: 100 (started from 0, OK)")
        
        value = write_node.get(test_key)
        if value != "100":
            print(f"  FAILED: Key value is '{value}', expected '100'")
            return False
        print("  Key created with value '100': OK")
        
        print("  PASSED")
        return True
    
    def test_incrby_negative(self) -> bool:
        print("\nTest: INCRBY with negative increment")
        
        test_key = "incrby_negative_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '20'...")
        write_node.set(test_key, "20")
        
        print(f"  INCRBY '{test_key}' -5...")
        try:
            result = write_node.incrby(test_key, -5)
            if result != 15:
                print(f"  FAILED: Expected 15, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: INCRBY failed - {e}")
            return False
        print("  Result: 15 (OK)")
        
        print(f"  INCRBY '{test_key}' -20...")
        try:
            result = write_node.incrby(test_key, -20)
            if result != -5:
                print(f"  FAILED: Expected -5, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: INCRBY failed - {e}")
            return False
        print("  Result: -5 (OK)")
        
        print("  PASSED")
        return True
    
    def test_incrby_zero(self) -> bool:
        print("\nTest: INCRBY with zero increment")
        
        test_key = "incrby_zero_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '42'...")
        write_node.set(test_key, "42")
        
        print(f"  INCRBY '{test_key}' 0...")
        try:
            result = write_node.incrby(test_key, 0)
            if result != 42:
                print(f"  FAILED: Expected 42, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: INCRBY failed - {e}")
            return False
        print("  Result: 42 (unchanged, OK)")
        
        print("  PASSED")
        return True
    
    def test_incrby_overflow(self) -> bool:
        print("\nTest: INCRBY overflow detection")
        
        test_key = "incrby_overflow_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '9223372036854775806' (i64::MAX - 1)...")
        write_node.set(test_key, "9223372036854775806")
        
        print(f"  INCRBY '{test_key}' 5 (should overflow)...")
        try:
            result = write_node.incrby(test_key, 5)
            print(f"  FAILED: Expected overflow error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "overflow" not in error_msg:
                print(f"  FAILED: Expected overflow error, got: {e}")
                return False
            print(f"  Got expected overflow error: {e}")
        
        value = write_node.get(test_key)
        if value != "9223372036854775806":
            print(f"  FAILED: Value was modified to '{value}'")
            return False
        print("  Value unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_incrby_invalid_value(self) -> bool:
        print("\nTest: INCRBY on non-integer string value")
        
        test_key = "incrby_invalid_string_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = 'not_a_number'...")
        write_node.set(test_key, "not_a_number")
        
        print(f"  INCRBY '{test_key}' 5 (should fail)...")
        try:
            result = write_node.incrby(test_key, 5)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "not an integer" not in error_msg and "out of range" not in error_msg:
                print(f"  FAILED: Expected 'not an integer' error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        value = write_node.get(test_key)
        if value != "not_a_number":
            print(f"  FAILED: Value was modified to '{value}'")
            return False
        print("  Value unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_incrby_wrong_type(self) -> bool:
        print("\nTest: INCRBY on hash key (wrong type)")
        
        test_key = "incrby_wrong_type_key"
        write_node = self._get_random_node()
        
        print(f"  HSET '{test_key}' field 'value'...")
        try:
            write_node.hset(test_key, "field", "value")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        print(f"  INCRBY '{test_key}' 10 (should fail with WRONGTYPE)...")
        try:
            result = write_node.incrby(test_key, 10)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).upper()
            if "WRONGTYPE" not in error_msg:
                print(f"  FAILED: Expected WRONGTYPE error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        result = write_node.hget(test_key, "field")
        if result != "value":
            print(f"  FAILED: Hash was modified, field value is '{result}'")
            return False
        print("  Hash unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_incrby_replication(self) -> bool:
        print("\nTest: INCRBY replication")
        
        test_key = "incrby_repl_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  INCRBY '{test_key}' 100 on random node...")
        try:
            result = write_node.incrby(test_key, 100)
            if result != 100:
                print(f"  FAILED: Expected 100, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: INCRBY failed - {e}")
            return False
        
        print("  Verifying all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
                if value != "100":
                    print(f"    Node {i}: FAILED (expected '100', got '{value}')")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_incrby_large_values(self) -> bool:
        print("\nTest: INCRBY with large values")
        
        test_key = "incrby_large_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  INCRBY '{test_key}' 9223372036854775807 (i64::MAX)...")
        try:
            result = write_node.incrby(test_key, 9223372036854775807)
            if result != 9223372036854775807:
                print(f"  FAILED: Expected 9223372036854775807, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: INCRBY failed - {e}")
            return False
        print("  Result: 9223372036854775807 (OK)")
        
        write_node.delete(test_key)
        
        print(f"  INCRBY '{test_key}' -9223372036854775808 (i64::MIN)...")
        try:
            result = write_node.incrby(test_key, -9223372036854775808)
            if result != -9223372036854775808:
                print(f"  FAILED: Expected -9223372036854775808, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: INCRBY failed - {e}")
            return False
        print("  Result: -9223372036854775808 (OK)")
        
        print("  PASSED")
        return True
    
    def test_decr_basic(self) -> bool:
        """Test DECR basic operation."""
        print("\nTest: DECR basic operation")
        
        test_key = "decr_basic_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  DECR '{test_key}' (non-existent key)...")
        try:
            result = write_node.decr(test_key)
            if result != -1:
                print(f"  FAILED: Expected -1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECR failed - {e}")
            return False
        print("  Result: -1 (OK)")
        
        print(f"  DECR '{test_key}' again...")
        try:
            result = write_node.decr(test_key)
            if result != -2:
                print(f"  FAILED: Expected -2, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECR failed - {e}")
            return False
        print("  Result: -2 (OK)")
        
        print(f"  SET '{test_key}' = '10'...")
        try:
            write_node.set(test_key, "10")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECR '{test_key}'...")
        try:
            result = write_node.decr(test_key)
            if result != 9:
                print(f"  FAILED: Expected 9, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECR failed - {e}")
            return False
        print("  Result: 9 (OK)")
        
        print("  PASSED")
        return True
    
    def test_decr_nonexistent_key(self) -> bool:
        """Test DECR on non-existent key (should start at 0)."""
        print("\nTest: DECR on non-existent key")
        
        test_key = "decr_nonexistent_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  DECR '{test_key}' (non-existent)...")
        try:
            result = write_node.decr(test_key)
            if result != -1:
                print(f"  FAILED: Expected -1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECR failed - {e}")
            return False
        
        value = write_node.get(test_key)
        if value != "-1":
            print(f"  FAILED: Expected value '-1', got '{value}'")
            return False
        print("  Value is '-1': OK")
        
        print("  PASSED")
        return True
    
    def test_decr_negative_values(self) -> bool:
        """Test DECR with negative values."""
        print("\nTest: DECR with negative values")
        
        test_key = "decr_negative_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '-5'...")
        try:
            write_node.set(test_key, "-5")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECR '{test_key}'...")
        try:
            result = write_node.decr(test_key)
            if result != -6:
                print(f"  FAILED: Expected -6, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECR failed - {e}")
            return False
        print("  Result: -6 (OK)")
        
        print("  PASSED")
        return True
    
    def test_decr_zero(self) -> bool:
        """Test DECR on zero value."""
        print("\nTest: DECR on zero value")
        
        test_key = "decr_zero_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '0'...")
        try:
            write_node.set(test_key, "0")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECR '{test_key}'...")
        try:
            result = write_node.decr(test_key)
            if result != -1:
                print(f"  FAILED: Expected -1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECR failed - {e}")
            return False
        print("  Result: -1 (OK)")
        
        print("  PASSED")
        return True
    
    def test_decr_overflow(self) -> bool:
        """Test DECR overflow (i64::MIN - 1)."""
        print("\nTest: DECR overflow")
        
        test_key = "decr_overflow_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '-9223372036854775808' (i64::MIN)...")
        try:
            write_node.set(test_key, "-9223372036854775808")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECR '{test_key}' (should fail with overflow)...")
        try:
            result = write_node.decr(test_key)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "overflow" not in error_msg:
                print(f"  FAILED: Expected overflow error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        print("  PASSED")
        return True
    
    def test_decr_invalid_value(self) -> bool:
        """Test DECR on non-integer string value."""
        print("\nTest: DECR on non-integer string value")
        
        test_key = "decr_invalid_string_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = 'not_an_integer'...")
        try:
            write_node.set(test_key, "not_an_integer")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECR '{test_key}' (should fail)...")
        try:
            result = write_node.decr(test_key)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "not an integer" not in error_msg and "out of range" not in error_msg:
                print(f"  FAILED: Expected 'not an integer' error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        value = write_node.get(test_key)
        if value != "not_an_integer":
            print(f"  FAILED: Value was modified to '{value}', expected 'not_an_integer'")
            return False
        print("  Value unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_decr_wrong_type(self) -> bool:
        """Test DECR on hash key (wrong type)."""
        print("\nTest: DECR on hash key (wrong type)")
        
        test_key = "decr_wrong_type_key"
        write_node = self._get_random_node()
        
        print(f"  HSET '{test_key}' field 'value'...")
        try:
            write_node.hset(test_key, "field", "value")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        print(f"  DECR '{test_key}' (should fail with WRONGTYPE)...")
        try:
            result = write_node.decr(test_key)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).upper()
            if "WRONGTYPE" not in error_msg:
                print(f"  FAILED: Expected WRONGTYPE error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        result = write_node.hget(test_key, "field")
        if result != "value":
            print(f"  FAILED: Hash was modified, field value is '{result}'")
            return False
        print("  Hash unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_decr_replication(self) -> bool:
        """Test that DECR results are replicated to all nodes."""
        print("\nTest: DECR replication")
        
        test_key = "decr_repl_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  DECR '{test_key}' on random node...")
        try:
            result = write_node.decr(test_key)
            if result != -1:
                print(f"  FAILED: Expected -1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECR failed - {e}")
            return False
        
        print("  Verifying all nodes have value '-1'...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
                if value != "-1":
                    print(f"    Node {i}: FAILED (expected '-1', got '{value}')")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_decr_large_values(self) -> bool:
        """Test DECR with large values."""
        print("\nTest: DECR with large values")
        
        test_key = "decr_large_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  SET '{test_key}' = '9223372036854775807' (i64::MAX)...")
        try:
            write_node.set(test_key, "9223372036854775807")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECR '{test_key}'...")
        try:
            result = write_node.decr(test_key)
            if result != 9223372036854775806:
                print(f"  FAILED: Expected 9223372036854775806, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECR failed - {e}")
            return False
        print("  Result: 9223372036854775806 (OK)")
        
        print("  PASSED")
        return True
    
    def test_decrby_basic(self) -> bool:
        """Test DECRBY basic operation."""
        print("\nTest: DECRBY basic operation")
        
        test_key = "decrby_basic_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  DECRBY '{test_key}' 10 (non-existent key)...")
        try:
            result = write_node.decrby(test_key, 10)
            if result != -10:
                print(f"  FAILED: Expected -10, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECRBY failed - {e}")
            return False
        print("  Result: -10 (OK)")
        
        print(f"  DECRBY '{test_key}' 5...")
        try:
            result = write_node.decrby(test_key, 5)
            if result != -15:
                print(f"  FAILED: Expected -15, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECRBY failed - {e}")
            return False
        print("  Result: -15 (OK)")
        
        print(f"  SET '{test_key}' = '100'...")
        try:
            write_node.set(test_key, "100")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECRBY '{test_key}' 30...")
        try:
            result = write_node.decrby(test_key, 30)
            if result != 70:
                print(f"  FAILED: Expected 70, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECRBY failed - {e}")
            return False
        print("  Result: 70 (OK)")
        
        print("  PASSED")
        return True
    
    def test_decrby_nonexistent_key(self) -> bool:
        """Test DECRBY on non-existent key (should start at 0)."""
        print("\nTest: DECRBY on non-existent key")
        
        test_key = "decrby_nonexistent_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  DECRBY '{test_key}' 5 (non-existent)...")
        try:
            result = write_node.decrby(test_key, 5)
            if result != -5:
                print(f"  FAILED: Expected -5, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECRBY failed - {e}")
            return False
        
        value = write_node.get(test_key)
        if value != "-5":
            print(f"  FAILED: Expected value '-5', got '{value}'")
            return False
        print("  Value is '-5': OK")
        
        print("  PASSED")
        return True
    
    def test_decrby_negative(self) -> bool:
        """Test DECRBY with negative decrement (should increment)."""
        print("\nTest: DECRBY with negative decrement")
        
        test_key = "decrby_negative_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '10'...")
        try:
            write_node.set(test_key, "10")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECRBY '{test_key}' -5 (negative decrement)...")
        try:
            result = write_node.decrby(test_key, -5)
            if result != 15:
                print(f"  FAILED: Expected 15, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECRBY failed - {e}")
            return False
        print("  Result: 15 (OK - negative decrement increments)")
        
        print("  PASSED")
        return True
    
    def test_decrby_zero(self) -> bool:
        """Test DECRBY with zero decrement."""
        print("\nTest: DECRBY with zero decrement")
        
        test_key = "decrby_zero_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '42'...")
        try:
            write_node.set(test_key, "42")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECRBY '{test_key}' 0...")
        try:
            result = write_node.decrby(test_key, 0)
            if result != 42:
                print(f"  FAILED: Expected 42, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECRBY failed - {e}")
            return False
        print("  Result: 42 (OK - unchanged)")
        
        print("  PASSED")
        return True
    
    def test_decrby_overflow(self) -> bool:
        """Test DECRBY overflow."""
        print("\nTest: DECRBY overflow")
        
        test_key = "decrby_overflow_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '-9223372036854775808' (i64::MIN)...")
        try:
            write_node.set(test_key, "-9223372036854775808")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECRBY '{test_key}' 1 (should fail with overflow)...")
        try:
            result = write_node.decrby(test_key, 1)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "overflow" not in error_msg:
                print(f"  FAILED: Expected overflow error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        print("  PASSED")
        return True
    
    def test_decrby_invalid_value(self) -> bool:
        """Test DECRBY on non-integer string value."""
        print("\nTest: DECRBY on non-integer string value")
        
        test_key = "decrby_invalid_string_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = 'not_a_number'...")
        try:
            write_node.set(test_key, "not_a_number")
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        print(f"  DECRBY '{test_key}' 5 (should fail)...")
        try:
            result = write_node.decrby(test_key, 5)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "not an integer" not in error_msg and "out of range" not in error_msg:
                print(f"  FAILED: Expected 'not an integer' error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        value = write_node.get(test_key)
        if value != "not_a_number":
            print(f"  FAILED: Value was modified to '{value}'")
            return False
        print("  Value unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_decrby_wrong_type(self) -> bool:
        """Test DECRBY on hash key (wrong type)."""
        print("\nTest: DECRBY on hash key (wrong type)")
        
        test_key = "decrby_wrong_type_key"
        write_node = self._get_random_node()
        
        print(f"  HSET '{test_key}' field 'value'...")
        try:
            write_node.hset(test_key, "field", "value")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        print(f"  DECRBY '{test_key}' 10 (should fail with WRONGTYPE)...")
        try:
            result = write_node.decrby(test_key, 10)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).upper()
            if "WRONGTYPE" not in error_msg:
                print(f"  FAILED: Expected WRONGTYPE error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        result = write_node.hget(test_key, "field")
        if result != "value":
            print(f"  FAILED: Hash was modified, field value is '{result}'")
            return False
        print("  Hash unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_decrby_replication(self) -> bool:
        """Test that DECRBY results are replicated to all nodes."""
        print("\nTest: DECRBY replication")
        
        test_key = "decrby_repl_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  DECRBY '{test_key}' 7 on random node...")
        try:
            result = write_node.decrby(test_key, 7)
            if result != -7:
                print(f"  FAILED: Expected -7, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECRBY failed - {e}")
            return False
        
        print("  Verifying all nodes have value '-7'...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
                if value != "-7":
                    print(f"    Node {i}: FAILED (expected '-7', got '{value}')")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_decrby_large_values(self) -> bool:
        """Test DECRBY with large values."""
        print("\nTest: DECRBY with large values")
        
        test_key = "decrby_large_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  DECRBY '{test_key}' 9223372036854775807 (i64::MAX)...")
        try:
            result = write_node.decrby(test_key, 9223372036854775807)
            if result != -9223372036854775807:
                print(f"  FAILED: Expected -9223372036854775807, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECRBY failed - {e}")
            return False
        print("  Result: -9223372036854775807 (OK)")
        
        write_node.delete(test_key)
        
        # Test negative decrement (increment) with value just below overflow
        # 0 - (-9223372036854775807) = 9223372036854775807 (i64::MAX, valid)
        print(f"  DECRBY '{test_key}' -9223372036854775807 (i64::MIN + 1, negative)...")
        try:
            result = write_node.decrby(test_key, -9223372036854775807)
            if result != 9223372036854775807:
                print(f"  FAILED: Expected 9223372036854775807, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: DECRBY failed - {e}")
            return False
        print("  Result: 9223372036854775807 (OK)")
        
        print("  PASSED")
        return True
    
    def test_append_new_key(self) -> bool:
        """Test APPEND on a non-existent key (should create and return length)."""
        print("\nTest: APPEND on non-existent key")
        
        test_key = "append_new_key"
        test_value = "hello"
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        print(f"  APPEND '{test_key}' '{test_value}' (non-existent key)...")
        try:
            result = write_node.append(test_key, test_value)
            if result != len(test_value):
                print(f"  FAILED: Expected {len(test_value)}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: APPEND failed - {e}")
            return False
        print(f"  Result: {result} (OK)")
        
        # Verify value was created
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Expected '{test_value}', got '{value}'")
            return False
        print(f"  Value is '{value}': OK")
        
        print("  PASSED")
        return True
    
    def test_append_existing_key(self) -> bool:
        """Test APPEND on an existing key."""
        print("\nTest: APPEND on existing key")
        
        test_key = "append_existing_key"
        initial_value = "hello"
        append_value = " world"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        print(f"  APPEND '{test_key}' '{append_value}'...")
        try:
            result = write_node.append(test_key, append_value)
            expected_len = len(initial_value) + len(append_value)
            if result != expected_len:
                print(f"  FAILED: Expected {expected_len}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: APPEND failed - {e}")
            return False
        print(f"  Result: {result} (OK)")
        
        # Verify value was appended
        expected_value = initial_value + append_value
        value = write_node.get(test_key)
        if value != expected_value:
            print(f"  FAILED: Expected '{expected_value}', got '{value}'")
            return False
        print(f"  Value is '{value}': OK")
        
        print("  PASSED")
        return True
    
    def test_append_empty_string(self) -> bool:
        """Test APPEND with empty string."""
        print("\nTest: APPEND with empty string")
        
        test_key = "append_empty_key"
        initial_value = "hello"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        print(f"  APPEND '{test_key}' '' (empty string)...")
        try:
            result = write_node.append(test_key, "")
            if result != len(initial_value):
                print(f"  FAILED: Expected {len(initial_value)}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: APPEND failed - {e}")
            return False
        print(f"  Result: {result} (unchanged, OK)")
        
        # Verify value is unchanged
        value = write_node.get(test_key)
        if value != initial_value:
            print(f"  FAILED: Expected '{initial_value}', got '{value}'")
            return False
        print(f"  Value unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_append_wrong_type(self) -> bool:
        """Test APPEND on hash key (wrong type)."""
        print("\nTest: APPEND on hash key (wrong type)")
        
        test_key = "append_wrong_type_key"
        write_node = self._get_random_node()
        
        print(f"  HSET '{test_key}' field 'value'...")
        try:
            write_node.hset(test_key, "field", "value")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        print(f"  APPEND '{test_key}' 'data' (should fail with WRONGTYPE)...")
        try:
            result = write_node.append(test_key, "data")
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).upper()
            if "WRONGTYPE" not in error_msg:
                print(f"  FAILED: Expected WRONGTYPE error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        result = write_node.hget(test_key, "field")
        if result != "value":
            print(f"  FAILED: Hash was modified, field value is '{result}'")
            return False
        print("  Hash unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_append_replication(self) -> bool:
        """Test that APPEND results are replicated to all nodes."""
        print("\nTest: APPEND replication")
        
        test_key = "append_repl_key"
        write_node = self._get_random_node()
        
        write_node.delete(test_key)
        
        print(f"  APPEND '{test_key}' 'hello' on random node...")
        try:
            result = write_node.append(test_key, "hello")
            if result != 5:
                print(f"  FAILED: Expected 5, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: APPEND failed - {e}")
            return False
        
        print(f"  APPEND '{test_key}' ' world'...")
        try:
            result = write_node.append(test_key, " world")
            if result != 11:
                print(f"  FAILED: Expected 11, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: APPEND failed - {e}")
            return False
        
        print("  Verifying all nodes have value 'hello world'...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
                if value != "hello world":
                    print(f"    Node {i}: FAILED (expected 'hello world', got '{value}')")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_append_preserves_expiration(self) -> bool:
        """Test that APPEND preserves expiration time."""
        print("\nTest: APPEND preserves expiration")
        
        test_key = "append_ttl_key"
        initial_value = "hello"
        append_value = " world"
        write_node = self._get_random_node()
        
        # Set with 1 second TTL
        print(f"  SET '{test_key}' = '{initial_value}' with 1000ms TTL...")
        write_node.set(test_key, initial_value, px=1000)
        
        # Append immediately
        print(f"  APPEND '{test_key}' '{append_value}'...")
        try:
            result = write_node.append(test_key, append_value)
            if result != len(initial_value + append_value):
                print(f"  FAILED: Expected {len(initial_value + append_value)}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: APPEND failed - {e}")
            return False
        
        # Verify value was appended
        value = write_node.get(test_key)
        if value != initial_value + append_value:
            print(f"  FAILED: Expected '{initial_value + append_value}', got '{value}'")
            return False
        print(f"  Value appended: OK")
        
        # Wait for expiration
        print("  Waiting for expiration (1.1s)...")
        time.sleep(1.1)
        
        # Verify key expired
        value = write_node.get(test_key)
        if value is not None:
            print(f"  FAILED: Key should have expired but got '{value}'")
            return False
        print("  Key expired correctly: OK")
        
        print("  PASSED")
        return True
    
    def test_strlen_existing_key(self) -> bool:
        """Test STRLEN on an existing key."""
        print("\nTest: STRLEN on existing key")
        
        test_key = "strlen_existing_key"
        test_value = "Hello World"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        print(f"  STRLEN '{test_key}'...")
        try:
            result = write_node.strlen(test_key)
            if result != len(test_value):
                print(f"  FAILED: Expected {len(test_value)}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: STRLEN failed - {e}")
            return False
        print(f"  Result: {result} (OK)")
        
        print("  PASSED")
        return True
    
    def test_strlen_nonexistent_key(self) -> bool:
        """Test STRLEN on a non-existent key (should return 0)."""
        print("\nTest: STRLEN on non-existent key")
        
        test_key = "strlen_nonexistent_key"
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        print(f"  STRLEN '{test_key}' (non-existent)...")
        try:
            result = write_node.strlen(test_key)
            if result != 0:
                print(f"  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: STRLEN failed - {e}")
            return False
        print(f"  Result: 0 (OK)")
        
        print("  PASSED")
        return True
    
    def test_strlen_empty_string(self) -> bool:
        """Test STRLEN on an empty string."""
        print("\nTest: STRLEN on empty string")
        
        test_key = "strlen_empty_key"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '' (empty string)...")
        write_node.set(test_key, "")
        
        print(f"  STRLEN '{test_key}'...")
        try:
            result = write_node.strlen(test_key)
            if result != 0:
                print(f"  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: STRLEN failed - {e}")
            return False
        print(f"  Result: 0 (OK)")
        
        print("  PASSED")
        return True
    
    def test_strlen_binary_data(self) -> bool:
        """Test STRLEN on binary data."""
        print("\nTest: STRLEN on binary data")
        
        test_key = "strlen_binary_key"
        # Binary data with null bytes and special characters
        test_value = b"\x00\x01\x02\xff\xfe\xfd"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = <binary data> ({len(test_value)} bytes)...")
        write_node.set(test_key, test_value)
        
        print(f"  STRLEN '{test_key}'...")
        try:
            result = write_node.strlen(test_key)
            if result != len(test_value):
                print(f"  FAILED: Expected {len(test_value)}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: STRLEN failed - {e}")
            return False
        print(f"  Result: {result} (OK)")
        
        print("  PASSED")
        return True
    
    def test_strlen_unicode(self) -> bool:
        """Test STRLEN on Unicode strings (counts bytes, not characters)."""
        print("\nTest: STRLEN on Unicode strings")
        
        test_key = "strlen_unicode_key"
        # UTF-8: each Chinese character is 3 bytes
        test_value = "你好世界"  # 4 characters, 12 bytes in UTF-8
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '{test_value}' ({len(test_value)} chars, {len(test_value.encode('utf-8'))} bytes)...")
        write_node.set(test_key, test_value)
        
        print(f"  STRLEN '{test_key}'...")
        try:
            result = write_node.strlen(test_key)
            expected_bytes = len(test_value.encode('utf-8'))
            if result != expected_bytes:
                print(f"  FAILED: Expected {expected_bytes} bytes, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: STRLEN failed - {e}")
            return False
        print(f"  Result: {result} bytes (OK)")
        
        print("  PASSED")
        return True
    
    def test_strlen_large_value(self) -> bool:
        """Test STRLEN on a large value."""
        print("\nTest: STRLEN on large value")
        
        test_key = "strlen_large_key"
        # Create a 1MB value
        test_value = "x" * (1024 * 1024)
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = <1MB value>...")
        write_node.set(test_key, test_value)
        
        print(f"  STRLEN '{test_key}'...")
        try:
            result = write_node.strlen(test_key)
            if result != len(test_value):
                print(f"  FAILED: Expected {len(test_value)}, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: STRLEN failed - {e}")
            return False
        print(f"  Result: {result} (OK)")
        
        print("  PASSED")
        return True
    
    def test_strlen_replication(self) -> bool:
        """Test that STRLEN works on all nodes after write."""
        print("\nTest: STRLEN replication")
        
        test_key = "strlen_repl_key"
        test_value = "replicated_value"
        write_node = self._get_random_node()
        
        print(f"  SET '{test_key}' = '{test_value}' on random node...")
        write_node.set(test_key, test_value)
        
        print("  STRLEN from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.strlen(test_key)
                if result != len(test_value):
                    print(f"    Node {i}: FAILED (expected {len(test_value)}, got {result})")
                    return False
                print(f"    Node {i}: OK ({result})")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_strlen_wrong_type(self) -> bool:
        """Test STRLEN on hash key (wrong type)."""
        print("\nTest: STRLEN on hash key (wrong type)")
        
        test_key = "strlen_wrong_type_key"
        write_node = self._get_random_node()
        
        print(f"  HSET '{test_key}' field 'value'...")
        try:
            write_node.hset(test_key, "field", "value")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        print(f"  STRLEN '{test_key}' (should fail with WRONGTYPE)...")
        try:
            result = write_node.strlen(test_key)
            print(f"  FAILED: Expected error but got result: {result}")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).upper()
            if "WRONGTYPE" not in error_msg:
                print(f"  FAILED: Expected WRONGTYPE error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        result = write_node.hget(test_key, "field")
        if result != "value":
            print(f"  FAILED: Hash was modified, field value is '{result}'")
            return False
        print("  Hash unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_type_string_key(self) -> bool:
        """Test TYPE command on string key."""
        print("\nTest: TYPE on string key")
        
        test_key = "type_string_key"
        test_value = "type_string_value"
        
        write_node = self._get_random_node()
        
        # Set a string key
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        # TYPE should return "string"
        print(f"  TYPE '{test_key}'...")
        try:
            result = write_node.type(test_key)
            if result != "string":
                print(f"  FAILED: Expected 'string', got '{result}'")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: TYPE failed - {e}")
            return False
        print(f"  TYPE returned 'string': OK")
        
        print("  PASSED")
        return True
    
    def test_type_hash_key(self) -> bool:
        """Test TYPE command on hash key."""
        print("\nTest: TYPE on hash key")
        
        test_key = "type_hash_key"
        
        write_node = self._get_random_node()
        
        # Set a hash key
        print(f"  HSET '{test_key}' field 'value'...")
        try:
            write_node.hset(test_key, "field", "value")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # TYPE should return "hash"
        print(f"  TYPE '{test_key}'...")
        try:
            result = write_node.type(test_key)
            if result != "hash":
                print(f"  FAILED: Expected 'hash', got '{result}'")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: TYPE failed - {e}")
            return False
        print(f"  TYPE returned 'hash': OK")
        
        print("  PASSED")
        return True
    
    def test_type_nonexistent_key(self) -> bool:
        """Test TYPE command on non-existent key."""
        print("\nTest: TYPE on non-existent key")
        
        test_key = "type_nonexistent_key"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # TYPE should return "none"
        print(f"  TYPE '{test_key}' (non-existent)...")
        try:
            result = write_node.type(test_key)
            if result != "none":
                print(f"  FAILED: Expected 'none', got '{result}'")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: TYPE failed - {e}")
            return False
        print(f"  TYPE returned 'none': OK")
        
        print("  PASSED")
        return True
    
    def test_type_after_del(self) -> bool:
        """Test TYPE returns 'none' after DEL."""
        print("\nTest: TYPE after DEL")
        
        test_key = "type_after_del_key"
        
        write_node = self._get_random_node()
        
        # Set a string key
        print(f"  SET '{test_key}'...")
        write_node.set(test_key, "value")
        
        # Verify TYPE returns "string"
        result = write_node.type(test_key)
        if result != "string":
            print(f"  FAILED: Key should have type 'string' before DEL, got '{result}'")
            return False
        
        # Delete the key
        print(f"  DEL '{test_key}'...")
        write_node.delete(test_key)
        
        # TYPE should return "none"
        print(f"  TYPE '{test_key}' after DEL...")
        try:
            result = write_node.type(test_key)
            if result != "none":
                print(f"  FAILED: Expected 'none' after DEL, got '{result}'")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: TYPE failed - {e}")
            return False
        print("  TYPE returned 'none' after DEL: OK")
        
        print("  PASSED")
        return True
    
    def test_type_replication(self) -> bool:
        """Test that TYPE works on all nodes."""
        print("\nTest: TYPE replication")
        
        test_key = "type_repl_key"
        test_value = "type_repl_value"
        
        write_node = self._get_random_node()
        
        # Set a string key
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        # TYPE should return "string" on all nodes
        print("  TYPE on all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                result = node.conn.type(test_key)
                if result != "string":
                    print(f"    Node {i}: FAILED (expected 'string', got '{result}')")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_type_expired_key(self) -> bool:
        """Test TYPE on expired key returns 'none'."""
        print("\nTest: TYPE on expired key")
        
        test_key = "type_expired_key"
        test_value = "will_expire"
        
        write_node = self._get_random_node()
        
        # SET with 500ms expiration
        print(f"  SET '{test_key}' with 500ms TTL...")
        write_node.set(test_key, test_value, px=500)
        
        # Verify TYPE returns "string" immediately
        result = write_node.type(test_key)
        if result != "string":
            print(f"  FAILED: Expected 'string' before expiration, got '{result}'")
            return False
        print("  TYPE returned 'string' before expiration: OK")
        
        # Wait for expiration
        print("  Waiting for expiration...")
        time.sleep(1)
        
        # TYPE should return "none" after expiration
        print(f"  TYPE '{test_key}' after expiration...")
        try:
            result = write_node.type(test_key)
            if result != "none":
                print(f"  FAILED: Expected 'none' after expiration, got '{result}'")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: TYPE failed - {e}")
            return False
        print("  TYPE returned 'none' after expiration: OK")
        
        print("  PASSED")
        return True
    
    def test_setex_basic(self) -> bool:
        """Test basic SETEX operation."""
        print("\nTest: SETEX basic operation")
        
        test_key = "setex_basic_key"
        test_value = "setex_value"
        
        write_node = self._get_random_node()
        
        # SETEX with 10 seconds expiration
        print(f"  SETEX '{test_key}' 10 '{test_value}'...")
        try:
            result = write_node.setex(test_key, 10, test_value)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SETEX failed - {e}")
            return False
        
        # Verify value is readable immediately
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Expected '{test_value}', got '{value}'")
            return False
        print("  Value readable: OK")
        
        print("  PASSED")
        return True
    
    def test_setex_expiration(self) -> bool:
        """Test that SETEX key expires after specified time."""
        print("\nTest: SETEX expiration")
        
        test_key = "setex_exp_key"
        test_value = "will_expire"
        
        write_node = self._get_random_node()
        
        # SETEX with 1 second expiration
        print(f"  SETEX '{test_key}' 1 '{test_value}'...")
        try:
            result = write_node.setex(test_key, 1, test_value)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SETEX failed - {e}")
            return False
        
        # Verify value is readable immediately
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Expected '{test_value}', got '{value}'")
            return False
        print("  Value readable immediately: OK")
        
        # Wait for expiration
        print("  Waiting for expiration (1.5s)...")
        time.sleep(1.5)
        
        # Verify key expired
        value = write_node.get(test_key)
        if value is not None:
            print(f"  FAILED: Key should have expired but got '{value}'")
            return False
        print("  Key expired correctly: OK")
        
        print("  PASSED")
        return True
    
    def test_setex_overwrite(self) -> bool:
        """Test that SETEX overwrites existing key."""
        print("\nTest: SETEX overwrites existing key")
        
        test_key = "setex_overwrite_key"
        initial_value = "initial_value"
        new_value = "new_value"
        
        write_node = self._get_random_node()
        
        # Set initial value without expiration
        print(f"  SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        # Verify initial value
        value = write_node.get(test_key)
        if value != initial_value:
            print(f"  FAILED: Initial value not set correctly")
            return False
        
        # SETEX to overwrite with expiration
        print(f"  SETEX '{test_key}' 10 '{new_value}'...")
        try:
            result = write_node.setex(test_key, 10, new_value)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SETEX failed - {e}")
            return False
        
        # Verify value was overwritten
        value = write_node.get(test_key)
        if value != new_value:
            print(f"  FAILED: Expected '{new_value}', got '{value}'")
            return False
        print("  Value overwritten: OK")
        
        print("  PASSED")
        return True
    
    def test_setex_replication(self) -> bool:
        """Test that SETEX data is replicated to all nodes."""
        print("\nTest: SETEX replication")
        
        test_key = "setex_repl_key"
        test_value = "setex_repl_value"
        
        write_node = self._get_random_node()
        
        # SETEX on random node
        print(f"  SETEX '{test_key}' 10 '{test_value}' on random node...")
        try:
            result = write_node.setex(test_key, 10, test_value)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SETEX failed - {e}")
            return False
        
        # Verify all nodes have the data
        print("  Verifying all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
                if value != test_value:
                    print(f"    Node {i}: FAILED (expected '{test_value}', got '{value}')")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_setex_equivalent_to_set_ex(self) -> bool:
        """Test that SETEX is equivalent to SET with EX option."""
        print("\nTest: SETEX equivalent to SET EX")
        
        setex_key = "setex_eq_key"
        set_key = "set_ex_eq_key"
        test_value = "test_value"
        
        write_node = self._get_random_node()
        
        # SETEX
        print(f"  SETEX '{setex_key}' 10 '{test_value}'...")
        try:
            result = write_node.setex(setex_key, 10, test_value)
            if result is not True:
                print(f"  FAILED: SETEX failed")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SETEX failed - {e}")
            return False
        
        # SET with EX
        print(f"  SET '{set_key}' '{test_value}' EX 10...")
        try:
            result = write_node.set(set_key, test_value, ex=10)
            if result is not True:
                print(f"  FAILED: SET EX failed")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET EX failed - {e}")
            return False
        
        # Both should have same value
        setex_value = write_node.get(setex_key)
        set_value = write_node.get(set_key)
        
        if setex_value != set_value:
            print(f"  FAILED: Values differ - SETEX: '{setex_value}', SET EX: '{set_value}'")
            return False
        print("  Both commands set same value: OK")
        
        print("  PASSED")
        return True
    
    def test_setex_wrong_args(self) -> bool:
        """Test SETEX with wrong number of arguments."""
        print("\nTest: SETEX wrong arguments")
        
        write_node = self._get_random_node()
        
        # Too few arguments
        print("  Testing with too few arguments...")
        try:
            # Use execute_command to bypass redis-py validation
            write_node.execute_command("SETEX", "key1")
            print("  FAILED: Expected error for too few arguments")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "wrong number" not in error_msg:
                print(f"  FAILED: Expected 'wrong number' error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        print("  PASSED")
        return True
    
    def test_psetex_basic(self) -> bool:
        """Test basic PSETEX operation."""
        print("\nTest: PSETEX basic operation")
        
        test_key = "psetex_basic_key"
        test_value = "psetex_value"
        
        write_node = self._get_random_node()
        
        # PSETEX with 10000 milliseconds (10 seconds) expiration
        print(f"  PSETEX '{test_key}' 10000 '{test_value}'...")
        try:
            result = write_node.psetex(test_key, 10000, test_value)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: PSETEX failed - {e}")
            return False
        
        # Verify value is readable immediately
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Expected '{test_value}', got '{value}'")
            return False
        print("  Value readable: OK")
        
        print("  PASSED")
        return True
    
    def test_psetex_expiration(self) -> bool:
        """Test that PSETEX key expires after specified time."""
        print("\nTest: PSETEX expiration")
        
        test_key = "psetex_exp_key"
        test_value = "will_expire"
        
        write_node = self._get_random_node()
        
        # PSETEX with 500 milliseconds expiration
        print(f"  PSETEX '{test_key}' 500 '{test_value}'...")
        try:
            result = write_node.psetex(test_key, 500, test_value)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: PSETEX failed - {e}")
            return False
        
        # Verify value is readable immediately
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Expected '{test_value}', got '{value}'")
            return False
        print("  Value readable immediately: OK")
        
        # Wait for expiration
        print("  Waiting for expiration (1s)...")
        time.sleep(1)
        
        # Verify key expired
        value = write_node.get(test_key)
        if value is not None:
            print(f"  FAILED: Key should have expired but got '{value}'")
            return False
        print("  Key expired correctly: OK")
        
        print("  PASSED")
        return True
    
    def test_psetex_overwrite(self) -> bool:
        """Test that PSETEX overwrites existing key."""
        print("\nTest: PSETEX overwrites existing key")
        
        test_key = "psetex_overwrite_key"
        initial_value = "initial_value"
        new_value = "new_value"
        
        write_node = self._get_random_node()
        
        # Set initial value without expiration
        print(f"  SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        # Verify initial value
        value = write_node.get(test_key)
        if value != initial_value:
            print(f"  FAILED: Initial value not set correctly")
            return False
        
        # PSETEX to overwrite with expiration
        print(f"  PSETEX '{test_key}' 10000 '{new_value}'...")
        try:
            result = write_node.psetex(test_key, 10000, new_value)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: PSETEX failed - {e}")
            return False
        
        # Verify value was overwritten
        value = write_node.get(test_key)
        if value != new_value:
            print(f"  FAILED: Expected '{new_value}', got '{value}'")
            return False
        print("  Value overwritten: OK")
        
        print("  PASSED")
        return True
    
    def test_psetex_replication(self) -> bool:
        """Test that PSETEX data is replicated to all nodes."""
        print("\nTest: PSETEX replication")
        
        test_key = "psetex_repl_key"
        test_value = "psetex_repl_value"
        
        write_node = self._get_random_node()
        
        # PSETEX on random node
        print(f"  PSETEX '{test_key}' 10000 '{test_value}' on random node...")
        try:
            result = write_node.psetex(test_key, 10000, test_value)
            if result is not True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: PSETEX failed - {e}")
            return False
        
        # Verify all nodes have the data
        print("  Verifying all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
                if value != test_value:
                    print(f"    Node {i}: FAILED (expected '{test_value}', got '{value}')")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_psetex_equivalent_to_set_px(self) -> bool:
        """Test that PSETEX is equivalent to SET with PX option."""
        print("\nTest: PSETEX equivalent to SET PX")
        
        psetex_key = "psetex_eq_key"
        set_key = "set_px_eq_key"
        test_value = "test_value"
        
        write_node = self._get_random_node()
        
        # PSETEX with 10000ms
        print(f"  PSETEX '{psetex_key}' 10000 '{test_value}'...")
        try:
            result = write_node.psetex(psetex_key, 10000, test_value)
            if result is not True:
                print(f"  FAILED: PSETEX failed")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: PSETEX failed - {e}")
            return False
        
        # SET with PX (milliseconds)
        print(f"  SET '{set_key}' '{test_value}' PX 10000...")
        try:
            result = write_node.set(set_key, test_value, px=10000)
            if result is not True:
                print(f"  FAILED: SET PX failed")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SET PX failed - {e}")
            return False
        
        # Both should have same value
        psetex_value = write_node.get(psetex_key)
        set_value = write_node.get(set_key)
        
        if psetex_value != set_value:
            print(f"  FAILED: Values differ - PSETEX: '{psetex_value}', SET PX: '{set_value}'")
            return False
        print("  Both commands set same value: OK")
        
        print("  PASSED")
        return True
    
    def test_psetex_wrong_args(self) -> bool:
        """Test PSETEX with wrong number of arguments."""
        print("\nTest: PSETEX wrong arguments")
        
        write_node = self._get_random_node()
        
        # Too few arguments
        print("  Testing with too few arguments...")
        try:
            # Use execute_command to bypass redis-py validation
            write_node.execute_command("PSETEX", "key1")
            print("  FAILED: Expected error for too few arguments")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "wrong number" not in error_msg:
                print(f"  FAILED: Expected 'wrong number' error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        print("  PASSED")
        return True
    
    def test_setnx_new_key(self) -> bool:
        """Test SETNX on a new key (should return 1)."""
        print("\nTest: SETNX on new key")
        
        test_key = "setnx_new_key"
        test_value = "setnx_value"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # SETNX on new key should return 1 (success)
        print(f"  SETNX '{test_key}' = '{test_value}'...")
        try:
            result = write_node.setnx(test_key, test_value)
            if result != 1:
                print(f"  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SETNX failed - {e}")
            return False
        
        # Verify value was set
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key not set, got '{value}'")
            return False
        print("  Key set successfully: OK")
        
        print("  PASSED")
        return True
    
    def test_setnx_existing_key(self) -> bool:
        """Test SETNX on existing key (should return 0)."""
        print("\nTest: SETNX on existing key")
        
        test_key = "setnx_existing_key"
        initial_value = "initial_value"
        new_value = "new_value"
        
        write_node = self._get_random_node()
        
        # First set the key
        print(f"  Initial SET '{test_key}' = '{initial_value}'...")
        write_node.set(test_key, initial_value)
        
        # Verify initial value is readable
        value = write_node.get(test_key)
        if value != initial_value:
            print(f"  FAILED: Initial value not readable")
            return False
        
        # SETNX on existing key should return 0 (not set)
        print(f"  SETNX '{test_key}' = '{new_value}'...")
        try:
            result = write_node.setnx(test_key, new_value)
            if result != 0:
                print(f"  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SETNX failed - {e}")
            return False
        
        # Verify value was NOT changed
        value = write_node.get(test_key)
        if value != initial_value:
            print(f"  FAILED: Value was changed to '{value}', expected '{initial_value}'")
            return False
        print("  Value unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_setnx_replication(self) -> bool:
        """Test that SETNX data is replicated to all nodes."""
        print("\nTest: SETNX replication")
        
        test_key = "setnx_repl_key"
        test_value = "setnx_repl_value"
        
        write_node = self._get_random_node()
        
        # Make sure key doesn't exist
        write_node.delete(test_key)
        
        # SETNX on random node
        print(f"  SETNX '{test_key}' = '{test_value}' on random node...")
        try:
            result = write_node.setnx(test_key, test_value)
            if result != 1:
                print(f"  FAILED: Expected 1, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SETNX failed - {e}")
            return False
        
        # Verify all nodes have the data
        print("  Verifying all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
                if value != test_value:
                    print(f"    Node {i}: FAILED (expected '{test_value}', got '{value}')")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_setnx_wrong_type(self) -> bool:
        """Test SETNX on hash key (wrong type) returns 0."""
        print("\nTest: SETNX on hash key (wrong type)")
        
        test_key = "setnx_wrong_type_key"
        test_value = "setnx_value"
        
        write_node = self._get_random_node()
        
        # Create a hash key
        print(f"  HSET '{test_key}' field 'value'...")
        try:
            write_node.hset(test_key, "field", "value")
        except redis.RedisError as e:
            print(f"  FAILED: HSET failed - {e}")
            return False
        
        # SETNX on hash key should return 0 (key exists)
        print(f"  SETNX '{test_key}' = '{test_value}'...")
        try:
            result = write_node.setnx(test_key, test_value)
            if result != 0:
                print(f"  FAILED: Expected 0, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: SETNX failed - {e}")
            return False
        
        # Verify hash was not modified
        result = write_node.hget(test_key, "field")
        if result != "value":
            print(f"  FAILED: Hash was modified, field value is '{result}'")
            return False
        print("  Hash unchanged: OK")
        
        print("  PASSED")
        return True
    
    def test_setnx_wrong_args(self) -> bool:
        """Test SETNX with wrong number of arguments."""
        print("\nTest: SETNX wrong arguments")
        
        write_node = self._get_random_node()
        
        # Too few arguments
        print("  Testing with too few arguments...")
        try:
            # Use execute_command to bypass redis-py validation
            write_node.execute_command("SETNX", "key1")
            print("  FAILED: Expected error for too few arguments")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "wrong number" not in error_msg:
                print(f"  FAILED: Expected 'wrong number' error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
        # Too many arguments
        print("  Testing with too many arguments...")
        try:
            write_node.execute_command("SETNX", "key1", "value1", "extra")
            print("  FAILED: Expected error for too many arguments")
            return False
        except redis.ResponseError as e:
            error_msg = str(e).lower()
            if "wrong number" not in error_msg:
                print(f"  FAILED: Expected 'wrong number' error, got: {e}")
                return False
            print(f"  Got expected error: {e}")
        
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
    
    def test_expire_basic(self) -> bool:
        """Test EXPIRE sets TTL on an existing key."""
        print("\nTest: EXPIRE basic")
        
        test_key = "expire_basic_key"
        test_value = "expire_basic_value"
        
        write_node = self._get_random_node()
        
        # Set a key without expiration
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        # Set expiration to 2 seconds
        print(f"  EXPIRE '{test_key}' 2...")
        try:
            result = write_node.expire(test_key, 2)
            if result != True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE failed - {e}")
            return False
        print("  EXPIRE returned True: OK")
        
        # Verify key is still readable
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key not readable immediately, got '{value}'")
            return False
        print("  Key still readable: OK")
        
        # Wait for expiration
        print("  Waiting for expiration...")
        time.sleep(3)
        
        # Verify key is expired
        value = self.nodes[0].conn.get(test_key)
        if value is not None:
            print(f"  FAILED: Key should have expired but got '{value}'")
            return False
        print("  Key expired correctly: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_nonexistent_key(self) -> bool:
        """Test EXPIRE on non-existent key returns 0."""
        print("\nTest: EXPIRE non-existent key")
        
        test_key = "expire_nonexistent_key"
        
        write_node = self._get_random_node()
        write_node.delete(test_key)
        
        print(f"  EXPIRE '{test_key}' 60 (non-existent)...")
        try:
            result = write_node.expire(test_key, 60)
            if result != False:
                print(f"  FAILED: Expected False, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE failed - {e}")
            return False
        print("  EXPIRE returned False: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_replication(self) -> bool:
        """Test that EXPIRE replicates to all nodes."""
        print("\nTest: EXPIRE replication")
        
        test_key = "expire_repl_key"
        test_value = "expire_repl_value"
        
        write_node = self._get_random_node()
        
        # Set and expire on one node
        print(f"  SET '{test_key}' = '{test_value}'...")
        write_node.set(test_key, test_value)
        
        print(f"  EXPIRE '{test_key}' 2...")
        write_node.expire(test_key, 2)
        
        # Verify all nodes see the expiration
        print("  Waiting for expiration...")
        time.sleep(3)
        
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.conn.get(test_key)
                if value is not None:
                    print(f"    Node {i}: FAILED (key should have expired)")
                    return False
                print(f"    Node {i}: OK (key expired)")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def test_expire_nx_no_existing_ttl(self) -> bool:
        """Test EXPIRE NX on key without existing TTL (should succeed)."""
        print("\nTest: EXPIRE NX (no existing TTL)")
        
        test_key = "expire_nx_no_ttl_key"
        test_value = "expire_nx_value"
        
        write_node = self._get_random_node()
        write_node.set(test_key, test_value)
        
        print(f"  EXPIRE '{test_key}' 60 NX (no existing TTL)...")
        try:
            result = write_node.expire(test_key, 60, nx=True)
            if result != True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE NX failed - {e}")
            return False
        print("  EXPIRE NX returned True: OK")
        
        # Verify key still readable
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key value changed")
            return False
        
        print("  PASSED")
        return True
    
    def test_expire_nx_with_existing_ttl(self) -> bool:
        """Test EXPIRE NX on key with existing TTL (should fail)."""
        print("\nTest: EXPIRE NX (with existing TTL)")
        
        test_key = "expire_nx_with_ttl_key"
        test_value = "expire_nx_value"
        
        write_node = self._get_random_node()
        write_node.set(test_key, test_value, px=5000)
        
        print(f"  EXPIRE '{test_key}' 60 NX (already has TTL)...")
        try:
            result = write_node.expire(test_key, 60, nx=True)
            if result != False:
                print(f"  FAILED: Expected False, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE NX failed - {e}")
            return False
        print("  EXPIRE NX returned False: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_xx_with_existing_ttl(self) -> bool:
        """Test EXPIRE XX on key with existing TTL (should succeed)."""
        print("\nTest: EXPIRE XX (with existing TTL)")
        
        test_key = "expire_xx_with_ttl_key"
        test_value = "expire_xx_value"
        
        write_node = self._get_random_node()
        write_node.set(test_key, test_value, px=5000)
        
        print(f"  EXPIRE '{test_key}' 60 XX (has TTL)...")
        try:
            result = write_node.expire(test_key, 60, xx=True)
            if result != True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE XX failed - {e}")
            return False
        print("  EXPIRE XX returned True: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_xx_no_existing_ttl(self) -> bool:
        """Test EXPIRE XX on key without existing TTL (should fail)."""
        print("\nTest: EXPIRE XX (no existing TTL)")
        
        test_key = "expire_xx_no_ttl_key"
        test_value = "expire_xx_value"
        
        write_node = self._get_random_node()
        write_node.set(test_key, test_value)
        
        print(f"  EXPIRE '{test_key}' 60 XX (no TTL)...")
        try:
            result = write_node.expire(test_key, 60, xx=True)
            if result != False:
                print(f"  FAILED: Expected False, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE XX failed - {e}")
            return False
        print("  EXPIRE XX returned False: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_gt_greater(self) -> bool:
        """Test EXPIRE GT with new TTL greater than current (should succeed)."""
        print("\nTest: EXPIRE GT (greater)")
        
        test_key = "expire_gt_greater_key"
        test_value = "expire_gt_value"
        
        write_node = self._get_random_node()
        # Set with 1 second TTL
        write_node.set(test_key, test_value, px=1000)
        
        # EXPIRE GT with 100 seconds (much greater)
        print(f"  EXPIRE '{test_key}' 100 GT (new > current)...")
        try:
            result = write_node.expire(test_key, 100, gt=True)
            if result != True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE GT failed - {e}")
            return False
        print("  EXPIRE GT returned True: OK")
        
        # Key should still be readable after 2 seconds (since TTL was extended)
        time.sleep(2)
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key expired prematurely")
            return False
        print("  Key still readable after 2s (TTL extended): OK")
        
        print("  PASSED")
        return True
    
    def test_expire_gt_not_greater(self) -> bool:
        """Test EXPIRE GT with new TTL not greater than current (should fail)."""
        print("\nTest: EXPIRE GT (not greater)")
        
        test_key = "expire_gt_not_greater_key"
        test_value = "expire_gt_value"
        
        write_node = self._get_random_node()
        # Set with 100 seconds TTL
        write_node.set(test_key, test_value, px=100000)
        
        # EXPIRE GT with 1 second (less than current)
        print(f"  EXPIRE '{test_key}' 1 GT (new < current)...")
        try:
            result = write_node.expire(test_key, 1, gt=True)
            if result != False:
                print(f"  FAILED: Expected False, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE GT failed - {e}")
            return False
        print("  EXPIRE GT returned False: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_lt_less(self) -> bool:
        """Test EXPIRE LT with new TTL less than current (should succeed)."""
        print("\nTest: EXPIRE LT (less)")
        
        test_key = "expire_lt_less_key"
        test_value = "expire_lt_value"
        
        write_node = self._get_random_node()
        # Set with 100 seconds TTL
        write_node.set(test_key, test_value, px=100000)
        
        # EXPIRE LT with 1 second (less than current)
        print(f"  EXPIRE '{test_key}' 1 LT (new < current)...")
        try:
            result = write_node.expire(test_key, 1, lt=True)
            if result != True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE LT failed - {e}")
            return False
        print("  EXPIRE LT returned True: OK")
        
        # Key should expire within 2 seconds
        time.sleep(2)
        value = write_node.get(test_key)
        if value is not None:
            print(f"  FAILED: Key should have expired")
            return False
        print("  Key expired correctly: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_lt_not_less(self) -> bool:
        """Test EXPIRE LT with new TTL not less than current (should fail)."""
        print("\nTest: EXPIRE LT (not less)")
        
        test_key = "expire_lt_not_less_key"
        test_value = "expire_lt_value"
        
        write_node = self._get_random_node()
        # Set with 1 second TTL
        write_node.set(test_key, test_value, px=1000)
        
        # EXPIRE LT with 100 seconds (greater than current)
        print(f"  EXPIRE '{test_key}' 100 LT (new > current)...")
        try:
            result = write_node.expire(test_key, 100, lt=True)
            if result != False:
                print(f"  FAILED: Expected False, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE LT failed - {e}")
            return False
        print("  EXPIRE LT returned False: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_on_hash_key(self) -> bool:
        """Test EXPIRE works on hash keys too."""
        print("\nTest: EXPIRE on hash key")
        
        test_key = "expire_hash_key"
        
        write_node = self._get_random_node()
        write_node.delete(test_key)
        
        # Create a hash
        print(f"  HSET '{test_key}' field value...")
        write_node.hset(test_key, "field", "value")
        
        # Set expiration
        print(f"  EXPIRE '{test_key}' 2...")
        try:
            result = write_node.expire(test_key, 2)
            if result != True:
                print(f"  FAILED: Expected True, got {result}")
                return False
        except redis.RedisError as e:
            print(f"  FAILED: EXPIRE on hash failed - {e}")
            return False
        print("  EXPIRE returned True: OK")
        
        # Verify hash is still readable
        value = write_node.hget(test_key, "field")
        if value != "value":
            print(f"  FAILED: Hash field not readable")
            return False
        print("  Hash still readable: OK")
        
        # Wait for expiration
        print("  Waiting for expiration...")
        time.sleep(3)
        
        # Verify hash is expired
        value = write_node.hget(test_key, "field")
        if value is not None:
            print(f"  FAILED: Hash should have expired")
            return False
        print("  Hash expired correctly: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_preserves_value(self) -> bool:
        """Test EXPIRE does not modify the key's value."""
        print("\nTest: EXPIRE preserves value")
        
        test_key = "expire_preserve_key"
        test_value = "original_value_12345"
        
        write_node = self._get_random_node()
        write_node.set(test_key, test_value)
        
        print(f"  EXPIRE '{test_key}' 60...")
        write_node.expire(test_key, 60)
        
        # Value should be unchanged
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Value changed to '{value}', expected '{test_value}'")
            return False
        print("  Value preserved: OK")
        
        print("  PASSED")
        return True
    
    def test_expire_update_existing_ttl(self) -> bool:
        """Test EXPIRE overwrites an existing TTL."""
        print("\nTest: EXPIRE updates existing TTL")
        
        test_key = "expire_update_key"
        test_value = "expire_update_value"
        
        write_node = self._get_random_node()
        
        # Set with 1 second TTL
        print(f"  SET '{test_key}' with 1s TTL...")
        write_node.set(test_key, test_value, px=1000)
        
        # Wait a bit, then extend to 5 seconds
        time.sleep(0.5)
        print(f"  EXPIRE '{test_key}' 5 (extend TTL)...")
        write_node.expire(test_key, 5)
        
        # After 2 seconds, key should still exist (original 1s would have expired)
        time.sleep(2)
        value = write_node.get(test_key)
        if value != test_value:
            print(f"  FAILED: Key should still exist (TTL was extended)")
            return False
        print("  Key still exists after 2s (TTL extended): OK")
        
        # Wait for the new TTL to expire
        print("  Waiting for new TTL to expire...")
        time.sleep(4)
        value = write_node.get(test_key)
        if value is not None:
            print(f"  FAILED: Key should have expired")
            return False
        print("  Key expired with new TTL: OK")
        
        print("  PASSED")
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
            self.test_setex_basic,
            self.test_setex_expiration,
            self.test_setex_overwrite,
            self.test_setex_replication,
            self.test_setex_equivalent_to_set_ex,
            self.test_setex_wrong_args,
            self.test_psetex_basic,
            self.test_psetex_expiration,
            self.test_psetex_overwrite,
            self.test_psetex_replication,
            self.test_psetex_equivalent_to_set_px,
            self.test_psetex_wrong_args,
            self.test_setnx_new_key,
            self.test_setnx_existing_key,
            self.test_setnx_replication,
            self.test_setnx_wrong_type,
            self.test_setnx_wrong_args,
            self.test_del_single_key,
            self.test_del_multiple_keys,
            self.test_del_nonexistent_key,
            self.test_del_mixed_keys,
            self.test_del_replication,
            self.test_mget_single_key,
            self.test_mget_multiple_keys,
            self.test_mget_nonexistent_keys,
            self.test_mget_mixed_keys,
            self.test_mget_replication,
            self.test_mset_single_pair,
            self.test_mset_multiple_pairs,
            self.test_mset_overwrite,
            self.test_mset_replication,
            self.test_mset_atomicity_batch_consistency,
            self.test_incr_invalid_value,
            self.test_incr_wrong_type,
            self.test_incrby_basic,
            self.test_incrby_nonexistent_key,
            self.test_incrby_negative,
            self.test_incrby_zero,
            self.test_incrby_overflow,
            self.test_incrby_invalid_value,
            self.test_incrby_wrong_type,
            self.test_incrby_replication,
            self.test_incrby_large_values,
            self.test_decr_basic,
            self.test_decr_nonexistent_key,
            self.test_decr_negative_values,
            self.test_decr_zero,
            self.test_decr_overflow,
            self.test_decr_invalid_value,
            self.test_decr_wrong_type,
            self.test_decr_replication,
            self.test_decr_large_values,
            self.test_decrby_basic,
            self.test_decrby_nonexistent_key,
            self.test_decrby_negative,
            self.test_decrby_zero,
            self.test_decrby_overflow,
            self.test_decrby_invalid_value,
            self.test_decrby_wrong_type,
            self.test_decrby_replication,
            self.test_decrby_large_values,
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
            self.test_append_new_key,
            self.test_append_existing_key,
            self.test_append_empty_string,
            self.test_append_wrong_type,
            self.test_append_replication,
            self.test_append_preserves_expiration,
            self.test_strlen_existing_key,
            self.test_strlen_nonexistent_key,
            self.test_strlen_empty_string,
            self.test_strlen_binary_data,
            self.test_strlen_unicode,
            self.test_strlen_large_value,
            self.test_strlen_replication,
            self.test_strlen_wrong_type,
            self.test_type_string_key,
            self.test_type_hash_key,
            self.test_type_nonexistent_key,
            self.test_type_after_del,
            self.test_type_replication,
            self.test_type_expired_key,
            self.test_chaos_set_get,  # Chaos test enabled
            self.test_expire_basic,
            self.test_expire_nonexistent_key,
            self.test_expire_replication,
            self.test_expire_nx_no_existing_ttl,
            self.test_expire_nx_with_existing_ttl,
            self.test_expire_xx_with_existing_ttl,
            self.test_expire_xx_no_existing_ttl,
            self.test_expire_gt_greater,
            self.test_expire_gt_not_greater,
            self.test_expire_lt_less,
            self.test_expire_lt_not_less,
            self.test_expire_on_hash_key,
            self.test_expire_preserves_value,
            self.test_expire_update_existing_ttl,
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
