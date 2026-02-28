#!/usr/bin/env python3
"""
CoreDB Cluster Integration Tests

This test suite:
1. Starts a 3-node CoreDB cluster
2. Performs basic SET/GET operations
3. Verifies data replication across nodes
4. Tests persistence after restart
5. Stops the cluster

Usage:
    pip install -r requirements.txt
    python test_cluster.py
"""

import subprocess
import time
import sys
import os
import signal
from typing import List

import redis


class ClusterManager:
    """Manages CoreDB cluster lifecycle for testing."""
    
    def __init__(self, tests_dir: str):
        self.tests_dir = tests_dir
        self.start_script = os.path.join(tests_dir, "start.sh")
        
    def _run_command(self, cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Run a command in the tests directory."""
        return subprocess.run(
            cmd,
            cwd=self.tests_dir,
            capture_output=True,
            text=True,
            check=check
        )
    
    def build(self) -> bool:
        """Build the CoreDB project."""
        print("Building CoreDB...")
        result = self._run_command(["./start.sh", "build"], check=False)
        if result.returncode != 0:
            print(f"Build failed:\n{result.stdout}\n{result.stderr}")
            return False
        print("Build successful")
        return True
    
    def start(self) -> bool:
        """Start the 3-node cluster."""
        print("Starting CoreDB cluster...")
        result = self._run_command(["./start.sh", "start"], check=False)
        if result.returncode != 0:
            print(f"Failed to start cluster:\n{result.stdout}\n{result.stderr}")
            return False
        
        # Wait for cluster to be ready
        print("Waiting for cluster to be ready...")
        time.sleep(3)
        
        print("Cluster started successfully")
        return True
    
    def stop(self) -> None:
        """Stop the cluster (without cleaning data)."""
        print("Stopping CoreDB cluster...")
        self._run_command(["./start.sh", "stop"], check=False)
        print("Cluster stopped")
    
    def clean(self) -> None:
        """Clean up data and logs."""
        print("Cleaning up...")
        self._run_command(["./start.sh", "clean"], check=False)


class TestClusterBasic:
    """Basic cluster functionality tests."""
    
    def __init__(self, cluster: ClusterManager):
        self.cluster = cluster
        self.nodes: List[redis.Redis] = []
        
    def setup(self) -> bool:
        """Setup connections to all nodes."""
        try:
            self.nodes = [
                redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=5),
                redis.Redis(host='localhost', port=6380, decode_responses=True, socket_connect_timeout=5),
                redis.Redis(host='localhost', port=6381, decode_responses=True, socket_connect_timeout=5),
            ]
            # Verify connections with a simple SET/GET
            for i, node in enumerate(self.nodes, 1):
                node.set('_test_conn', 'ok')
                print(f"  Connected to Node {i} (port {6378 + i})")
            return True
        except redis.RedisError as e:
            print(f"Failed to connect to nodes: {e}")
            return False
    
    def test_set_and_get(self) -> bool:
        """Test basic SET and GET operations."""
        print("\nTest: SET and GET")
        
        test_key = "test_key"
        test_value = "test_value_123"
        
        # SET to Node 1 (leader)
        print(f"  SET '{test_key}' = '{test_value}' on Node 1...")
        try:
            self.nodes[0].set(test_key, test_value)
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        # GET from all nodes
        print("  GET from all nodes...")
        for i, node in enumerate(self.nodes, 1):
            try:
                value = node.get(test_key)
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
        
        # SET with 500ms expiration
        print(f"  SET with 500ms TTL...")
        try:
            self.nodes[0].set(test_key, test_value, px=500)
        except redis.RedisError as e:
            print(f"  FAILED: SET failed - {e}")
            return False
        
        # Verify it's readable immediately
        value = self.nodes[0].get(test_key)
        if value != test_value:
            print(f"  FAILED: Key not readable immediately after write")
            return False
        print("  Key readable immediately: OK")
        
        # Wait for expiration
        print("  Waiting for expiration...")
        time.sleep(1)
        
        # Verify it's expired (returns None)
        value = self.nodes[0].get(test_key)
        if value is not None:
            print(f"  FAILED: Key should have expired but got '{value}'")
            return False
        print("  Key expired correctly: OK")
        
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
                node.close()
            except:
                pass
        self.nodes = []
    
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
            self.nodes[0].set(test_key_1, test_value_1)
            self.nodes[0].set(test_key_2, test_value_2)
            print(f"    SET '{test_key_1}' = '{test_value_1}'")
            print(f"    SET '{test_key_2}' = '{test_value_2}'")
        except redis.RedisError as e:
            print(f"  FAILED: Failed to write initial data - {e}")
            return False
        
        # Verify data is written
        value = self.nodes[0].get(test_key_1)
        if value != test_value_1:
            print(f"  FAILED: Initial write verification failed")
            return False
        print("  Initial data written successfully")
        
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
            value_1 = self.nodes[0].get(test_key_1)
            value_2 = self.nodes[0].get(test_key_2)
            
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
            self.nodes[0].set(new_key, new_value)
            value = self.nodes[0].get(new_key)
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
                v1 = node.get(test_key_1)
                v2 = node.get(test_key_2)
                v3 = node.get(new_key)
                
                if v1 != test_value_1 or v2 != test_value_2 or v3 != new_value:
                    print(f"    Node {i}: FAILED (data mismatch)")
                    return False
                print(f"    Node {i}: OK")
            except redis.RedisError as e:
                print(f"    Node {i}: FAILED - {e}")
                return False
        
        print("  PASSED")
        return True
    
    def run_all_tests(self) -> bool:
        """Run all tests."""
        print("\n" + "="*50)
        print("Running Tests")
        print("="*50)
        
        if not self.setup():
            return False
        
        tests = [
            self.test_set_and_get,
            self.test_set_with_expiration,
            self.test_restart_persistence,
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
        tester = TestClusterBasic(cluster)
        success = tester.run_all_tests()
        
        if success:
            print("\n✅ All tests passed!")
        else:
            print("\n❌ Some tests failed!")
            
    finally:
        # Always stop cluster
        cluster.stop()
        cluster.clean()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
