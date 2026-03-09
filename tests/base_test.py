#!/usr/bin/env python3
"""CoreDB Cluster Test Base Class

This module provides the base class for all cluster integration tests,
including chaos engineering capabilities for fault tolerance testing.
"""

import random
import subprocess
import time
from typing import List, Optional, Callable
from contextlib import contextmanager

import redis

from cluster_manager import ClusterManager


class NodeInfo:
    """Information about a cluster node."""
    
    def __init__(self, index: int, port: int, redis_conn: redis.Redis):
        self.index = index  # 0, 1, 2
        self.port = port    # 6379, 6380, 6381
        self.conn = redis_conn
        self.alive = True
    
    def __repr__(self):
        status = "alive" if self.alive else "dead"
        return f"Node{self.index + 1}(port={self.port}, {status})"


class TestClusterBase:
    """Base class for cluster integration tests with chaos testing support."""
    
    # Override this in subclasses to specify the verification command
    VERIFY_COMMAND: Optional[tuple] = None
    
    def __init__(self, cluster: ClusterManager):
        self.cluster = cluster
        self.nodes: List[NodeInfo] = []
        self._node_ports = [6379, 6380, 6381]
    
    def _get_random_node(self) -> redis.Redis:
        """Get a random alive node from the cluster for writing."""
        alive_nodes = self.get_alive_nodes()
        if not alive_nodes:
            raise RuntimeError("No alive nodes available")
        return random.choice(alive_nodes).conn
    
    def get_alive_nodes(self) -> List[NodeInfo]:
        """Get list of alive nodes."""
        return [n for n in self.nodes if n.alive]
    
    def get_dead_nodes(self) -> List[NodeInfo]:
        """Get list of dead nodes."""
        return [n for n in self.nodes if not n.alive]
    
    def setup(self) -> bool:
        """Setup connections to all nodes."""
        try:
            self.nodes = [
                NodeInfo(0, 6379, redis.Redis(host='localhost', port=6379, 
                        decode_responses=True, socket_connect_timeout=5)),
                NodeInfo(1, 6380, redis.Redis(host='localhost', port=6380, 
                        decode_responses=True, socket_connect_timeout=5)),
                NodeInfo(2, 6381, redis.Redis(host='localhost', port=6381, 
                        decode_responses=True, socket_connect_timeout=5)),
            ]
            # Verify connections using the subclass-specified command
            for node in self.nodes:
                self._verify_connection(node.conn)
                print(f"  Connected to {node}")
            return True
        except redis.RedisError as e:
            print(f"Failed to connect to nodes: {e}")
            return False
    
    def _verify_connection(self, node: redis.Redis) -> None:
        """Verify connection to a node. Override in subclass if needed."""
        if self.VERIFY_COMMAND:
            cmd, args = self.VERIFY_COMMAND[0], self.VERIFY_COMMAND[1:]
            getattr(node, cmd)(*args)
        else:
            # Default: just ping
            node.ping()
    
    def _do_kill_node(self, victim: NodeInfo) -> bool:
        """Internal method to actually kill a node process.
        
        Uses PID file from start.sh if available, otherwise falls back to lsof.
        Only kills processes named 'coredb' to avoid affecting other processes.
        
        Args:
            victim: The node to kill
            
        Returns:
            True if successfully killed
        """
        import os
        
        # Try PID file first (more reliable)
        pid_file = f"/tmp/coredb/pids/node{victim.index + 1}.pid"
        try:
            if os.path.exists(pid_file):
                with open(pid_file, 'r') as f:
                    pid = f.read().strip()
                if pid:
                    subprocess.run(['kill', '-9', pid], check=False)
                    # Remove PID file to indicate process is dead
                    os.remove(pid_file)
                    victim.alive = False
                    print(f"  Killed node on port {victim.port} (PID: {pid})")
                    time.sleep(1)
                    return True
        except Exception as e:
            print(f"  PID file method failed: {e}")
        
        # Fallback: use lsof, but only kill if process name contains 'coredb'
        try:
            # Get PID and process name
            result = subprocess.run(
                ['lsof', '-ti', f'tcp:{victim.port}'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                for pid in pids:
                    # Check if process is coredb
                    name_result = subprocess.run(
                        ['ps', '-p', pid, '-o', 'comm='],
                        capture_output=True,
                        text=True
                    )
                    if name_result.returncode == 0:
                        proc_name = name_result.stdout.strip()
                        if 'coredb' in proc_name.lower():
                            subprocess.run(['kill', '-9', pid], check=False)
                            victim.alive = False
                            print(f"  Killed node on port {victim.port} (PID: {pid}, {proc_name})")
                            time.sleep(1)
                            return True
                print(f"  No coredb process found for port {victim.port}")
                return False
            else:
                print(f"  Could not find process for port {victim.port}")
                return False
        except Exception as e:
            print(f"  Failed to kill node: {e}")
            return False
    
    def kill_random_node(self) -> Optional[NodeInfo]:
        """Kill a random alive node and return its info.
        
        Returns:
            NodeInfo of the killed node, or None if no alive nodes
        """
        alive_nodes = self.get_alive_nodes()
        if not alive_nodes:
            print("  No alive nodes to kill")
            return None
        
        victim = random.choice(alive_nodes)
        print(f"  Killing {victim}...")
        
        if self._do_kill_node(victim):
            return victim
        return None
    
    def kill_specific_node(self, node_index: int) -> bool:
        """Kill a specific node by index.
        
        Args:
            node_index: 0, 1, or 2
            
        Returns:
            True if successfully killed
        """
        if node_index < 0 or node_index >= len(self.nodes):
            print(f"  Invalid node index: {node_index}")
            return False
        
        victim = self.nodes[node_index]
        if not victim.alive:
            print(f"  Node {node_index + 1} is already dead")
            return False
        
        print(f"  Killing {victim}...")
        return self._do_kill_node(victim)
    
    def recover_node(self, node: NodeInfo) -> bool:
        """Restart a dead node.
        
        Args:
            node: The dead node to recover
            
        Returns:
            True if successfully recovered
        """
        if node.alive:
            print(f"  {node} is already alive")
            return True
        
        print(f"  Recovering {node}...")
        
        # Use the cluster manager's start script to restart the specific node
        # Node index is 0-based, but script uses 1-based (node1, node2, node3)
        node_name = f"node{node.index + 1}"
        result = subprocess.run(
            ['./start.sh', 'start', node_name],
            cwd=self.cluster.tests_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            # Wait for node to be ready
            time.sleep(2)
            # Re-create connection (old one may be stale)
            node.conn = redis.Redis(
                host='localhost', 
                port=node.port,
                decode_responses=True, 
                socket_connect_timeout=5
            )
            node.alive = True
            print(f"  {node} recovered successfully")
            return True
        else:
            print(f"  Failed to recover {node}: {result.stderr}")
            return False
    
    def recover_all_nodes(self) -> bool:
        """Restart all dead nodes.
        
        Returns:
            True if all nodes recovered successfully
        """
        dead_nodes = self.get_dead_nodes()
        if not dead_nodes:
            return True
        
        print(f"  Recovering {len(dead_nodes)} dead node(s)...")
        
        # Recover each dead node using recover_node
        success = True
        for node in dead_nodes:
            if not self.recover_node(node):
                success = False
        
        if success:
            print("  All nodes recovered")
        return success
    
    @contextmanager
    def chaos_context(self, kill_count: int = 1, auto_recover: bool = True):
        """Context manager for chaos testing.
        
        Usage:
            with self.chaos_context(kill_count=1):
                # Do operations while one node is down
                self.nodes[0].conn.set('key', 'value')
        
        Args:
            kill_count: Number of nodes to kill (default 1)
            auto_recover: Whether to recover nodes on exit (default True)
        """
        killed_nodes = []
        try:
            print(f"\n  [Chaos] Starting chaos: killing {kill_count} node(s)")
            for _ in range(kill_count):
                victim = self.kill_random_node()
                if victim:
                    killed_nodes.append(victim)
            
            if killed_nodes:
                alive = self.get_alive_nodes()
                print(f"  [Chaos] {len(alive)} node(s) remaining: {alive}")
            
            yield killed_nodes
            
        finally:
            if auto_recover and killed_nodes:
                print(f"  [Chaos] Auto-recovering {len(killed_nodes)} node(s)")
                self.recover_all_nodes()
    
    def run_with_chaos(self, test_func: Callable, kill_count: int = 1) -> bool:
        """Run a test function with chaos (random node failure).
        
        This is a helper method to easily add chaos testing to any test.
        
        Usage:
            def my_test(self) -> bool:
                def do_test():
                    self.nodes[0].conn.set('key', 'value')
                    return self.nodes[1].conn.get('key') == 'value'
                return self.run_with_chaos(do_test, kill_count=1)
        
        Args:
            test_func: Function to run during chaos (no args, returns bool)
            kill_count: Number of nodes to kill
            
        Returns:
            True if test_func returns True and chaos was handled properly
        """
        with self.chaos_context(kill_count=kill_count, auto_recover=True):
            print(f"  [Chaos] Running test with {kill_count} node(s) down...")
            try:
                result = test_func()
                if result:
                    print(f"  [Chaos] Test passed despite node failure!")
                else:
                    print(f"  [Chaos] Test failed")
                return result
            except Exception as e:
                print(f"  [Chaos] Test raised exception: {e}")
                return False
    
    def run_all_tests(self) -> bool:
        """Run all tests. Must be implemented by subclass."""
        raise NotImplementedError("Subclasses must implement run_all_tests()")
