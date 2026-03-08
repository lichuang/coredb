#!/usr/bin/env python3
"""CoreDB Cluster Test Base Class

This module provides the base class for all cluster integration tests.
"""

import random
from typing import List, Optional

import redis

from cluster_manager import ClusterManager


class TestClusterBase:
    """Base class for cluster integration tests."""
    
    # Override this in subclasses to specify the verification command
    VERIFY_COMMAND: Optional[tuple] = None
    
    def __init__(self, cluster: ClusterManager):
        self.cluster = cluster
        self.nodes: List[redis.Redis] = []
    
    def _get_random_node(self) -> redis.Redis:
        """Get a random node from the cluster for writing."""
        return random.choice(self.nodes)
    
    def setup(self) -> bool:
        """Setup connections to all nodes."""
        try:
            self.nodes = [
                redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=5),
                redis.Redis(host='localhost', port=6380, decode_responses=True, socket_connect_timeout=5),
                redis.Redis(host='localhost', port=6381, decode_responses=True, socket_connect_timeout=5),
            ]
            # Verify connections using the subclass-specified command
            for i, node in enumerate(self.nodes, 1):
                self._verify_connection(node)
                print(f"  Connected to Node {i} (port {6378 + i})")
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
    
    def run_all_tests(self) -> bool:
        """Run all tests. Must be implemented by subclass."""
        raise NotImplementedError("Subclasses must implement run_all_tests()")
