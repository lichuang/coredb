#!/usr/bin/env python3
"""CoreDB Cluster Management Module

This module provides ClusterManager class for managing CoreDB cluster lifecycle.
"""

import subprocess
import time
import os
from typing import List


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
