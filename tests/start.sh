#!/bin/bash

# CoreDB Cluster Management Script
# Usage: ./start.sh [start|stop|status|clean]

# Don't use set -e as it causes issues with loops and conditional checks
# set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."
LOG_DIR="/tmp/coredb/logs"
PID_DIR="/tmp/coredb/pids"
DATA_DIR="/tmp/coredb"

# Binary path
BINARY="$PROJECT_ROOT/target/release/coredb"

# Create directories
mkdir -p "$LOG_DIR" "$PID_DIR"

# Check if binary exists
check_binary() {
    if [ ! -f "$BINARY" ]; then
        echo -e "${RED}Error: Binary not found at $BINARY${NC}"
        echo "Please run './start.sh build' first"
        return 1
    fi
    return 0
}

# Check if port is in use
check_port() {
    local port=$1
    if lsof -i :"$port" > /dev/null 2>&1; then
        return 1
    fi
    return 0
}

# Build the project
build() {
    echo "Building CoreDB..."
    cd "$PROJECT_ROOT"
    cargo build --release
    if [ $? -ne 0 ]; then
        echo -e "${RED}Build failed${NC}"
        exit 1
    fi
    echo -e "${GREEN}Build complete${NC}"
}

# Start a single node
start_node() {
    local node_id=$1
    local config_file=$2
    local pid_file="$PID_DIR/node${node_id}.pid"
    local log_file="$LOG_DIR/node${node_id}.log"
    
    # Check if already running
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file" 2>/dev/null)
        if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${YELLOW}Node $node_id is already running (PID: $pid)${NC}"
            return 0
        fi
    fi
    
    # Start the node
    echo "Starting Node $node_id..."
    nohup "$BINARY" --conf "$config_file" > "$log_file" 2>&1 &
    local pid=$!
    
    # Wait a bit and check if process is still running
    sleep 1
    if ! ps -p "$pid" > /dev/null 2>&1; then
        echo -e "${RED}Node $node_id failed to start (crashed immediately)${NC}"
        echo "Last 20 lines of log:"
        tail -20 "$log_file" 2>/dev/null || echo "(no log file)"
        return 1
    fi
    
    echo $pid > "$pid_file"
    echo -e "${GREEN}Node $node_id started (PID: $pid)${NC}"
    return 0
}

# Start all nodes or a specific node
start() {
    # Check if a specific node is requested
    local target_node="${1:-all}"
    
    if [ "$target_node" != "all" ]; then
        # Validate node name
        case "$target_node" in
            node1) start_node 1 "$SCRIPT_DIR/conf/node1.toml" ;;
            node2) start_node 2 "$SCRIPT_DIR/conf/node2.toml" ;;
            node3) start_node 3 "$SCRIPT_DIR/conf/node3.toml" ;;
            *)
                echo -e "${RED}Error: Unknown node '$target_node'. Use node1, node2, or node3.${NC}"
                exit 1
                ;;
        esac
        return $?
    fi
    
    echo "Starting CoreDB cluster..."
    
    # Check binary
    if ! check_binary; then
        exit 1
    fi
    
    # Check ports before starting
    local ports=("6379" "6380" "6381" "7001" "7002" "7003")
    local port_in_use=0
    for port in "${ports[@]}"; do
        if ! check_port "$port"; then
            echo -e "${RED}Error: Port $port is already in use${NC}"
            port_in_use=1
        fi
    done
    if [ $port_in_use -eq 1 ]; then
        echo "Please stop existing processes or change ports in configuration"
        exit 1
    fi
    
    # Start Node 1 (single mode, initializes cluster)
    if ! start_node 1 "$SCRIPT_DIR/conf/node1.toml"; then
        echo -e "${RED}Failed to start Node 1${NC}"
        exit 1
    fi
    
    # Wait for Node 1 to be ready
    echo "Waiting for Node 1 to initialize (3 seconds)..."
    sleep 3
    
    # Verify Node 1 is still running
    if [ -f "$PID_DIR/node1.pid" ]; then
        local pid=$(cat "$PID_DIR/node1.pid" 2>/dev/null)
        if ! ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${RED}Node 1 crashed during initialization${NC}"
            echo "Full log:"
            cat "$LOG_DIR/node1.log" 2>/dev/null || echo "(no log file)"
            exit 1
        fi
    fi
    
    # Start Node 2
    if ! start_node 2 "$SCRIPT_DIR/conf/node2.toml"; then
        echo -e "${YELLOW}Warning: Node 2 failed to start, continuing...${NC}"
    fi
    
    # Start Node 3
    if ! start_node 3 "$SCRIPT_DIR/conf/node3.toml"; then
        echo -e "${YELLOW}Warning: Node 3 failed to start, continuing...${NC}"
    fi
    
    echo ""
    echo -e "${GREEN}CoreDB cluster started!${NC}"
    echo "Node 1 (Redis): 127.0.0.1:6379, Raft: 127.0.0.1:7001"
    echo "Node 2 (Redis): 127.0.0.1:6380, Raft: 127.0.0.1:7002"
    echo "Node 3 (Redis): 127.0.0.1:6381, Raft: 127.0.0.1:7003"
    echo ""
    echo "Logs: $LOG_DIR"
    echo "Run './start.sh status' to check status"
}

# Stop a specific node by index
stop_node() {
    local i=$1
    local pid_file="$PID_DIR/node$i.pid"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file" 2>/dev/null)
        if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
            echo "Stopping Node $i (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            # Wait for process to terminate
            local j=0
            while [ $j -lt 20 ]; do
                if ! ps -p "$pid" > /dev/null 2>&1; then
                    break
                fi
                sleep 0.5
                j=$((j + 1))
            done
            # Force kill if still running
            if ps -p "$pid" > /dev/null 2>&1; then
                echo "Force killing Node $i..."
                kill -9 "$pid" 2>/dev/null || true
            fi
            echo -e "${GREEN}Node $i stopped${NC}"
        else
            echo -e "${YELLOW}Node $i was not running${NC}"
        fi
        rm -f "$pid_file"
    else
        echo -e "${YELLOW}Node $i PID file not found${NC}"
    fi
}

# Stop all nodes or a specific node
stop() {
    # Check if a specific node is requested
    local target_node="${1:-all}"
    
    if [ "$target_node" != "all" ]; then
        case "$target_node" in
            node1) stop_node 1 ;;
            node2) stop_node 2 ;;
            node3) stop_node 3 ;;
            *)
                echo -e "${RED}Error: Unknown node '$target_node'. Use node1, node2, or node3.${NC}"
                exit 1
                ;;
        esac
        return 0
    fi
    
    echo "Stopping CoreDB cluster..."
    
    for i in 1 2 3; do
        stop_node $i
    done
    
    # Also kill any remaining coredb processes
    pkill -9 coredb 2>/dev/null || true
    
    echo -e "${GREEN}CoreDB cluster stopped${NC}"
}

# Check status
status() {
    echo "CoreDB Cluster Status:"
    echo "====================="
    
    local running=0
    for i in 1 2 3; do
        local pid_file="$PID_DIR/node$i.pid"
        local port=$((6378 + i))
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file" 2>/dev/null)
            if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
                echo -e "Node $i: ${GREEN}RUNNING${NC} (PID: $pid, Redis Port: $port)"
                running=$((running + 1))
            else
                echo -e "Node $i: ${RED}STOPPED${NC} (stale PID file)"
                rm -f "$pid_file"
            fi
        else
            echo -e "Node $i: ${RED}STOPPED${NC}"
        fi
    done
    
    echo ""
    echo "Total: $running/3 nodes running"
}

# View logs
logs() {
    if [ -z "$2" ]; then
        echo "Usage: $0 logs <node1|node2|node3>"
        exit 1
    fi
    
    local node=$2
    local log_file="$LOG_DIR/${node}.log"
    
    if [ -f "$log_file" ]; then
        tail -f "$log_file"
    else
        echo "Log file not found: $log_file"
        exit 1
    fi
}

# Clean up data and logs
clean() {
    echo "Cleaning up CoreDB data and logs..."
    stop
    rm -rf "$DATA_DIR"
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Test cluster
test_cluster() {
    echo "Testing CoreDB cluster..."
    
    # Check if redis-cli is available
    if ! command -v redis-cli &> /dev/null; then
        echo -e "${YELLOW}Warning: redis-cli not found, skipping test${NC}"
        return
    fi
    
    # Test write to Node 1
    echo "Setting key 'test_key' to 'test_value' on Node 1..."
    if redis-cli -p 6379 SET test_key test_value; then
        echo -e "${GREEN}Write successful${NC}"
    else
        echo -e "${RED}Write failed${NC}"
        return 1
    fi
    
    # Test read from all nodes
    echo "Reading from all nodes..."
    for port in 6379 6380 6381; do
        echo -n "Node (port $port): "
        redis-cli -p $port GET test_key
    done
    
    # Clean up test key
    redis-cli -p 6379 DEL test_key > /dev/null
    
    echo -e "${GREEN}Cluster test complete${NC}"
}

# Main command handler
case "${1:-}" in
    build)
        build
        ;;
    start)
        # Check if second arg is a specific node
        if [ -n "${2:-}" ]; then
            start "$2"
        else
            start
        fi
        ;;
    stop)
        # Check if second arg is a specific node
        if [ -n "${2:-}" ]; then
            stop "$2"
        else
            stop
        fi
        ;;
    restart)
        if [ -n "${2:-}" ]; then
            stop "$2"
            sleep 2
            start "$2"
        else
            stop
            sleep 2
            start
        fi
        ;;
    status)
        status
        ;;
    logs)
        logs "$@"
        ;;
    clean)
        clean
        ;;
    test)
        test_cluster
        ;;
    *)
        echo "CoreDB Cluster Management Script"
        echo ""
        echo "Usage: $0 [command] [node]"
        echo ""
        echo "Commands:"
        echo "  build                   Build the project"
        echo "  start [node]            Start all nodes or a specific node (node1|node2|node3)"
        echo "  stop [node]             Stop all nodes or a specific node"
        echo "  restart [node]          Restart all nodes or a specific node"
        echo "  status                  Show cluster status"
        echo "  logs <node>             View logs for a node (node1|node2|node3)"
        echo "  clean                   Stop and clean up all data"
        echo "  test                    Run basic cluster test"
        echo ""
        echo "Examples:"
        echo "  $0 build"
        echo "  $0 start                # Start all nodes"
        echo "  $0 start node1          # Start only node 1"
        echo "  $0 stop node2           # Stop only node 2"
        echo "  $0 restart node3        # Restart only node 3"
        echo "  $0 status"
        echo "  $0 logs node1"
        echo "  $0 test"
        echo "  $0 stop"
        exit 1
        ;;
esac
