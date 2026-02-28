# CoreDB

A Redis-compatible distributed KV database with strong consistency, built on Raft consensus and RocksDB.

## Features

- **Redis Protocol Compatible**: Use any Redis client to connect
- **Distributed Consensus**: Built on Raft for strong consistency
- **Persistent Storage**: RocksDB backend for durability
- **Cluster Mode**: Multi-node cluster with automatic failover

## Quick Start

### Build

```bash
cargo build --release
```

## Configuration

CoreDB uses TOML configuration files. Example:

```toml
node_id = 1
server_addr = "0.0.0.0:6379"

[raft]
address = "127.0.0.1:7001"
advertise_host = "localhost"
single = true  # Set to false for cluster mode
join = []      # Addresses to join for cluster mode

[rocksdb]
data_path = "/tmp/coredb/node1"
max_open_files = 10000

[log]
level = "info"
```

### Configuration Fields

| Field | Description |
|-------|-------------|
| `node_id` | Unique node ID in the cluster |
| `server_addr` | Redis protocol listening address |
| `raft.address` | Raft consensus listening address |
| `raft.advertise_host` | Host advertised to other nodes |
| `raft.single` | Run as single node (no cluster) |
| `raft.join` | List of nodes to join for cluster mode |
| `rocksdb.data_path` | RocksDB data directory |
| `rocksdb.max_open_files` | Max open files for RocksDB |
| `log.level` | Log level (debug, info, warn, error) |

## Usage

### Using redis-cli

```bash
# Connect to node
redis-cli -p 6379

# Set a key
SET mykey "myvalue"

# Get a key
GET mykey

# Set with expiration (in seconds)
SET mykey "myvalue" EX 60

# Delete a key
DEL mykey
```

### Using any Redis client

```python
import redis

r = redis.Redis(host='localhost', port=6379)
r.set('key', 'value')
print(r.get('key'))
```

## Architecture

```
┌─────────────────┐
│   Redis Client  │
└────────┬────────┘
         │ RESP Protocol
         ▼
┌─────────────────┐     ┌─────────────────┐
│   CoreDB Server │────▶│   Raft Node     │
│   (Redis API)   │     │   (Consensus)   │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│   State Machine │     │   Raft Log      │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └──────────┬────────────┘
                    ▼
            ┌───────────────┐
            │   RocksDB     │
            └───────────────┘
```

## Commands

| Command | Description | Status |
|---------|-------------|--------|
| GET key | Get value by key | ✅ |
| SET key value | Set key-value | ✅ |
| SET key value EX seconds | Set with expiration | ✅ |
| SET key value PX ms | Set with expiration (ms) | ✅ |
| DEL key | Delete key | ✅ |
| PING | Ping server | ✅ |

## Testing

### Unit Tests

```bash
cargo test
```

### Integration Tests (Python)

The `tests/` directory contains Python-based integration tests that verify cluster functionality.

```bash
# Install dependencies
cd tests
pip install -r requirements.txt

# Run integration tests
python test_cluster.py
```

This will:
1. Build the project
2. Start a 3-node cluster
3. Run SET/GET tests
4. Verify data replication across nodes
5. Stop the cluster

### Manual Cluster Testing

```bash
cd tests

# Start cluster
./start.sh start

# Check status
./start.sh status

# Stop cluster
./start.sh stop

# Clean up data
./start.sh clean
```

## Development

### Run tests

```bash
cargo test
```

### Format code

```bash
cargo fmt
```

### Run clippy

```bash
cargo clippy
```

## License

Apache-2.0
