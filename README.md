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

Or use the start script:

```bash
./scripts/start.sh build
```

### Run Single Node

```bash
# Run with default configuration
cargo run -- --conf conf/node1.toml
```

### Run 3-Node Cluster

```bash
# Start all nodes
./scripts/start.sh start

# Check status
./scripts/start.sh status

# View logs
./scripts/start.sh logs node1

# Run basic test
./scripts/start.sh test

# Stop cluster
./scripts/start.sh stop
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
