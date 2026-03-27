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

### String Commands

| Command | Description | Status |
|---------|-------------|--------|
| `GET key` | Get value by key | ✅ |
| `SET key value` | Set key-value | ✅ |
| `SET key value [NX\|XX] [GET] [EX seconds\|PX milliseconds\|EXAT timestamp\|PXAT timestamp\|KEEPTTL]` | Set with options | ✅ |
| `DEL key [key ...]` | Delete key(s) | ✅ |
| `MGET key [key ...]` | Get multiple keys | ✅ |
| `MSET key value [key value ...]` | Set multiple key-value pairs | ✅ |
| `INCR key` | Increment key by 1 | ✅ |
| `INCRBY key increment` | Increment key by value | ✅ |
| `DECR key` | Decrement key by 1 | ✅ |
| `DECRBY key decrement` | Decrement key by value | ✅ |
| `APPEND key value` | Append value to key | ✅ |
| `STRLEN key` | Get string length | ✅ |
| `GETSET key value` | Get old value and set new value | ❌ |
| `SETEX key seconds value` | Set with expiration (seconds) | ✅ |
| `PSETEX key milliseconds value` | Set with expiration (ms) | ✅ |
| `SETNX key value` | Set if key not exists | ✅ |

### Hash Commands

| Command | Description | Status |
|---------|-------------|--------|
| `HGET key field` | Get hash field value | ✅ |
| `HSET key field value [field value ...]` | Set hash field(s) | ✅ |
| `HDEL key field [field ...]` | Delete hash field(s) | ✅ |
| `HEXISTS key field` | Check if field exists | ✅ |
| `HGETALL key` | Get all fields and values | ✅ |
| `HKEYS key` | Get all field names | ✅ |
| `HLEN key` | Get number of fields | ✅ |
| `HMGET key field [field ...]` | Get multiple field values | ✅ |
| `HSETNX key field value` | Set field if not exists | ✅ |
| `HVALS key` | Get all field values | ✅ |
| `HINCRBY key field increment` | Increment field value | ✅ |

### Connection Commands

| Command | Description | Status |
|---------|-------------|--------|
| `PING [message]` | Ping server | ✅ |
| `ECHO message` | Echo message | ❌ |
| `SELECT index` | Select database | ❌ |
| `QUIT` | Close connection | ❌ |

### Key Commands

| Command | Description | Status |
|---------|-------------|--------|
| `EXISTS key [key ...]` | Check if key(s) exist | ✅ |
| `EXPIRE key seconds [NX|XX|GT|LT]` | Set expiration in seconds | ✅ |
| `PEXPIRE key milliseconds` | Set expiration in ms | ❌ |
| `TTL key` | Get remaining TTL | ❌ |
| `PTTL key` | Get remaining TTL in ms | ❌ |
| `PERSIST key` | Remove expiration | ❌ |
| `KEYS pattern` | Find keys matching pattern | ❌ |
| `RENAME key newkey` | Rename key | ❌ |
| `RENAMENX key newkey` | Rename if newkey not exists | ❌ |
| `TYPE key` | Get value type | ✅ |
| `UNLINK key [key ...]` | Delete key(s) asynchronously | ❌ |
| `FLUSHDB` | Delete all keys in current DB | ❌ |
| `FLUSHALL` | Delete all keys in all DBs | ❌ |

### List Commands

| Command | Description | Status |
|---------|-------------|--------|
| `LPUSH key value [value ...]` | Push to left | ❌ |
| `RPUSH key value [value ...]` | Push to right | ❌ |
| `LPOP key [count]` | Pop from left | ❌ |
| `RPOP key [count]` | Pop from right | ❌ |
| `LLEN key` | Get list length | ❌ |
| `LRANGE key start stop` | Get range of elements | ❌ |
| `LINDEX key index` | Get element at index | ❌ |
| `LSET key index value` | Set element at index | ❌ |
| `LREM key count value` | Remove elements | ❌ |
| `LTRIM key start stop` | Trim list | ❌ |

### Set Commands

| Command | Description | Status |
|---------|-------------|--------|
| `SADD key member [member ...]` | Add member(s) to set | ❌ |
| `SREM key member [member ...]` | Remove member(s) from set | ❌ |
| `SMEMBERS key` | Get all members | ❌ |
| `SISMEMBER key member` | Check if member exists | ❌ |
| `SCARD key` | Get set cardinality | ❌ |
| `SPOP key [count]` | Remove and return random member(s) | ❌ |
| `SRANDMEMBER key [count]` | Get random member(s) | ❌ |
| `SINTER key [key ...]` | Intersection of sets | ❌ |
| `SUNION key [key ...]` | Union of sets | ❌ |
| `SDIFF key [key ...]` | Difference of sets | ❌ |

### Sorted Set Commands

| Command | Description | Status |
|---------|-------------|--------|
| `ZADD key [NX\|XX] [GT\|LT] [CH] [INCR] score member [score member ...]` | Add member(s) with score | ❌ |
| `ZREM key member [member ...]` | Remove member(s) | ❌ |
| `ZRANGE key start stop [WITHSCORES]` | Get range by rank | ❌ |
| `ZREVRANGE key start stop [WITHSCORES]` | Get range by rank (reverse) | ❌ |
| `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | Get range by score | ❌ |
| `ZSCORE key member` | Get member score | ❌ |
| `ZCARD key` | Get sorted set cardinality | ❌ |
| `ZCOUNT key min max` | Count members in score range | ❌ |
| `ZRANK key member` | Get member rank | ❌ |
| `ZREVRANK key member` | Get member rank (reverse) | ❌ |
| `ZINCRBY key increment member` | Increment member score | ❌ |
| `ZREMKEY key` | Remove key | ❌ |

### Transaction Commands

| Command | Description | Status |
|---------|-------------|--------|
| `MULTI` | Start transaction | ❌ |
| `EXEC` | Execute transaction | ❌ |
| `DISCARD` | Discard transaction | ❌ |
| `WATCH key [key ...]` | Watch key(s) for changes | ❌ |
| `UNWATCH` | Unwatch all keys | ❌ |

### Server Commands

| Command | Description | Status |
|---------|-------------|--------|
| `INFO [section]` | Get server info | ❌ |
| `CONFIG GET parameter` | Get configuration | ❌ |
| `CONFIG SET parameter value` | Set configuration | ❌ |
| `DBSIZE` | Get key count | ❌ |
| `TIME` | Get server time | ❌ |
| `COMMAND` | Get command info | ❌ |
| `MEMORY USAGE key` | Get memory usage | ❌ |
| `CLIENT LIST` | List connections | ❌ |
| `CLIENT KILL [ip:port]` | Kill connection | ❌ |
| `SAVE` | Synchronous save | ❌ |
| `BGSAVE` | Asynchronous save | ❌ |
| `LASTSAVE` | Get last save time | ❌ |
| `SHUTDOWN` | Stop server | ❌ |

### Pub/Sub Commands

| Command | Description | Status |
|---------|-------------|--------|
| `SUBSCRIBE channel [channel ...]` | Subscribe to channel(s) | ❌ |
| `UNSUBSCRIBE [channel ...]` | Unsubscribe from channel(s) | ❌ |
| `PUBLISH channel message` | Publish message | ❌ |
| `PSUBSCRIBE pattern [pattern ...]` | Subscribe to pattern(s) | ❌ |
| `PUNSUBSCRIBE [pattern ...]` | Unsubscribe from pattern(s) | ❌ |
| `PUBSUB subcommand` | Pub/Sub introspection | ❌ |

### Stream Commands

| Command | Description | Status |
|---------|-------------|--------|
| `XADD key [NOMKSTREAM] [MAXLEN\|MINID [=\|~] threshold [LIMIT count]] [ID id] field value [field value ...]` | Add entry to stream | ❌ |
| `XRANGE key start end [COUNT count]` | Get range from stream | ❌ |
| `XREVRANGE key end start [COUNT count]` | Get range from stream (reverse) | ❌ |
| `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]` | Read from stream(s) | ❌ |
| `XDEL key id [id ...]` | Delete entries from stream | ❌ |
| `XLEN key` | Get stream length | ❌ |
| `XTRIM key MAXLEN\|MINID [=\|~] threshold [LIMIT count]` | Trim stream | ❌ |
| `XGROUP` | Stream consumer groups | ❌ |
| `XREADGROUP` | Read from stream with group | ❌ |
| `XACK` | Acknowledge message | ❌ |
| `XCLAIM` | Claim message | ❌ |
| `XPENDING` | Get pending messages | ❌ |

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
5. Test persistence after cluster restart
6. Stop the cluster

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
