# CoreDB - Agent Guide

## Project Overview

CoreDB is a Redis-compatible distributed KV database with strong consistency, built on Raft consensus and RocksDB.

### Key Features
- **Redis Protocol Compatible**: Uses RESP (REdis Serialization Protocol), compatible with any Redis client
- **Distributed Consensus**: Built on Raft (via `rockraft` crate) for strong consistency
- **Persistent Storage**: RocksDB backend for durability
- **Cluster Mode**: Multi-node cluster with automatic failover

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

### Module Structure

```
src/
├── main.rs           # Entry point, signal handling, config loading
├── config/           # Configuration (TOML-based)
│   └── mod.rs        # Config struct with raft + server_addr + log
├── server/           # TCP server implementation
│   └── server.rs     # Server struct with Raft node integration
├── error/            # Error types
│   └── mod.rs        # Error definitions
├── protocol/         # Redis protocol implementation
│   ├── command.rs    # Command trait & factory
│   ├── resp.rs       # RESP parser & Value enum
│   ├── connection/   # Connection commands (PING)
│   ├── key/          # Key commands (DEL, EXISTS, EXPIRE, PEXPIRE, TYPE)
│   ├── string/       # String commands (GET, SET, APPEND, INCR, DECR, MGET, MSET, SETEX, PSETEX, SETNX, STRLEN)
│   ├── hash/         # Hash commands (HSET, HGET, HDEL, HEXISTS, HGETALL, HKEYS, HLEN, HMGET, HSETNX, HVALS, HINCRBY)
│   ├── list/         # List commands (LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE)
│   ├── set/          # Set commands (SADD)
│   ├── zset/         # Sorted set commands (ZADD)
│   └── bitmap/       # Bitmap commands (SETBIT, GETBIT)
├── encoding/         # Storage encoding
│   ├── string.rs     # StringValue encoding (flags|expires_at|data)
│   ├── hash.rs       # HashMetadata & HashFieldValue encoding
│   ├── list.rs       # ListMetadata & ListElementValue encoding
│   ├── set.rs        # SetMetadata & SetMemberValue encoding
│   ├── zset.rs       # ZSetMetadata & ZSetMemberValue encoding
│   ├── bitmap.rs     # BitmapMetadata & BitmapFragment encoding
│   ├── bloomfilter.rs # BloomFilterMetadata & BloomFilterSubKey encoding
│   ├── hyperloglog.rs # HyperLogLogMetadata & HyperLogLogSubKey encoding
│   └── json.rs       # JsonMetadata encoding (flags|expires_at|format|payload)
├── util/             # Utilities
│   ├── mod.rs        # Module declarations
│   └── time.rs       # now_ms() for timestamp
tests/
├── base_test.py      # TestClusterBase class
├── cluster_manager.py # ClusterManager for build/start/stop/clean
├── run_all_tests.py  # Runs all test files
├── test_cluster_string.py
├── test_cluster_hash.py
├── test_cluster_list.py
├── test_cluster_set.py
├── test_cluster_zset.py
└── test_cluster_bitmap.py
```

## Coding Style

### General Rules
- **Indentation**: 2 spaces (configured in `rustfmt.toml`)
- **Max line width**: 100 characters
- **Rust edition**: 2024
- **Rust version**: 1.91.0 (see `rust-toolchain.toml`)

### Type Import Rules

**DO NOT** use long path references to types directly in code:

```rust
// ❌ Wrong
pub fn to_openai_tool(&self) -> async_openai::types::chat::ChatCompletionTools {
    // ...
}

// ❌ Wrong
pub fn process_response(response: async_openai::types::chat::CreateChatCompletionResponse) {
    // ...
}
```

**MUST** use `use` to import types at the top of the file, then use short names:

```rust
// ✅ Correct
use async_openai::types::chat::ChatCompletionTools;

pub fn to_openai_tool(&self) -> ChatCompletionTools {
    // ...
}

// ✅ Correct
use async_openai::types::chat::CreateChatCompletionResponse;

pub fn process_response(response: CreateChatCompletionResponse) {
    // ...
}
```

### Code Patterns

#### Command Implementation Pattern
All Redis commands follow this pattern:

```rust
// 1. Define params struct
#[derive(Debug, Clone, PartialEq)]
pub struct XxxParams {
    pub key: String,
    // ... other fields
}

impl XxxParams {
    // 2. Parse from RESP items
    fn parse(items: &[Value]) -> Option<Self> {
        // Parse and validate arguments
    }
}

// 3. Define command struct
pub struct XxxCommand;

// 4. Implement Command trait
#[async_trait]
impl Command for XxxCommand {
    async fn execute(&self, items: &[Value], server: &Server) -> Value {
        let params = match XxxParams::parse(items) {
            Some(params) => params,
            None => return Value::error("ERR wrong number of arguments for 'xxx' command"),
        };
        
        // Execute logic
        match server.operation(...).await {
            Ok(result) => Value::BulkString(Some(result)),
            Err(e) => Value::error(format!("ERR {}", e)),
        }
    }
}

// 5. Register in CommandFactory::init()
factory.register("XXX", XxxCommand);
```

#### Storage Encoding Pattern
Data types use postcard for serialization:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XxxValue {
    pub flags: u8,        // high 4 bits = version, low 4 bits = type
    pub expires_at: u64,  // 0 means no expiration
    pub data: Vec<u8>,
}

impl XxxValue {
    pub fn new(data: impl Into<Vec<u8>>) -> Self { /* ... */ }
    pub fn with_expiration(data: impl Into<Vec<u8>>, expires_at: u64) -> Self { /* ... */ }
    pub fn serialize(&self) -> Vec<u8> { postcard::to_allocvec(self).unwrap() }
    pub fn deserialize(bytes: &[u8]) -> Result<Self, DecodeError> { /* ... */ }
    pub fn is_expired(&self, now_ms: u64) -> bool { /* ... */ }
}
```

### Error Handling
- Use `Value::error(msg)` for Redis protocol errors
- Use `Result<T, String>` for internal operations
- Expired keys return `Value::BulkString(None)` (nil in Redis terms)

### Naming Conventions
- **Structs**: PascalCase (`SetCommand`, `StringValue`)
- **Functions/Methods**: snake_case (`parse_args`, `is_expired`)
- **Constants**: SCREAMING_SNAKE_CASE (`NO_EXPIRATION`, `CURRENT_VERSION`)
- **Modules**: snake_case (`hash`, `string`)

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime |
| `rockraft` | Raft consensus implementation |
| `rocksdb` | Embedded storage (via rockraft) |
| `postcard` | Compact serialization |
| `serde` | Serialization framework |
| `async-trait` | Async trait support |
| `tracing` | Structured logging |

## Testing

### Unit Tests
- Located inline in source files under `#[cfg(test)]` modules
- Run with: `cargo test`

### Integration Tests
- Located in `tests/` directory
- Python-based using `redis-py` client
- Tests cluster functionality, replication, persistence

```bash
cd tests
pip install -r requirements.txt

# Run all tests
python run_all_tests.py

# Or run individual test files
python test_cluster_string.py  # String command tests
python test_cluster_hash.py    # Hash command tests
python test_cluster_list.py    # List command tests
python test_cluster_set.py     # Set command tests
python test_cluster_zset.py    # Sorted set command tests
python test_cluster_bitmap.py  # Bitmap command tests
```

### Manual Cluster Testing
```bash
cd tests
./start.sh start    # Start 3-node cluster
./start.sh status   # Check status
./start.sh stop     # Stop cluster
./start.sh clean    # Clean data
```

## Adding New Commands

**⚠️ IMPORTANT: Before implementing any command, ALWAYS search for the command's exact format on https://redis.io/ to understand its syntax, arguments, and return value.**

1. **Create command file** in appropriate subdirectory:
   - String commands: `src/protocol/string/`
   - Hash commands: `src/protocol/hash/`
   - List commands: `src/protocol/list/`
   - Set commands: `src/protocol/set/`
   - Sorted set commands: `src/protocol/zset/`
   - Bitmap commands: `src/protocol/bitmap/`
   - Key commands: `src/protocol/key/`
   - Connection commands: `src/protocol/connection/`
   - Other types: create new subdirectory

2. **Implement Command trait** following the pattern above

3. **Export in mod.rs**:
   ```rust
   // src/protocol/string/mod.rs or src/protocol/hash/mod.rs etc.
   pub use xxx::XxxCommand;
   ```

4. **Register in CommandFactory** (`src/protocol/command.rs`):
   ```rust
   factory.register("XXX", XxxCommand);
   ```

5. **Add Unit Tests** (REQUIRED):
   - Test params parsing with valid inputs
   - Test params parsing with invalid inputs (error cases)
   - Test each option/flag combination
   - Place tests in `#[cfg(test)]` module at the bottom of the command file

6. **Add Integration Tests** (REQUIRED):
   - Create test methods in `tests/test_cluster_<type>.py`
   - **MUST use redis-py standard API only** (e.g., `r.get()`, `r.hset()`, `r.set()`)
   - **NEVER use `execute_command()`** to send raw commands - this bypasses redis-py's validation and may hide protocol incompatibilities
   - Test basic functionality (e.g., SET/GET for string commands)
   - Test edge cases (empty values, large values, special characters)
   - Test error handling (wrong args, invalid inputs)
   - Run `python run_all_tests.py` to verify all tests pass

7. **Update README.md**: Mark command as ✅ in the commands table

**⚠️ IMPORTANT: Every new command MUST include both unit tests AND integration tests! Tests are not optional!**

**⚠️ CRITICAL: Integration tests MUST use redis-py standard API. Using `execute_command()` is strictly prohibited as it hides compatibility issues!**

## Configuration

Example config file (`conf/node1.toml`):

```toml
node_id = 1
server_addr = "0.0.0.0:6379"

[raft]
address = "127.0.0.1:7001"
advertise_host = "localhost"
single = true   # Set to false for cluster mode
join = []       # Addresses to join for cluster mode

[rocksdb]
data_path = "/tmp/coredb/node1"
max_open_files = 10000

[log]
level = "info"
```

## Code Quality Requirements

**All code changes MUST pass the following checks before submission:**

### Formatting Check
```bash
# Check code formatting
cargo fmt --all -- --check

# Auto-fix formatting issues
cargo fmt --all
```

### Clippy Check
```bash
# Run clippy (treat warnings as errors)
cargo clippy --all-features -- -D warnings
```

**⚠️ IMPORTANT: After every code change, ensure both commands pass with ZERO errors and ZERO warnings!**

### New Command Test Gate

When adding a new command, both unit tests AND integration tests MUST pass before the task is considered complete:

```bash
# Unit tests (must pass)
cargo test

# Integration tests (must pass)
cd tests && python run_all_tests.py
```

**⚠️ IMPORTANT: A new command is NOT done until `cargo test` AND `python run_all_tests.py` both exit with zero failures!**

## Build & Run

```bash
# Build
cargo build

# Run single node
cargo run -- --conf {config file}
```

## Important Notes

### Expiration Handling
- Expiration timestamps are in **milliseconds** (Unix timestamp)
- Keys are **lazily deleted** on read access
- `NO_EXPIRATION` (0) means never expire

### Data Type Constants

| Constant | Value | Type |
|----------|-------|------|
| `TYPE_STRING` | `0x01` | String |
| `TYPE_HASH` | `0x02` | Hash |
| `TYPE_LIST` | `0x03` | List |
| `TYPE_SET` | `0x04` | Set |
| `TYPE_ZSET` | `0x05` | Sorted Set |
| `TYPE_BITMAP` | `0x06` | Bitmap |
| `TYPE_JSON` | `0x0A` | JSON |
| `TYPE_BLOOMFILTER` | `0x09` | Bloom Filter |
| `TYPE_HYPERLOGLOG` | `0x0B` | HyperLogLog |

### Storage Layout Patterns

Two patterns are used depending on data type complexity:

#### Simple Types (single key)
**String, JSON** — all data stored at the key itself.
- `key` → `flags|expires_at|[format|]payload`

#### Complex Types (metadata + sub-keys)
**Hash, List, Set, ZSet, Bitmap, BloomFilter, HyperLogLog** — metadata at the key, sub-items stored as separate keys with version for fast deletion.
- Sub-keys are hex-encoded as `key_len|key|version|sub_key_part`
- Incrementing the version invalidates all existing sub-keys instantly

#### Hash Storage Layout
- **Metadata**: stored at `key` — contains flags, expires_at, version, size
- **Field-Value**: stored at hex-encoded `key|version|field` — contains only value data
- Version is used for fast deletion (increment version to invalidate all fields)

#### List Storage Layout
- **Metadata**: stored at `key` — contains flags, expires_at, version, head_seq, tail_seq, size
- **Elements**: stored at hex-encoded `key|version|seq_number` — contains element data
- Uses sequence numbers (head_seq/tail_seq) for LPUSH/RPUSH operations

#### Set Storage Layout
- **Metadata**: stored at `key` — contains flags, expires_at, version, size
- **Members**: stored at hex-encoded `key|version|member` — empty value (existence = membership)

#### Sorted Set Storage Layout
- **Metadata**: stored at `key` — contains flags, expires_at, version, size
- **Members**: stored at hex-encoded `key|version|member` — contains 8-byte big-endian f64 score

#### Bitmap Storage Layout
- **Metadata**: stored at `key` — contains flags, expires_at, version
- **Fragments**: stored at hex-encoded `key|version|fragment_index` — 1 KiB fragments (8192 bits each)
- Inspired by Linux virtual memory paging; fragment_index = bit_offset / 8192
- LSB (Least Significant Bit) numbering within each fragment

#### Bloom Filter Storage Layout
- **Metadata**: stored at `key` — contains flags, expires_at, version, filter_options (n, p)
- **Sub-keys**: stored at hex-encoded `key|version|sub_key_index` — layered/cascading filter data

#### HyperLogLog Storage Layout
- **Metadata**: stored at `key` — contains flags, expires_at, version
- **Segments**: stored at hex-encoded `key|version|segment_index` — 16 segments of 768 bytes each
- Each segment holds 1024 registers × 6 bits; uses MurmurHash2 for hashing
- 14 bits for register index, 50 bits for leading zeros count

#### JSON Storage Layout
- **Metadata**: stored at `key` — contains flags, expires_at, format (0=JSON, 1=CBOR reserved), payload
- Simple single-key layout like String, but with an extra format byte

### Raft Integration
- All writes go through Raft consensus via `server.set()` / `server.delete()`
- **Batch writes** use `server.batch_write()` for atomic multi-key operations (rockraft 0.1.4+)
- Reads can be local via `server.get()` (with optional consistency levels)
- The `rockraft` crate handles leader election, log replication, and state machine

### Batch Atomic Writes (rockraft 0.1.4+)

**Rule**: Any command that modifies multiple data items MUST use `batch_write()` for atomic all-or-nothing semantics.

This ensures either all operations succeed together, or none are applied.

#### Implementation

```rust
// Prepare all entries to modify
let mut entries: Vec<rockraft::raft::types::UpsertKV> = Vec::new();

// Add all field operations
for (field, value) in fields {
    entries.push(rockraft::raft::types::UpsertKV::insert(key, value));
}

// Add metadata update
entries.push(rockraft::raft::types::UpsertKV::insert(meta_key, meta_value));

// Atomic batch write - all or nothing
if let Err(e) = server.batch_write(entries).await {
    return Value::error(format!("ERR batch write failed: {}", e));
}
```

#### Testing Requirements

Commands using batch write MUST include `all_or_none` type integration tests:

```python
def test_xxx_all_or_none(self) -> bool:
    """Test that multi-key operations are atomic (all or nothing).
    
    Verifies that when a command modifies multiple items,
    either all modifications are applied, or none are.
    """
    # Setup: prepare data
    # Operation: execute multi-key command
    # Verify: check all items modified (not partial)
```

#### Examples

| Command | Batch Write Usage | Test Type |
|---------|-------------------|-----------|
| HSET (multiple fields) | Fields + metadata | `test_hset_atomicity_batch_consistency` |
| HDEL (multiple fields) | Deletions + metadata | `test_hdel_atomicity_batch_consistency` |
| HSETNX | Field + metadata | `test_hsetnx_atomicity_field_creation` |

## Common Tasks

### Adding a New Data Type

1. Create encoding module in `src/encoding/`
2. Define metadata struct and value struct
3. Implement serialization/deserialization
4. Create command handlers in `src/protocol/<type>/`
5. Register commands in `CommandFactory`

### Modifying Protocol Behavior

- RESP parsing: `src/protocol/resp.rs`
- Command routing: `src/protocol/command.rs`
- Error responses: Use `Value::error(msg)` with proper Redis error format

### Debugging

Enable debug logging:
```bash
RUST_LOG=debug cargo run -- --conf {config file}
```

Log levels: `trace`, `debug`, `info`, `warn`, `error`
