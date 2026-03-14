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
├── protocol/         # Redis protocol implementation
│   ├── command.rs    # Command trait & factory
│   ├── resp.rs       # RESP parser & Value enum
│   ├── connection/   # Connection commands (PING)
│   ├── string/       # String commands (GET, SET)
│   └── hash/         # Hash commands (HGET, HSET, HDEL, HEXISTS, HGETALL)
├── encoding/         # Storage encoding
│   ├── string.rs     # StringValue encoding
│   └── hash.rs       # HashMetadata & HashFieldValue encoding
└── util/             # Utilities
    └── time.rs       # now_ms() for timestamp
```

## Coding Style

### General Rules
- **Indentation**: 2 spaces (configured in `rustfmt.toml`)
- **Max line width**: 100 characters
- **Rust edition**: 2024
- **Rust version**: 1.91.0 (see `rust-toolchain.toml`)

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

1. **Create command file** in appropriate subdirectory:
   - String commands: `src/protocol/string/`
   - Hash commands: `src/protocol/hash/`
   - Other types: create new subdirectory

2. **Implement Command trait** following the pattern above

3. **Export in mod.rs**:
   ```rust
   // src/protocol/string/mod.rs or src/protocol/hash/mod.rs
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
   - Test basic functionality (e.g., SET/GET for string commands)
   - Test edge cases (empty values, large values, special characters)
   - Test error handling (wrong args, invalid inputs)
   - Run `python run_all_tests.py` to verify all tests pass

7. **Update README.md**: Mark command as ✅ in the commands table

**⚠️ IMPORTANT: Every new command MUST include both unit tests AND integration tests! Tests are not optional!**

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

### Hash Storage Layout
- **Metadata**: stored at `key` - contains flags, expires_at, version, size
- **Field-Value**: stored at hex-encoded `key|version|field` - contains only value data
- Version is used for fast deletion (increment version to invalidate all fields)

### Raft Integration
- All writes go through Raft consensus via `server.set()` / `server.delete()`
- Reads can be local via `server.get()` (with optional consistency levels)
- The `rockraft` crate handles leader election, log replication, and state machine

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
