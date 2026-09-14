# CoreDB Performance Benchmark

Compares CoreDB against native Redis using the same tool (`redis-benchmark`) with
identical parameters, so results are directly comparable and can be referenced
against public Redis benchmark data.

## What it answers

| Comparison | Question |
|---|---|
| CoreDB single vs native Redis | Cost of strong consistency (Raft) + RocksDB vs pure in-memory |
| CoreDB 3-node vs CoreDB single | Overhead of Raft replication |
| CoreDB vs Redis write throughput | Note: different architectures — CoreDB replicates every write to all replicas with majority ack; Redis standalone has no replication. Read throughput is comparable; write throughput difference is an architecture property, not an implementation defect. |

## Quick start

```bash
# 1. Build (or let the script build)
cargo build --release

# 2. Smoke run (small n, 1 iteration, ~a few minutes)
cd bench && ./run_bench.sh --quick

# 3. Full matrix (single + cluster + redis + log-level compare)
cd bench && ./run_bench.sh

# Subset runs
./run_bench.sh --coredb-only
./run_bench.sh --coredb-single
./run_bench.sh --redis-only
```

Results are written to `bench/results/<timestamp>/`:

- `summary.csv` — every iteration + `MEDIAN` rows per scenario
- `raw/` — raw redis-benchmark CSV output per run, plus server logs

## Methodology

- **Tool**: `redis-benchmark --csv -q` (Redis 8.x emits 8 columns per row; the
  parser uses rps / avg / p50 / p99 and skips the header row).
- **PING variant**: uses `-t PING_MBULK` (a RESP array `*1 $4 PING`) rather than
  `-t PING`, whose wire format (inline vs array) varies across redis-benchmark
  versions. Both CoreDB and Redis support the RESP-array form.
- **Iterations**: each scenario runs `BENCH_ITERATIONS` (default 3) times; the
  script reports the median.
- **Matrix**:
  - Commands: `PING, SET, GET, INCR, HSET, LPUSH, SADD, ZADD, MSET, GETSET`
    (`GETSET` runs as an explicit command form — it is not a built-in `-t` test;
    it uses a fixed 64B value)
  - Value sizes: 64B / 1KB / 16KB (PING/GETSET excluded from the size sweep)
  - Connections: 1 / 10 / 50 / 100 (SET, GET)
  - Pipelines: 1 / 16 / 64 (SET, GET)
  - Prefilled read: 1M keys preloaded, then random GET (`-r 1000000`)
- **Random keys**: all key-writing scenarios use `-r 1000000` so keys are spread
  over a 1M keyspace (random distribution, no sequential locality).
- **Warmup**: 5k SET requests at pipeline 16 before any measurement.
- **Per-run timeout**: each redis-benchmark run is guarded by `BENCH_TIMEOUT`
  (default 600s) via `timeout`/`gtimeout`. A timeout is itself a finding — it
  means that scenario has an extremely slow path; check the raw logs.

## Environment controls

- **Release build only**: the script refuses to proceed without
  `target/release/coredb` (dev builds are meaningless).
- **RUST_LOG=warn**: CoreDB logs every incoming command at `info` level,
  including a full debug print of the value. This is expensive. The script
  forces `RUST_LOG=warn` for all measurement runs. Note: the `[log] level`
  config key is *not* wired to the subscriber in `main.rs` — only `RUST_LOG`
  takes effect. The `all` mode includes a dedicated `warn vs info` comparison
  (`mode=log-warn` / `mode=log-info` rows) to quantify this cost; expect a
  large gap. This is a candidate optimization, not a benchmarking artifact.
- **Native Redis**: `--save '' --appendonly no` (pure in-memory baseline). For a
  persistence-fair comparison you may rerun with `--appendonly yes`.
- **Data accumulation**: neither system is flushed between scenarios — CoreDB
  has no FLUSHALL. Both systems accumulate data across scenarios identically,
  which keeps the comparison fair.
- **Ports**: CoreDB bench nodes use 16379-16381 (Raft 17371-17373), native Redis
  uses 16999 — deliberately disjoint from the `tests/` cluster (6379-6381) so
  both can coexist.

## Tunables (env vars)

| Variable | Default | Meaning |
|---|---|---|
| `BENCH_N` | 50000 | requests per run |
| `BENCH_CLIENTS` | 50 | default connection count |
| `BENCH_ITERATIONS` | 3 | runs per scenario (median reported) |
| `BENCH_TIMEOUT` | 600 | per-run timeout seconds |
| `BENCH_DATAKEYS` | 1000000 | random key range + prefill size |
| `BENCH_CMDS` | see script | command matrix |
| `BENCH_SWEEP_CLIENTS` | 1,10,50,100 | connection sweep |
| `BENCH_PIPELINES` | 1,16,64 | pipeline sweep |
| `BENCH_SIZES` | 64,1024,16384 | value-size sweep |

## Reading the results

`summary.csv` columns:

```
system,mode,command,clients,pipeline,value_size,iter,rps,avg_latency_ms,p50_ms,p99_ms
```

- `system` = coredb | redis; `mode` = single | cluster | standalone | log-warn | log-info
- Rows with `iter` = 1..N are raw iterations; `MEDIAN` rows aggregate them
- A scenario with `rps=0` and `command=ERR` means the run failed or timed out —
  check `raw/` and the CoreDB server logs in `raw/coredb_node*.log`

## Tips

- Run on an otherwise idle machine; macOS background tasks add noise.
- For CPU profiling of a slow scenario, use `cargo flamegraph` while replaying
  the offending scenario with redis-benchmark manually.
- To compare against public Redis numbers, use the same redis-benchmark version
  and note the CPU model.