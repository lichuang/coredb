#!/bin/bash
#
# CoreDB Performance Benchmark Runner
#
# Compares CoreDB (single node / 3-node cluster) against native Redis using
# redis-benchmark. Produces per-scenario CSV files plus a summary report.
#
# Usage:
#   ./run_bench.sh              # full matrix: coredb single + cluster + redis + log-level compare
#   ./run_bench.sh --quick      # smoke run, small n, 1 iteration
#   ./run_bench.sh --coredb-only
#   ./run_bench.sh --redis-only
#   ./run_bench.sh --coredb-single
#
# Results: bench/results/<timestamp>/summary.csv (+ raw outputs)
#
# Tunables via env: BENCH_N, BENCH_CLIENTS, BENCH_ITERATIONS, BENCH_TIMEOUT,
#                   BENCH_DATAKEYS, BENCH_CMDS, BENCH_SWEEP_CLIENTS,
#                   BENCH_PIPELINES, BENCH_SIZES
#
# Requirements: redis-benchmark (>= 7.x, CSV with 8 columns), redis-cli,
#               timeout (or gtimeout on macOS), cargo release build.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."
RESULTS_BASE="$SCRIPT_DIR/results"
BENCH_DATA="/tmp/coredb-bench"
REDIS_DATA="/tmp/redis-bench"

# ---------------- Tunables ----------------
N_REQUESTS="${BENCH_N:-50000}"
DEFAULT_CLIENTS="${BENCH_CLIENTS:-50}"
ITERATIONS="${BENCH_ITERATIONS:-3}"
WARMUP_REQUESTS="${BENCH_WARMUP:-5000}"
DATA_KEYS="${BENCH_DATAKEYS:-1000000}"
BENCH_TIMEOUT="${BENCH_TIMEOUT:-600}"
REDIS_PORT=16999

# Commands tested in the value-size matrix. GETSET uses a fixed 64B value.
BENCH_CMDS="${BENCH_CMDS:-PING,SET,GET,INCR,HSET,LPUSH,SADD,ZADD,MSET,GETSET}"
SWEEP_CMDS="${BENCH_SWEEP_CMDS:-SET,GET}"
SWEEP_CLIENTS="${BENCH_SWEEP_CLIENTS:-1,10,50,100}"
PIPELINE_LEVELS="${BENCH_PIPELINES:-1,16,64}"
VALUE_SIZES="${BENCH_SIZES:-64,1024,16384}"

# timeout wrapper: falls back to no guard when neither timeout nor gtimeout exists
TIMEOUT_BIN=""
command -v timeout  >/dev/null 2>&1 && TIMEOUT_BIN="timeout"
[ -z "$TIMEOUT_BIN" ] && command -v gtimeout >/dev/null 2>&1 && TIMEOUT_BIN="gtimeout"
bench_timeout() {
  local secs="$1"; shift
  if [ -n "$TIMEOUT_BIN" ]; then "$TIMEOUT_BIN" "$secs" "$@"; else "$@"; fi
}

# ---------------- Globals ----------------
TS="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="$RESULTS_BASE/$TS"
RAW_DIR="$OUT_DIR/raw"
mkdir -p "$RAW_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()   { echo -e "${CYAN}[bench]${NC} $*"; }
ok()    { echo -e "${GREEN}[ok]${NC} $*"; }
warn_() { echo -e "${YELLOW}[warn]${NC} $*"; }
die()   { echo -e "${RED}[fail]${NC} $*"; exit 1; }

# ---------------- Result store ----------------
SUMMARY_CSV="$OUT_DIR/summary.csv"
# columns: system,mode,command,clients,pipeline,value_size,iter,rps,avg_ms,p50_ms,p99_ms
echo "system,mode,command,clients,pipeline,value_size,iter,rps,avg_latency_ms,p50_ms,p99_ms" > "$SUMMARY_CSV"

# redis-benchmark --csv row: "test","rps","avg_ms","min_ms","p50_ms","p95_ms","p99_ms","max_ms"
# The first row is the header; rows with a non-numeric rps field are skipped.
parse_csv_line() {
  local system="$1" mode="$2" clients="$3" pipeline="$4" size="$5" iter="$6" line="$7"
  local cmd rps avg p50 p99
  cmd="$(echo "$line" | awk -F'","' '{print $1}' | tr -d '"')"
  rps="$(echo "$line" | cut -d',' -f2 | tr -d '"')"
  case "$rps" in
    ''|*[!0-9.]*) return 0 ;;
  esac
  avg="$(echo "$line" | cut -d',' -f3 | tr -d '"')"
  p50="$(echo "$line" | cut -d',' -f5 | tr -d '"')"
  p99="$(echo "$line" | cut -d',' -f7 | tr -d '"')"
  echo "$system,$mode,$cmd,$clients,$pipeline,$size,$iter,$rps,$avg,$p50,$p99" >> "$SUMMARY_CSV"
}

record_err() {
  local system="$1" mode="$2" clients="$3" pipeline="$4" size="$5" iter="$6"
  echo "$system,$mode,ERR,$clients,$pipeline,$size,$iter,0,0,0,0" >> "$SUMMARY_CSV"
}

# Aggregate per-(system,mode,command,clients,pipeline,size) medians across
# iterations; appends one MEDIAN row per group aligned to the summary columns.
aggregate_medians() {
  awk -F',' '
    NR>1 && $1!="MEDIAN" {
      key=$1","$2","$3","$4","$5","$6
      rps[key]=rps[key]" "$8
      avg[key]=avg[key]" "$9
      p50[key]=p50[key]" "$10
      p99[key]=p99[key]" "$11
      order[++n]=key
    }
    function med(str,  m,a,i,j,t) {
      m=split(str,a," ")
      for(i=1;i<=m;i++) for(j=i+1;j<=m;j++) if(a[j]+0<a[i]+0){t=a[i];a[i]=a[j];a[j]=t}
      if(m%2) return a[(m+1)/2]
      return (a[m/2]+a[m/2+1])/2
    }
    END {
      seen=0
      for(i=1;i<=n;i++){
        k=order[i]
        if(k in done) continue
        done[k]=1
        printf "MEDIAN,%s,,%.1f,%.3f,%.3f,%.3f\n", k, med(rps[k]), med(avg[k]), med(p50[k]), med(p99[k])
      }
    }' "$SUMMARY_CSV" > "$SUMMARY_CSV.medians"
  {
    head -1 "$SUMMARY_CSV"
    tail -n +2 "$SUMMARY_CSV"
    LC_ALL=C sort -t',' -k4,4 -k2,2 -k3,3 "$SUMMARY_CSV.medians"
  } > "$SUMMARY_CSV.new"
  mv "$SUMMARY_CSV.new" "$SUMMARY_CSV"
  rm -f "$SUMMARY_CSV.medians"
}

# ---------------- redis-benchmark wrapper ----------------
# run_bench <system> <mode> <port> <tag> <extra benchmark args...>
# Env: CL (clients), PL (pipeline), SZ (recorded value size)
run_bench() {
  local system="$1" mode="$2" port="$3" tag="$4"; shift 4
  local iter

  for iter in $(seq 1 "$ITERATIONS"); do
    local raw="$RAW_DIR/${system}_${mode}_${tag}_iter${iter}.txt"
    bench_timeout "$BENCH_TIMEOUT" redis-benchmark -h 127.0.0.1 -p "$port" \
      -n "$N_REQUESTS" --csv -q "$@" > "$raw" 2>/dev/null
    local rc=$?
    if [ $rc -ne 0 ] || [ ! -s "$raw" ]; then
      if [ $rc -eq 124 ]; then
        warn_ "TIMEOUT (${BENCH_TIMEOUT}s) for ${system}/${mode}/${tag} iter${iter} — possible slow path, investigate this scenario"
      else
        warn_ "no output for ${system}/${mode}/${tag} iter${iter} (rc=$rc)"
      fi
      record_err "$system" "$mode" "$CL" "$PL" "$SZ" "$iter"
      continue
    fi
    local line
    while IFS= read -r line; do
      [ -z "$line" ] && continue
      parse_csv_line "$system" "$mode" "$CL" "$PL" "$SZ" "$iter" "$line"
    done < "$raw"
  done
}

# ---------------- Server lifecycle ----------------
COREDB_PIDS=()

stop_everything() {
  local pid
  for pid in "${COREDB_PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
  done
  if [ -n "${REDIS_PID:-}" ]; then
    redis-cli -h 127.0.0.1 -p "$REDIS_PORT" shutdown nosave >/dev/null 2>&1
    kill "$REDIS_PID" 2>/dev/null
  fi
  # belt-and-suspenders: nothing named coredb should survive on bench ports
  sleep 1
  for port in 16379 16380 16381; do
    local pids
    pids="$(lsof -ti tcp:"$port" 2>/dev/null || true)"
    [ -n "$pids" ] && kill $pids 2>/dev/null
  done
  rm -rf "$BENCH_DATA"
}
trap stop_everything EXIT

wait_ready() {
  local port="$1" name="$2" tries="${3:-30}"
  for _ in $(seq 1 "$tries"); do
    if redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1; then
      ok "$name ready on :$port"
      return 0
    fi
    sleep 1
  done
  die "$name failed to become ready on :$port"
}

start_coredb_node() {
  local node_id="$1" port="$2"
  local conf="$SCRIPT_DIR/conf/node${node_id}.toml"
  local log="$RAW_DIR/coredb_node${node_id}.log"
  # RUST_LOG=warn: per-command info! logging (with full value debug print) is
  # expensive; config [log] level is not wired to the subscriber, so force via env.
  RUST_LOG=warn nohup "$PROJECT_ROOT/target/release/coredb" --conf "$conf" \
    > "$log" 2>&1 &
  COREDB_PIDS+=($!)
  wait_ready "$port" "coredb node$node_id"
}

start_coredb_single() {
  log "Starting CoreDB single node (:16379)..."
  rm -rf "$BENCH_DATA/node1"
  COREDB_PIDS=()
  start_coredb_node 1 16379
}

start_coredb_cluster() {
  log "Starting CoreDB 3-node cluster (:16379-16381)..."
  rm -rf "$BENCH_DATA/node1" "$BENCH_DATA/node2" "$BENCH_DATA/node3"
  COREDB_PIDS=()
  start_coredb_node 1 16379
  sleep 3
  start_coredb_node 2 16380
  start_coredb_node 3 16381
  sleep 2
  # verify replication: write via node1, read via node2
  redis-cli -h 127.0.0.1 -p 16379 SET __bench_probe__ ok >/dev/null
  sleep 1
  local v
  v="$(redis-cli -h 127.0.0.1 -p 16380 GET __bench_probe__)"
  [ "$v" = "ok" ] && ok "replication verified (node1 -> node2)" \
    || warn_ "replication probe failed (got '$v')"
  redis-cli -h 127.0.0.1 -p 16379 DEL __bench_probe__ >/dev/null
}

start_redis() {
  log "Starting native Redis (:16999)..."
  rm -rf "$REDIS_DATA"
  mkdir -p "$REDIS_DATA"
  redis-server --port "$REDIS_PORT" --save '' --appendonly no \
    --dir "$REDIS_DATA" --daemonize yes \
    --pidfile "$REDIS_DATA/redis.pid" --logfile "$REDIS_DATA/redis.log"
  REDIS_PID="$(cat "$REDIS_DATA/redis.pid")"
  wait_ready "$REDIS_PORT" "native redis"
}

# ---------------- Warmup & data prefill ----------------
warmup() {
  local port="$1"
  log "Warmup ($WARMUP_REQUESTS requests)..."
  bench_timeout "$BENCH_TIMEOUT" redis-benchmark -h 127.0.0.1 -p "$port" \
    -n "$WARMUP_REQUESTS" -c 50 -t set -d 64 -P 16 -q >/dev/null 2>&1 || true
}

prefill_for_read_tests() {
  local port="$1"
  log "Prefilling $DATA_KEYS keys for read-benchmark state..."
  bench_timeout "$BENCH_TIMEOUT" redis-benchmark -h 127.0.0.1 -p "$port" \
    -n "$DATA_KEYS" -c 50 -t set -d 64 -P 64 -q -r "$DATA_KEYS" >/dev/null 2>&1 || true
  log "Prefill done"
}

# ---------------- Matrix execution ----------------
IFS=',' read -r -a CMDS <<< "$BENCH_CMDS"
IFS=',' read -r -a SWEEP_C <<< "$SWEEP_CLIENTS"
IFS=',' read -r -a SWEEP_S <<< "$SWEEP_CMDS"
IFS=',' read -r -a PLVL <<< "$PIPELINE_LEVELS"
IFS=',' read -r -a SIZES <<< "$VALUE_SIZES"

# run_command_matrix <system> <mode> <port>
# Default clients, pipeline 1, one run per value size.
# PING and GETSET do not scale with -d, so they run once (recorded as size 0 / 64).
run_command_matrix() {
  local system="$1" mode="$2" port="$3"
  local size cmd
  for size in "${SIZES[@]}"; do
    for cmd in "${CMDS[@]}"; do
      case "$cmd" in
        PING)
          # PING_MBULK sends a RESP array (*1 $4 PING); the plain PING test
          # name sends the inline form, which differs across redis-benchmark
          # versions. Use the RESP-array variant for a stable comparison.
          [ "$size" != "64" ] && continue
          SZ="0" CL="$DEFAULT_CLIENTS" PL=1 \
            run_bench "$system" "$mode" "$port" "PING" -t PING_MBULK
          ;;
        GETSET)
          [ "$size" != "64" ] && continue
          SZ="64" CL="$DEFAULT_CLIENTS" PL=1 \
            run_bench "$system" "$mode" "$port" "GETSET_d64" -r "$DATA_KEYS" \
            GETSET "key:__rand_int__" "$(printf 'x%.0s' $(seq 1 64))"
          ;;
        MSET)
          SZ="$size" CL="$DEFAULT_CLIENTS" PL=1 \
            run_bench "$system" "$mode" "$port" "${cmd}_d${size}" \
            -t MSET -d "$size" -r "$DATA_KEYS"
          ;;
        *)
          SZ="$size" CL="$DEFAULT_CLIENTS" PL=1 \
            run_bench "$system" "$mode" "$port" "${cmd}_d${size}" \
            -t "$cmd" -d "$size" -r "$DATA_KEYS"
          ;;
      esac
    done
  done
}

run_pipeline_sweep() {
  local system="$1" mode="$2" port="$3"
  local pl cmd
  for pl in "${PLVL[@]}"; do
    for cmd in "${SWEEP_S[@]}"; do
      SZ="64" CL="$DEFAULT_CLIENTS" PL="$pl" \
        run_bench "$system" "$mode" "$port" "${cmd}_P${pl}" \
        -t "$cmd" -d 64 -r "$DATA_KEYS"
    done
  done
}

run_client_sweep() {
  local system="$1" mode="$2" port="$3"
  local cl cmd
  for cl in "${SWEEP_C[@]}"; do
    for cmd in "${SWEEP_S[@]}"; do
      SZ="64" CL="$cl" PL=1 \
        run_bench "$system" "$mode" "$port" "${cmd}_c${cl}" \
        -t "$cmd" -d 64 -r "$DATA_KEYS"
    done
  done
}

run_read_on_prefilled() {
  local system="$1" mode="$2" port="$3"
  prefill_for_read_tests "$port"
  SZ="64" CL="$DEFAULT_CLIENTS" PL=1 \
    run_bench "$system" "$mode" "$port" "GET_prefilled" -t GET -d 64 -r "$DATA_KEYS"
}

# Log-level cost: CoreDB single node with RUST_LOG=warn vs info (SET d=64)
run_log_level_compare() {
  log "Running log-level comparison (warn vs info) on CoreDB single node..."
  local lvl iter line
  for lvl in warn info; do
    stop_everything
    rm -rf "$BENCH_DATA/node1"
    RUST_LOG=$lvl nohup "$PROJECT_ROOT/target/release/coredb" \
      --conf "$SCRIPT_DIR/conf/node1.toml" > "$RAW_DIR/coredb_node1_loglevel_$lvl.log" 2>&1 &
    COREDB_PIDS=($!)
    wait_ready 16379 "coredb (RUST_LOG=$lvl)"
    warmup 16379
    for iter in $(seq 1 "$ITERATIONS"); do
      local raw="$RAW_DIR/coredb_single_log${lvl}_iter${iter}.txt"
      bench_timeout "$BENCH_TIMEOUT" redis-benchmark -h 127.0.0.1 -p 16379 \
        -n "$N_REQUESTS" -c 50 -t SET -d 64 -r "$DATA_KEYS" --csv -q > "$raw" 2>/dev/null
      if [ ! -s "$raw" ]; then
        warn_ "log-level compare ($lvl) iter${iter} produced no output"
        continue
      fi
      while IFS= read -r line; do
        [ -z "$line" ] && continue
        parse_csv_line "coredb" "log-$lvl" 50 1 64 "$iter" "$line"
      done < "$raw"
    done
  done
  stop_everything
}

# ---------------- Suites ----------------
run_coredb_single_suite() {
  start_coredb_single
  warmup 16379
  run_command_matrix "coredb" "single" 16379
  run_pipeline_sweep "coredb" "single" 16379
  run_client_sweep "coredb" "single" 16379
  run_read_on_prefilled "coredb" "single" 16379
}

run_coredb() {
  log "===== CoreDB single node ====="
  run_coredb_single_suite

  log "===== CoreDB 3-node cluster ====="
  stop_everything
  start_coredb_cluster
  warmup 16379
  run_command_matrix "coredb" "cluster" 16379
  run_pipeline_sweep "coredb" "cluster" 16379
  run_client_sweep "coredb" "cluster" 16379
}

run_redis() {
  log "===== Native Redis ====="
  stop_everything
  start_redis
  warmup 16999
  run_command_matrix "redis" "standalone" 16999
  run_pipeline_sweep "redis" "standalone" 16999
  run_client_sweep "redis" "standalone" 16999
  run_read_on_prefilled "redis" "standalone" 16999
}

# ---------------- Main ----------------
MODE="all"
case "${1:-}" in
  --coredb-only)   MODE="coredb" ;;
  --redis-only)    MODE="redis" ;;
  --coredb-single) MODE="coredb-single" ;;
  --quick)         MODE="all"; QUICK=1 ;;
esac
QUICK="${QUICK:-0}"

if [ "$QUICK" = "1" ]; then
  N_REQUESTS=20000
  ITERATIONS=1
  WARMUP_REQUESTS=2000
  DATA_KEYS=100000
  warn_ "quick mode: n=$N_REQUESTS, iters=1, prefill keys=$DATA_KEYS"
fi

log "Project root: $PROJECT_ROOT"
log "Results dir:  $OUT_DIR"
log "n=$N_REQUESTS iters=$ITERATIONS clients=$DEFAULT_CLIENTS sizes=$VALUE_SIZES pipelines=$PIPELINE_LEVELS clients_sweep=$SWEEP_CLIENTS"
[ -z "$TIMEOUT_BIN" ] && warn_ "timeout/gtimeout not found — per-run timeout guard disabled"

if [ ! -x "$PROJECT_ROOT/target/release/coredb" ]; then
  log "Building CoreDB (release)..."
  (cd "$PROJECT_ROOT" && cargo build --release) || die "build failed"
fi

case "$MODE" in
  coredb)        run_coredb ;;
  redis)         run_redis ;;
  coredb-single) log "===== CoreDB single node ====="; run_coredb_single_suite ;;
  all)
    run_coredb
    run_redis
    run_log_level_compare
    ;;
esac

stop_everything
trap - EXIT

aggregate_medians

echo ""
ok "Benchmark complete: $OUT_DIR"
echo "  summary: $SUMMARY_CSV"
echo ""
echo "  Median RPS per scenario:"
echo "  ------------------------"
LC_ALL=C awk -F',' '$1=="MEDIAN" {
  printf "  %-6s %-10s %-24s c=%-4s P=%-3s d=%-6s %s rps\n", $2,$3,$4,$5,$6,$7,$8
}' "$SUMMARY_CSV"
echo ""
log "Note: latency columns in summary.csv are ms; MEDIAN rows aggregate across $ITERATIONS iterations."