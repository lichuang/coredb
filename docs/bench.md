# CoreDB 性能测试记录与问题清单

> 记录一次完整性能测试的结果、观察到的问题与根因分析，供后续逐项修复和复测。
>
> 测试时间：2026-09-20
> 测试入口：`bench/run_bench.sh --quick`
> 测试结果目录：`bench/results/20260920-123837/`
> 环境：macOS (darwin)、Redis 8.2.1、`redis-benchmark 8.2.1`、release build

---

## 1. 测试方法

- 工具：`redis-benchmark --csv -q`，CoreDB 与原生 Redis 使用完全相同的参数，保证可比性。
- 集群：CoreDB 3 节点（:16379-16381，Raft :17371-17373），配置见 `bench/conf/node*.toml`。
- 原生 Redis：:16999，`--save '' --appendonly no`（纯内存基线）。
- quick 模式参数：`n=20000`、`iterations=1`、`prefill keys=100000`、`RUST_LOG=warn`。
- 对照组说明：写路径所有场景用 `-r 100000` 随机 key；`GETSET` 用固定 64B value。

---

## 2. 测试结果（quick run，MEDIAN 行）

### 2.1 核心场景对比

| 场景 | CoreDB single | CoreDB cluster | Redis standalone |
|---|---|---|---|
| PING | 85,106 | 96,618 | 106,383 |
| GET d64 | 96,621 | **753** ⚠️ | 97,814 |
| GET d1024 | 99,503 | 96,154 | 94,787 |
| GET d16384 | 85,837 | 89,686 | 90,090 |
| SET d64 | 48,662 | **415** ⚠️ | 105,263 |
| SET d1024 | 43,668 | 23,474 | 97,561 |
| SET d16384 | 15,397 | 4,811 | 76,336 |
| INCR d64 | 60,606 | 32,000 | 109,890 |
| HSET d64 | 52,219 | 28,409 | 100,503 |
| LPUSH d64 | 56,497 | 29,412 | 102,564 |
| SADD d64 | 53,050 | 28,531 | 111,111 |
| ZADD d64 | 54,645 | 30,675 | 101,010 |
| MSET(10) d64 | 31,397 | 17,794 | 72,993 |
| MSET(10) d16384 | 1,843 | **95** ⚠️ | 15,699 |
| GETSET d64 | 48,900 | 26,144 | 104,712 |

### 2.2 连接数 / pipeline / 大 value 扫描（SET/GET）

| 维度 | CoreDB single | CoreDB cluster | Redis standalone |
|---|---|---|---|
| SET c=1 | 47,733 | **290** ⚠️ | 106,383 |
| SET c=10 | 49,261 | **267** ⚠️ | 103,093 |
| SET c=50 | 48,662 | **415** ⚠️ | 105,263 |
| SET c=100 | 48,900 | **338** ⚠️ | 100,000 |
| SET P=1 | 48,662 | 415 | 105,263 |
| SET P=16 | 46,083 | **321** ⚠️ | 91,324 |
| SET P=64 | 46,189 | **362** ⚠️ | 102,041 |
| GET P=64 | 77,519 | 635 | 90,498 |

### 2.3 日志级别开销（CoreDB single，SET d64）

| 模式 | rps | 说明 |
|---|---|---|
| `RUST_LOG=warn` | 49,628 | bench 强制使用的级别 |
| `RUST_LOG=info` | 40,241 | 默认级别，约 **-19%** |

> 注意：这是短跑（n=20000）结果；长跑 + 大 value 时每条命令都 Debug 打印完整 value，差距会被放大。

### 2.4 结论性判断

- **单节点读性能已与 Redis 持平**（GET ~97k vs Redis ~98k）。
- **单节点写约为 Redis 的一半**，符合 Raft + RocksDB 的架构成本。
- **集群写路径是当前最大短板**：leader 直连约 32k rps，但通过 follower 转发只有 300~600 rps，差距 **50~100 倍**。

---

## 3. 发现的问题

### P0 🔴 集群写操作慢 50~100 倍（核心问题）

**现象（对照实验，`advertise_host=127.0.0.1` 干净环境下）**

| 写入目标 | rps | p99 |
|---|---|---|
| leader 节点（如 16479） | 32,000 | 2.1ms |
| follower 节点（16480） | 609 | 3000ms |
| follower 节点（16481） | 322 | 3000ms |

**根因（rockraft 0.1.8 `src/node/forward.rs`）**

1. `assume_leader()` → `get_leader()`：**每次写都**等待 metrics watch，deadline 2000ms。
2. `forward_request_to_leader()` → `send_forward_request()`：**每个转发请求都新建一个 gRPC channel**（`JoinConnectionFactory::create_rpc_channel` → `endpoint.connect_with_connector().await`），没有复用连接池。
3. 失败后 `execute_or_forward` 走 `MAX_RETRIES=20` + 指数退避（`RETRY_INITIAL_INTERVAL=200ms` → `RETRY_MAX_INTERVAL=3s`），单请求最坏可达几十秒。

**解释的现象**

- 只有 leader 快，任何非 leader 入口都慢。
- benchmark 中大负载场景（如 `MSET d16384`）之后，集群从 ~30k 掉到 ~300 rps 且无法恢复；`p99=3000.319ms` 正是 3s 重试上限的特征值。

**建议方向**

- 转发复用已有连接池（`ClientPool`），而不是每请求新建 channel。
- 减少每次写的 leader 探测开销（缓存 leader / 用 watch 而非每次 2s 等待）。
- 评估是否需要在 CoreDB 侧做「写请求一律路由到 leader」的客户端亲和，减少转发。

---

### P1 🔴 `advertise_host = "localhost"` 在 macOS 上导致 gRPC 连接失败

**现象**

`bench/conf/node2.toml`、`node3.toml` 使用 `advertise_host = "localhost"`。macOS 上 `localhost` 同时解析到 `::1`(IPv6) 和 `127.0.0.1`，而节点监听的是 IPv4；rockraft 使用 hickory DNS resolver，存在解析到 `::1` 导致连接失败的情况。

**A/B 对照证据**

| advertise_host | `Failed to connect to localhost:PORT` 次数 | leader 变更 |
|---|---|---|
| `localhost` | **4013**（node1） | 5 次 |
| `127.0.0.1` | **0** | 0 次 |

**受影响文件**

- `bench/conf/node1.toml` / `node2.toml` / `node3.toml`

**建议**

- 配置改为 `advertise_host = "127.0.0.1"`。
- 或推动 rockraft 支持 IPv4 优先解析。

---

### P2 🟠 每条命令都 Debug 打印完整 value，且 `config.log.level` 未生效

**证据**

- `src/server/server.rs:192`
  ```rust
  info!("Received command from {}: {:?}", peer_addr, value);
  ```
  每条命令都打印，包含完整 value（大 value 时尤其昂贵）。
- `src/main.rs:21-28` 只用 `RUST_LOG` 初始化 `EnvFilter`，**从未读取 `config.log.level`**。
- `src/config/mod.rs` 中 `LogConfig { file, level }` 定义完整，但 `file` 与 `level` 在 `src/` 内均无引用。
- 实测开销：`info` vs `warn` 约 **-19%**（见 2.3）。

**建议**

- 删除或降级 `server.rs:192` 的逐命令日志。
- 把 `config.log.level` 接入 `EnvFilter`；`LogConfig.file` 要么实现要么移除，避免误导。

---

### P3 🟠 集群 `MSET d16384` 灾难级慢，并会拖垮后续所有场景

**现象**

| 场景 | CoreDB single | CoreDB cluster |
|---|---|---|
| MSET(10) d16384 | 1,843 rps | **94.8 rps** |
| p50 / p99（cluster） | — | 72ms / 660ms |

触发后集群整体退化：后续 SET d64 从 ~30k 直接掉到 317 rps。

**相关代码**

- `src/protocol/string/mset.rs:59-65`：N 个 key 打包成一次 `batch_write`，每个 entry 重新序列化。
- 集群上一次 batch 需要跨节点复制；一旦失败即触发 P0 的重试风暴。

**建议**

- 复测确认是否由 P0 的转发/重试导致；若是，修复 P0 后再评估。
- 评估 batch 大小上限 / 分片策略。

---

### P4 🟡 大 value 与 read-modify-write 路径缺少优化

- **大 value**：SET d16384 single 仅 15k rps（Redis 76k），MSET d16384 single 1.8k（Redis 15.7k）。value 越大，`Vec<u8>` clone + postcard 序列化 + 逐命令日志的放大越明显。
- **SET 无条件预读**：`src/protocol/string/set.rs:226` 无条件执行 `server.get()`，用于支持 NX/XX/GET，即使客户端未传这些选项也要读一次（且这是走 Raft 的读）。
- **INCR 非原子 RMW**：`src/protocol/string/incr.rs:48-91` 是 read-then-write，不是原子 read-modify-write。

**建议**

- 仅当存在 `NX/XX/GET/KEEPTTL` 选项时才预读。
- 评估把 INCR/GETSET 等 RMW 收敛为单次事务（rockraft 已提供 `txn`）。

---

### P5 🟡 bench 脚本自身缺陷

1. **汇总列偏移错位**
   - `bench/run_bench.sh:449-451` 打印 `$8`，但 MEDIAN 行格式为
     `MEDIAN,coredb,single,SET,50,1,64,<空>,rps,...`
     实际 rps 在 `$9`。导致终端 "Median RPS" 全部为空。
2. **节点启动时序竞争**
   - `bench/run_bench.sh:218-221` 中 node2/node3 几乎同时 join，会撞上 Raft
     `the cluster is already undergoing a configuration change` 冲突；
     实测可导致 node2 直接启动失败。
   - 应改为：启动一个节点 → `wait_ready` 确认 → 再启动下一个。

---

## 4. 环境 / 误报说明

- 上一轮结果 `bench/results/20260920-120954/` 中 Redis 全部为 `ERR`、`log-warn` 为空，
  属于**环境问题**（当时 :16999 端口不可用），不是 CoreDB 的 bug。
  本次 quick run 中 Redis 全部正常。
- cluster 的 `GET d64 = 753 rps` 是**测量假象**：该场景用 `-r 100000` 随机 key，
  而这些 key 从未写入过，落到未命中/lazy-delete/转发路径；
  对照 `GET d1024 = 96k`、`GET d16384 = 90k` 均为正常值。

---

## 5. 修复优先级建议

| 优先级 | 问题 | 预估成本 | 收益 |
|---|---|---|---|
| 1 | P1 `advertise_host` 改 127.0.0.1 | 极低（配置） | 消除集群噪声与连接失败 |
| 2 | P2 移除逐命令日志 + 接通 `config.log.level` | 低 | 稳定约 +19% 写吞吐 |
| 3 | P0 follower 转发复用连接池 / 减少 leader 探测 | 中高（涉及 rockraft） | 集群吞吐提升 50x+ |
| 4 | P3 MSET 大 value | 中（依赖 P0） | 消除退化雪崩 |
| 5 | P5 bench 脚本列偏移 + 启动时序 | 低 | 结果可读、运行稳定 |
| 6 | P4 SET 预读 / INCR 原子化 | 中 | 单节点写吞吐提升 |

---

## 6. 复现方式

```bash
# 1. 构建
cargo build --release

# 2. quick 跑一遍（结果写入 bench/results/<timestamp>/）
cd bench && ./run_bench.sh --quick

# 3. 查看汇总
cat bench/results/<timestamp>/summary.csv
```

关键原始文件位于 `bench/results/<timestamp>/raw/`：

- `coredb_single_*_iter1.txt` / `coredb_cluster_*_iter1.txt` / `redis_standalone_*_iter1.txt`：redis-benchmark 原始输出
- `coredb_node{1,2,3}.log`：三个 CoreDB 节点日志（含 Raft 转发/连接错误）
- `coredb_node1_loglevel_{warn,info}.log`：日志级别对照
