# CoreDB 正确性问题清单

> 记录实测复现的正确性缺陷、根因分析与修复方向。
>
> 记录时间：2026-09-21（代码版本 `a54d5a0`）；随后已合入 `SET NX/XX`/`SETNX` 原子化与 `MAX_CAS_RETRIES` 归位 `util/cas.rs`。
> 测试环境：单节点（`127.0.0.1:16679`，Raft `127.0.0.1:17671`），release build
> 关联 issue：[lichuang/coredb#1 Operational atomicity issues](https://github.com/lichuang/coredb/issues/1)

---

## 0. 结论速览

CoreDB 存在**系统性**的并发正确性问题：所有"先读 metadata → 计算 → 写回"的写命令在并发下都会**丢失更新（lost update）**。

这是 issue #1 所描述问题的**同类根因**，但波及范围远大于 issue 中提到的 `SET NX`——hash / list / zset / set / bitmap / string 全部受影响。

| 严重度 | 命令 | 实测现象 | 状态 |
|---|---|---|---|
| 🔴 高 | `HINCRBY` | 100 并发 → 结果 64~68（丢失 ~35%） | ✅ 已修复 |
| 🔴 高 | `APPEND` | 100 并发 → 长度 64（丢失 ~36%） | ✅ 已修复 |
| 🔴 高 | `LPUSH` / `RPUSH` | 50 并发 → 元素 22（丢失 ~56%，数据真丢） | 🔶 LPUSH 已修复 / RPUSH ⬜ |
| 🔴 高 | `LPOP` / `RPOP` | 同 head/tail 竞态，可能重复弹出或漏弹 | ⬜ 未修复 |
| 🟠 中 | `ZADD` | 50 并发 → 48 个成员（丢失成员） | ⬜ 未修复 |
| 🟠 中 | `ZREM` | 同 ZADD 模式 | ⬜ 未修复 |
| 🟠 中 | `HSET` / `HDEL` | 100 并发 → `HLEN` 63、`HKEYS` 99（计数错乱） | ⬜ 未修复 |
| 🟠 中 | `SADD` / `SREM` / `SETBIT` | 元数据 `size` 计数错乱 | ⬜ 未修复 |
| 🟡 低 | `LREM` / `LSET` / `EXPIRE` / `RENAME` / `GETSET` | 同模式，受影响程度待实测 | ⬜ 未修复 |

> 已修复命令的详情见各 §1.x 小节与 §3.4 参考实现表。

---

## 1. 实测证据

单节点，`redis-cli` 并发发起（shell `&` 后台），无客户端库重试逻辑。

### 1.1 HINCRBY：丢失更新

```bash
redis-cli DEL hc
for i in $(seq 1 100); do (redis-cli HINCRBY hc f 1) & done; wait
redis-cli HGET hc f
```

| 轮次 | 期望 | 实际 |
|---|---|---|
| run 1 | 100 | **64** |
| run 2 | 100 | **67** |
| run 3 | 100 | **68** |

稳定性复现，丢失率约 32~36%。

**已修复**：HINCRBY 改为条件事务 CAS——同时锁定 hash metadata（`eq`/`not_exists`）与
field 子键（`eq`/`not_exists`），apply 时两者均未变才写入；输者带退避重试
（`src/protocol/hash/hincrby.rs`）。并发回归测试：
`tests/test_cluster_hash.py::test_hincrby_concurrent_no_lost_updates`
（8 客户端 × 25 次，要求 200 个回复互异 1..200）。

### 1.2 APPEND：丢失更新（数据截断）

```bash
redis-cli DEL ap; redis-cli SET ap ""
for i in $(seq 1 100); do (redis-cli APPEND ap x) & done; wait
redis-cli STRLEN ap
```

结果：**64**（期望 100）。

**已修复**：APPEND 改为条件事务 CAS——以观察到的整值序列化字节作 `TxnCondition::eq`
（不存在时 `not_exists`），apply 时值未变才追加；输者带退避重试
（`src/protocol/string/append.rs`）。并发回归测试：
`tests/test_cluster_string.py::test_append_concurrent_no_truncation`
（8 客户端 × 25 次，要求长度 1..200 互异且内容完整）。

**修复补丁的回归与再修**：首版 CAS 把「key 不存在」与「key 已过期」统一用
`not_exists` 提交，但 rockraft 的条件判定只看原始存储字节、不感知 TTL——过期 key
的字节仍在，条件永远为假，128 次重试全空转（实测客户端 ~59 秒后报
`ERR append retry limit exceeded`）。已拆出 `Expired` 状态：过期 key 的字节用
`TxnCondition::eq(key, 旧字节)` 锁定，apply 时写入**不带旧 TTL** 的全新字符串
（Redis 语义：过期 key 视为不存在）。回归测试：
`tests/test_cluster_string.py::test_append_on_expired_key`
（SET PX → 过期后首个命令是 APPEND，校验值/TTL/二次追加）。

### 1.3 LPUSH：元素互相覆盖（数据真丢）

```bash
redis-cli DEL lp2
for i in $(seq 1 50); do (redis-cli LPUSH lp2 v$i) & done; wait
redis-cli LLEN lp2        # -> 22
redis-cli LRANGE lp2 0 -1 | wc -l   # -> 22
```

`LLEN` 与 `LRANGE` 一致为 22，说明**元素确实丢失**，不是计数错误。

**已修复**：LPUSH 改为条件事务 CAS——以观察到的 metadata 序列化字节作
`TxnCondition::eq`（不存在时 `not_exists`），apply 时 metadata 未变才写入；
输者带退避重试（`src/protocol/list/lpush.rs`）。锁定 metadata 即锁定 `head`
游标：**胜者的 head 索引从未被其他成功 push 使用过（head 只减不增）**，
故子键覆盖不可能发生。过期 list 用 `eq` 锁旧字节、新 list 带新 `version`
（旧子键不可达，同 APPEND 的 TTL 盲区处理）。并发回归测试：
`tests/test_cluster_list.py::test_lpush_concurrent_no_loss`
（8 客户端 × 25 次，要求长度 1..200 互异、LLEN/LRANGE 一致、200 个元素无一丢失）。

### 1.4 ZADD：丢失成员

```bash
redis-cli DEL z2
for i in $(seq 1 50); do (redis-cli ZADD z2 $i m$i) & done; wait
redis-cli ZRANGE z2 0 -1 | wc -l   # -> 48
```

### 1.5 HSET：计数错乱（数据未丢）

```bash
redis-cli DEL hh
for i in $(seq 1 100); do (redis-cli HSET hh f$i v) & done; wait
redis-cli HLEN hh          # -> 63  (错)
redis-cli HKEYS hh | wc -l # -> 99  (接近正确)
```

### 1.6 对照组：看似正确的命令

| 命令 | 100 并发结果 | 说明 |
|---|---|---|
| `SADD sc m$i`（不同成员） | 100 ✅ | **不是原子，是巧合**：成员子键不含序号，不同成员不冲突 |
| `HSETNX hnx f$i v` | 100 ✅ | 同上；但 `HLEN` 计数同样会错，只是未测 |

> ⚠️ `SADD` / `HSETNX` 是"测不出问题"，不是"没有问题"。其元数据 `size` 计数与 `version` 重建仍有风险。
>
> 对照：已修复的 `SET NX` 在 8 客户端 × 5 次竞态下稳定 only-one-winner ✅（见 `tests/test_cluster_string.py::test_set_nx_concurrent_atomicity`）。

---

## 2. 根因分析

### 2.1 存储布局

复杂类型均为 **metadata + 子键** 布局（见 `AGENTS.md`）：

- **Metadata**：存于用户 key，含 `flags | expires_at | version | ...size/head/tail`
- **子键**：hex 编码的 `key_len | key | version | part`（member / field / seq）

### 2.2 命令执行流程（问题所在）

以 `LPUSH` 为例，`src/protocol/list/lpush.rs`：

```
71:  let mut metadata = match server.get(&args.key).await? { ... }   ← Raft 读
      ...
      let version = metadata.version;
      let head = metadata.head;                                     ← 基于读到的值计算
      let index = head - 1 - i as u64;                              ← 计算子键序号
      ...
121: server.batch_write(entries).await?                            ← 另一次 Raft 提交
```

**第 71 行（读）与第 121 行（写）之间没有任何原子性保证。**

### 2.3 并发时的失效方式

| 命令 | 失效机制 |
|---|---|
| `LPUSH`/`RPUSH` | N 个请求读到同一个 `head` → 算出**同一子键序号** → 互相覆盖，元素真丢 |
| `HINCRBY`/`APPEND` | N 个请求读到同一个旧值 → 都写 `旧值+Δ` → 丢失更新 |
| `HSET`/`SADD`/`ZADD` | 各自写 `size = 读到的size + 1` 回 metadata → 计数远小于实际 |
| `LPOP`/`RPOP` | 读到同一 `head`/`tail` → 弹出同一元素（重复弹出）或漏弹 |

补充一个复合失效：**`version` 由 `generate_version()`（毫秒时间戳）生成**（`encoding/hash.rs:79`、`list.rs:118`、`zset.rs:91`）。若并发首个写入落在**不同毫秒**，各请求会以不同 `version` 写子键，而 metadata 只保留最后写入者的 version —— 先前 version 空间里的子键**整体不可见**（孤儿数据）。这解释了 `ZADD` 丢成员（48/50）与 `SADD` 有时"恰好正确"（同毫秒则同一 version 空间、不同成员不冲突）的差异。

### 2.4 为什么 `batch_write` 不够

`server.batch_write()` 保证**单次提交内**的多个 entry 原子（all-or-nothing），但**跨请求**的读-改-写序列没有任何保证。原子性的边界画错了一层。

### 2.5 与 issue #1 的关系

issue #1 描述的是同一根因在 `SET NX` 上的表现：

> 两个 `SET key value NX` 都读到 key 不存在，于是都执行 insert，两条都成功。

该问题已在 `SET NX` / `SETNX` 上用**条件事务**修复（见第 4 节），并发回归测试见 `tests/test_cluster_string.py::test_set_nx_concurrent_atomicity`。同类的 hash/list/zset/set/bitmap 命令尚未修复。

---

## 3. 受影响命令清单

### 3.1 已确认丢失更新（实测）

| 文件 | 读 | 写 | 类型 | 状态 |
|---|---|---|---|---|
| `src/protocol/hash/hincrby.rs` | `:83`, `:111`, `:139` | `:157` | 数值丢失 | ✅ 已修复（§1.1） |
| `src/protocol/string/append.rs` | `:56` | `:67`, `:82`, `:95` | 数据截断 | ✅ 已修复（§1.2） |
| `src/protocol/list/lpush.rs` | `:71` | `:121` | 元素覆盖 | ✅ 已修复（§1.3） |
| `src/protocol/list/rpush.rs` | 同模式 | 同模式 | 元素覆盖 | ⬜ 未修复 |
| `src/protocol/zset/zadd.rs` | `:146`, `:171` | `:218` | 成员丢失 | ⬜ 未修复 |

### 3.2 同模式，疑似受影响（未逐一实测）

| 文件 | 读 | 写 |
|---|---|---|
| `src/protocol/hash/hset.rs` | `:84`, `:118` | `:135` |
| `src/protocol/hash/hdel.rs` | 同模式 | 同模式 |
| `src/protocol/hash/hsetnx.rs` | 同模式 | 同模式 |
| `src/protocol/set/sadd.rs` | `:49`, `:73` | `:86` |
| `src/protocol/set/srem.rs` | 同模式 | 同模式 |
| `src/protocol/zset/zrem.rs` | 同模式 | 同模式 |
| `src/protocol/list/lpop.rs` | `:66`, `:94` | `:119` |
| `src/protocol/list/rpop.rs` | 同模式 | 同模式 |
| `src/protocol/list/lrem.rs` | 同模式 | 同模式 |
| `src/protocol/list/lset.rs` | 同模式 | 同模式 |
| `src/protocol/bitmap/setbit.rs` | `:117`, `:140` | `:157` |
| `src/protocol/key/expire.rs` / `pexpire.rs` | 读 TTL → 写回 | 同模式 |
| `src/protocol/key/rename.rs` / `renamenx.rs` | 读源 → 批量搬移 | 同模式 |

### 3.3 已确认（写路径原子，但存在独立缺陷）

**`GETSET`**（`src/protocol/string/getset.rs`）：主写入走 `server.getset()`（无条件的 `TxnReq` + `return_previous`），原子性 ✅。但存在一个**独立的竞态**：当旧值不是 `StringValue`（如 hash 元数据）时，命令报 `WRONGTYPE` 并把原始字节写回去"恢复"（`getset.rs` 的 `server.set(params.key, raw)`）。这次恢复是独立的 Raft 写：

1. `GETSET` 已生效（写入了新值）
2. 并发客户端写入新值
3. `WRONGTYPE` 恢复路径用**过期快照**覆盖 → **并发写丢失**

属于 §1 同类问题，修复时应一并纳入。

### 3.4 已修复（作为参考实现）

| 命令 | 修复方式 | 文件 |
|---|---|---|
| `SET NX` / `SET XX` / `SETNX` | `TxnCondition` 条件事务 + CAS 重试 | `src/protocol/string/set.rs`, `setnx.rs` |
| `INCR` / `INCRBY` / `DECR` / `DECRBY` | 应用层 CAS（`TxnCondition::eq` + `if_then`） | `src/protocol/string/atomic_incr.rs` |
| `HINCRBY` | CAS 同时锁定 metadata 与 field 子键 | `src/protocol/hash/hincrby.rs` |
| `APPEND` | CAS 锁定整值序列化字节；过期 key 用 `eq` 锁旧字节（TTL 盲区） | `src/protocol/string/append.rs` |
| `LPUSH` | CAS 锁定 metadata 字节（head 游标派生子键索引，胜者索引唯一）；过期 list 用 `eq` 锁旧字节 | `src/protocol/list/lpush.rs` |

> ⚠️ **参考时须注意**：`atomic_incr` 的 CAS 重试在**热点 key 下会耗尽上限**（实测 100 并发打同一 key，2000 次请求仅约 60 次成功，其余报 `ERR increment retry limit exceeded`）。这是"每个重试都是完整读+事务往返"的固有代价——冲突概率 ≈ 1/N，N 个并发客户端需要 N 次重试。
>
> 含义：方案 B 若简单复制此模式，会把同样的可用性悬崖带进 hash/list 等命令。修复时需同时改进重试策略（加退避/jitter、提高上限，或改用 §5 方案 C 的单次 apply RMW）。

---

## 4. 已确认可用的修复原语

rockraft 0.1.8 的 `TxnReq` + `TxnCondition` 可表达所需条件，且**条件求值与写入在同一次 apply 内完成**：

- `src/raft/store/statemachine.rs:431 apply_txn`：先求值全部条件，再 stage 写入
- `src/raft/store/statemachine.rs:486 get_kv_with_overlay`：条件读先查 `PendingWrites` overlay，再落 RocksDB
  - 保证 leader / follower / 重启恢复三条路径的判定一致（注释明确说明：否则会 diverge）
- `src/raft/store/statemachine.rs:73 evaluate_condition`：支持 `Exists` / `NotExists` / `Equal` / `NotEqual` / `Greater` / `Less` / `GreaterEqual` / `LessEqual`

条件构造器：`TxnCondition::{exists, not_exists, eq, ne, gt, lt, ge, le}`。

### 依赖状态（2026-09-21 更新）

CoreDB 的 `Cargo.toml` 现指向**本地 rockraft**（`path = "../rockraft"`），本地工作区含 6 个文件的未提交性能优化（转发接入连接池、leader 探测 200ms + 5s 总预算 + jitter、池初始化竞态修复）。此前文档所记"git 干净、与注册表版本一致"已**不再成立**；上述 `apply_txn`/`get_kv_with_overlay`/`evaluate_condition` 的行为在两边一致，不受影响。

### 关键提醒

1. **条件读针对的是原始存储字节**（含 `flags | expires_at | ...` 编码），不是解码后的用户数据。
2. **rockraft 不感知 TTL**：物理存在但已过期的 key 仍满足 `Exists`。TTL 判定必须在应用层做，并用 `eq(旧字节)` 做 CAS。
3. **`with_return_previous()` 返回的是原始字节**，需要应用层解码（`SET ... GET` 曾因此返回 14 字节乱码，已修）。

---

## 5. 修复方案

### 方案 A：逐命令加 CAS 条件事务（短期）

参照 `set.rs` 的 `conditional_set` 与 `atomic_incr.rs`：

1. 读 metadata + 目标子键
2. 用 `TxnCondition::eq(key, 读到的metadata字节)` 钉住所观察状态
3. `if_then` 写入新子键 + 新 metadata
4. `branch=false` → 重读重试（有界，如 32 次）

- ✅ 可立即实施，无需改 rockraft
- ❌ 需改 15+ 个命令，重复代码多
- ❌ 热点 key 下有重试开销（`HINCRBY` 已暴露此风险；见 §3.4 的可用性悬崖警告）

### 方案 B：统一 `atomic_mutate` helper（推荐）

在 CoreDB 封装一个通用原语，让所有复杂类型命令复用：

```rust
// 伪代码
server.atomic_mutate(key, |old_meta| -> (Vec<UpsertKV>, T) )  // 条件读 + 条件写 + 重试
```

- ✅ 根治，避免逐命令打补丁
- ✅ 与 `atomic_incr.rs` / `conditional_set` 的既有模式一致
- ❌ 需要仔细设计接口（闭包内不能有副作用）

### 方案 C：rockraft 层提供原子 RMW 原语（长期）

在 rockraft 内实现"读-改-写"在单次 apply 内完成的原语（如 `TxnIncr` 的思路：命令本身进日志，状态机在 apply 时刻求值）。

- ✅ 最彻底，且能顺带解决 TTL 感知
- ✅ **同时消除 CAS 的两次往返与热点重试风暴**（无竞争时与无条件写同价）
- ⚠️ 曾经试过并回退：早期实现把裸整数写入存储（破坏 CoreDB 编码契约）且用 `io::Error` 表达业务错误导致 openraft RaftCore 退出。**方向正确、层次做错**——若重试，需把编码语义随命令下传（状态机只见应用层编码的字节），并确保业务错误不触发共识核心退出
- ❌ 需要改 rockraft 并维护 fork / 上游（CoreDB 当前已依赖本地 path，落地门槛比之前低）

**建议**：先做方案 B（CoreDB 内统一 helper），验证后再评估是否下沉到 rockraft（方案 C）。若走方案 B，**必须同步改进重试策略**（退避 + jitter + 更高上限），避免复制 `atomic_incr` 的可用性悬崖。

---

## 6. 复现脚本

```bash
# 构建
cargo build --release

# 启动单节点
mkdir -p /tmp/atomic
cat > /tmp/atomic/n.toml <<'EOF'
node_id = 1
server_addr = "0.0.0.0:16679"
[raft]
address = "127.0.0.1:17671"
advertise_host = "127.0.0.1"
join = []
[rocksdb]
data_path = "/tmp/atomic/data"
max_open_files = 10000
[log]
level = "warn"
EOF
RUST_LOG=warn ./target/release/coredb --conf /tmp/atomic/n.toml &

# HINCRBY 丢失更新（期望 100）
redis-cli -p 16679 DEL hc
for i in $(seq 1 100); do (redis-cli -p 16679 HINCRBY hc f 1 >/dev/null) & done; wait
redis-cli -p 16679 HGET hc f      # 实测 ~64

# LPUSH 元素覆盖（期望 50）
redis-cli -p 16679 DEL lp2
for i in $(seq 1 50); do (redis-cli -p 16679 LPUSH lp2 v$i >/dev/null) & done; wait
redis-cli -p 16679 LLEN lp2       # 实测 ~22
```

> 说明：并发由 shell `&` 提供，客户端无重试；单节点即可复现，不依赖集群。
> 集群环境另有独立的性能/稳定性问题（follower 转发、连接失败），记录见 `docs/bench.md`；
> 其中转发路径每请求新建 gRPC channel 的问题已由本地 rockraft 优化修复（follower 写 ~600 → ~10,900 rps）。

---

## 7. 待办

- [ ] 实测 `LPOP`/`RPOP`/`LREM`/`LSET`/`HDEL`/`SREM`/`ZREM` 的具体丢失率
- [ ] 实测 `GETSET` 对非 string 旧值的并发覆盖（§3.3 的恢复竞态）
- [ ] 设计并实现统一 `atomic_mutate` helper（方案 B），**同步改进重试策略（退避 + jitter）**
- [ ] 逐个迁移受影响命令
- [ ] 为每类命令补充并发集成测试（参照 `test_set_nx_concurrent_atomicity`）
- [ ] 评估是否将原子 RMW 下沉到 rockraft（方案 C），规避此前 TxnIncr 的两个实现错误
- [ ] 修复 `GETSET` 的 WRONGTYPE 恢复竞态（可并入方案 B）
- [ ] 评估 rockraft 条件事务的 TTL 感知：`TxnCondition::not_exists` 对「过期视为不存在」
      的 CAS 不成立（APPEND 修复时踩坑），后续 HSET/LPUSH 等修复会重复遇到——
      可在 `TxnOp` 增加 `NotExistsOrExpired`，或让条件判定感知 `expires_at`
- [ ] 评估 CAS 重试上限与热点 key 的可用性悬崖（§3.4 警告：100 并发仅约 60 次成功），
      APPEND/HINCRBY 的退避策略（`util/cas.rs`）应随方案 B 统一回迁
