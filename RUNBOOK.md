# kafka-solace-bridge Runbook — Kafka → 5 × Solace bridge

## 1. 架构一页

```
Kafka topic (28 partitions, 1 consumer group)
   │  spring-kafka, AckMode.MANUAL, 2 pods × 14 partitions
   ▼
PartitionWindow (每 partition 一个，内存，有界)
   │  同一个 Pending 对象进入 5 个 per-destination 队列
   ├──▶ DestinationSender p{n}-dest/1 ─ transacted JMS Session ─▶ Solace topic dest/1
   ├──▶ DestinationSender p{n}-dest/2 ─ transacted JMS Session ─▶ Solace topic dest/2
   ├──▶ ...                                                        (每 partition 1 个 JMS Connection)
   └──▶ DestinationSender p{n}-dest/5 ─ transacted JMS Session ─▶ Solace topic dest/5
```

| 机制 | 实现 |
|---|---|
| 吞吐 | 每 (partition, destination) 一个 transacted session，每 `app.batch.size` 条或 `app.batch.linger` 做一次 `commit()`。事务内 PERSISTENT 发送走 255 的滑动窗口，只有 commit 是一次 broker round trip。非事务 PERSISTENT 发送的窗口固定为 1，100 ms RTT 下全局上限 280 msg/s，所以事务不是可选项。 |
| at-least-once | Kafka offset 只提交到**5 个 destination 全部 commit 成功（或熔断丢弃）**的最高连续 offset（水位）。commit 失败 → broker 回滚整批 → 整批按原顺序重发。 |
| partition 内有序 | 每个 (partition, destination) 单线程串行；重试不跳过。 |
| 隔离 | 每个 destination 一个熔断器（跨 partition 共享）。窗口填满（`app.window.high`）时只暂停该 partition 的消费，水位回落到 `app.window.low` 时恢复。 |
| 去重键 | JMS property `kafka_partition`、`kafka_offset`、`kafka_timestamp`、`kafka_key`。下游按 (partition, offset) 去重。 |

熔断状态机（`CircuitBreaker`）：

```
CLOSED ─commit 失败─▶ RETRYING：停 retry-wait(1 min) → 重试同一批，最多 max-retries(3) 次
                         ├─ 任一次成功 ─▶ CLOSED
                         └─ 全部失败 ─▶ OPEN
OPEN：open-duration(5 min) 内该 destination 的消息（含窗口里积压的）直接标记已确认并丢弃，水位照常前进
OPEN ─到期─▶ HALF_OPEN：下一批实时消息试发 ─成功─▶ CLOSED ／ ─失败─▶ OPEN（再 5 min）
```

线程：每 pod 2 个 Kafka consumer 线程（只做 transform + 入队）+ 14 × 5 = 70 个 sender 线程（全部 Solace IO）。

## 2. 上线前置条件 — 逐项确认

### 2.1 Solace broker（请 Solace 管理员确认）

| 项 | 要求 | 不满足的后果 |
|---|---|---|
| client-profile `allow-transacted-sessions` | **enabled** | `createSession(true, …)` 失败，app 无法启动 worker，partition 一直 seek 重试。软件 broker 默认 profile 是 enabled；**appliance 和 Solace Cloud 新建 profile 默认 disabled**。 |
| client-profile `max-transacted-sessions` | ≥ 5（默认 10） | 每个 JMS Connection 挂 5 个 transacted session；超限抛 `EC_MAX_TRANSACTED_SESSIONS_EXCEEDED_ERROR`。 |
| client-profile `max-messages-per-transaction` | ≥ `app.batch.size`（默认 256，app 用 200） | commit 失败 `TRANSACTION_FAILURE`。改此值是 Controlled Availability 功能，Solace Cloud 不可改 → **不要把 `app.batch.size` 调到 256 以上**（`AppProperties` 会拒绝启动）。 |
| 目标 queue `reject-msg-to-sender-on-discard` | **enabled**（默认） | 若为 disabled，queue 满时 broker **静默丢弃并返回 commit 成功**，app 看不到任何错误，at-least-once 失效。 |
| client-username `max-connections` | ≥ pods × 14 + 余量（2 pod = 28） | 第 15 个 partition 的 Connection 创建失败。 |
| 出向带宽 app → Solace | ≥ 400 Mbps（10k msg/s × 1 KB × 5） | 需求 1 物理上不成立。 |

### 2.2 Kafka

- consumer group = `KAFKA_GROUP_ID`（默认 `kafka-solace-bridge`），必须是这个 app 独占的。
- `auto.offset.reset=latest`：**首次启动**或**已提交 offset 过期**（pod 宕机超过 retention）时从最新开始，中间的数据不会补发。首次上线前若需要从某个位置开始，先用 `kafka-consumer-groups --reset-offsets` 预置 offset。
- 查看 topic retention：

```bash
kafka-configs.sh --bootstrap-server <host:9092> --describe --all \
  --entity-type topics --entity-name <topic> \
  | grep -E 'retention\.(ms|bytes)|cleanup\.policy'
# 需要认证时加 --command-config client.properties
# 默认值 retention.ms=604800000 (7 天), retention.bytes=-1
```

## 3. 配置项与容量算术

| 配置 | 默认 | 作用 |
|---|---|---|
| `app.kafka.concurrency` | 2 | consumer 线程数。只做入队，2 足够 5k msg/s。 |
| `app.batch.size` | 200 | 一个 JMS 事务的消息数。≤ 256。 |
| `app.batch.linger` | 100ms | 批次最长等待时间；决定低流量时的延迟。 |
| `app.window.high` / `low` | 33000 / 16000 | 每 partition 未确认 record 数的暂停 / 恢复阈值。 |
| `app.breaker.retry-wait` | 1m | 熔断重试间隔。 |
| `app.breaker.max-retries` | 3 | 进 OPEN 前的重试次数。 |
| `app.breaker.open-duration` | 5m | OPEN（丢弃）持续时间。 |
| `app.shutdown-drain` | 2s | SIGTERM 后等在途 commit 的时间。 |

**每个 (partition, destination) 流的吞吐上限** ≈ `batch.size / (RTT + linger)` = 200 / 0.2 s ≈ 1000 msg/s。需求：10k / 28 = 357 msg/s。余量 2.8 倍。RTT 变大时先看这一项。

**窗口内存**（每 pod）= `window.high` × ~1.3 KB（1 KB payload + 对象开销）× 14 partition = 33000 × 1.3 KB × 14 ≈ **600 MB**，占 1.2 GB heap 的一半。改 `window.high` 或 pod 内存时同步改另一个。

**隔离时长** = `window.high` / 单 partition msg/s = 33000 / 357 ≈ **92 s**。一个 destination 进入 RETRYING 后，该 partition 上其他 4 个 destination 还能继续跑 92 s，然后该 partition 暂停消费，直到 RETRYING 结束（成功恢复或 3 min 后进 OPEN 丢弃）。要让其他 destination 完全不停：pod 内存提到 4 GB，`window.high` 提到 70000。

## 4. 指标与告警

Prometheus 抓 `/actuator/prometheus`。

| 指标 | 含义 | 建议告警 |
|---|---|---|
| `bridge_breaker_state{destination}` | 0 CLOSED / 1 RETRYING / 2 OPEN / 3 HALF_OPEN | `== 1` 持续 1 min → warning（下游在拒绝）；`== 2` → **critical：正在丢消息** |
| `bridge_discarded_total{destination}` | OPEN 期间丢弃的消息数 | `rate(...[5m]) > 0` → critical |
| `bridge_rolledback_total{destination}` | commit 失败次数 | `rate(...[5m]) > 0` → warning |
| `bridge_sent_total{destination}` | 成功 commit 的消息数 | 与 Kafka 入量比对；某个 destination 明显低于其他 → 排查 |
| `bridge_window_size{partition}` | 未确认 record 数 | `>= app.window.high` 持续 30 s → partition 已暂停 |
| `bridge_transform_failed_total` | transform 抛异常、被跳过的记录数 | `increase(...[5m]) > 0` → warning，看日志里的 partition/offset |
| `kafka_consumer_fetch_manager_records_lag_max` | consumer lag | 持续增长且 breaker 全为 0 → 吞吐不够，见 §3 |

## 5. 故障场景与处置

### 5.1 某个 Solace destination 拒绝（spool over quota）

时间线（`retry-wait=1m, max-retries=3, open-duration=5m`）：

| 时刻 | 状态 | app 行为 | 你看到 |
|---|---|---|---|
| t0 | commit 失败 | 该 destination 全部 sender 停止发送，持有各自的批次 | `bridge_breaker_state=1`，WARN 日志 `commit failed` |
| t0+1m / 2m / 3m | 重试 | 同一批重发；成功则回 CLOSED | `bridge_rolledback_total` 增长 |
| ~t0+92s | 窗口满 | 该 partition 暂停消费，其他 4 个 destination 在该 partition 上也停 | `bridge_window_size` 到顶，consumer lag 上升 |
| t0+3m | OPEN | 该 destination 的消息**开始丢弃**（含窗口里积压的），水位前进，partition 恢复消费 | `bridge_breaker_state=2`，`bridge_discarded_total` 增长 |
| t0+8m | HALF_OPEN | 用实时消息试发 | 成功 → 0；失败 → 2，再 5 min |

处置：修下游消费者 / 扩 queue quota。**OPEN 期间丢弃的消息不会自动补发。** 需要补发时用 Kafka 重置 offset 重放（§6），但重放会同时打到 5 个 destination，其他 4 个会收到重复 — 下游必须按 `kafka_partition + kafka_offset` 去重。

### 5.2 Solace 整体不可达

Solace JMS 无限重连（`SolaceConfig`：`reconnectRetries=-1`）。重连期间所有 destination 的 commit 都失败 → 5 个熔断器同时走 §5.1 的时间线，3 min 后全部 OPEN → **全部丢弃**。这是 Q9 决策（可容忍丢消息）的直接后果；如果 Solace 整体故障超过 3 min 且不可接受丢弃，`kubectl scale --replicas=0` 停掉 app，Kafka 会保留数据，恢复后从水位继续。

### 5.3 Pod 重启 / 滚动发布 / rebalance

partition 被收回 → 提交当前水位，丢弃窗口，关闭该 partition 的 JMS Connection（在独立线程上关闭，rebalance 不等 Solace）。新 owner 从水位重放，已经发出但未过水位的消息会**重复**到达（最多 `window.high` 条 per destination）。熔断器状态不持久化，重启后从 CLOSED 开始。

### 5.4 transform 抛异常

记录被跳过（不发任何 destination），`bridge_transform_failed_total` +1，ERROR 日志带 partition/offset。水位照常前进。

### 5.5 Kafka lag 增长、breaker 全 0

吞吐不够。依次看：RTT 是否变大（§3 公式）；`bridge_window_size` 是否常在高位（sender 跟不上）；pod CPU。可调：`app.batch.size` 提到 256（上限）；增加 pod（必须是 28 的因数）。

### 5.6 启动后 partition 一直不消费

日志 `cannot start worker for …` → Solace 连不上或 session 打不开。对照 §2.1 逐项检查 client-profile 和网络。建连在 Kafka consumer 线程上进行，所以故意设成**不重试、10 s 超时**（`SolaceConfig`），失败后由 `DefaultErrorHandler` 每 5 s 重试；配置修好后自动恢复。已建立的连接断开时由 Solace API 自己的线程无限重连，不占用 consumer 线程。

## 6. 运维操作

```bash
# 本地运行
KAFKA_BOOTSTRAP_SERVERS=... SOLACE_HOST=... SOLACE_USERNAME=... SOLACE_PASSWORD=... mvn spring-boot:run

# 构建镜像（不需要 Dockerfile）
mvn -DskipTests spring-boot:build-image -Dspring-boot.build-image.imageName=REGISTRY/kafka-solace-bridge:VERSION

# 测试（本机 JDK 25 时需要 pom 里已配置的 -Dnet.bytebuddy.experimental=true；JDK 21 不需要）
mvn test

# 查看 consumer group 进度
kafka-consumer-groups.sh --bootstrap-server <host:9092> --describe --group kafka-solace-bridge

# 重放（先 scale 到 0，否则 reset 会被拒绝）
kubectl scale deploy/kafka-solace-bridge --replicas=0
kafka-consumer-groups.sh --bootstrap-server <host:9092> --group kafka-solace-bridge --topic <topic> \
  --reset-offsets --to-datetime 2026-09-03T10:00:00.000 --execute
kubectl scale deploy/kafka-solace-bridge --replicas=2
```

## 7. 已知取舍（设计阶段明确接受）

- 单 consumer group + 内存窗口：一个 destination 降级时，其他 destination 在同一 partition 上最多停 ~92 s（§3）。真正无时限的隔离需要 per-destination 进度持久化，被「无外部存储 + 1x Kafka 带宽」排除。
- OPEN 期间丢消息，不写 DLT。
- at-least-once 带来重复，下游去重。
- 熔断器把「Solace 整体不可达」和「单个 destination 拒绝」同等处理（§5.2）。
- 熔断状态、窗口内容都在内存里，重启即失。
- `Transformer.apply` 目前是恒等函数，待替换。
