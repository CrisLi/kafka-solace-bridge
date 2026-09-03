# kafka-solace-bridge — Kafka → 5 × Solace 桥接：设计文档

运维手册见 [RUNBOOK.md](RUNBOOK.md)（上线前置条件、指标、故障处置）。本文档只讲**为什么这样设计**，以及**实现在哪些地方偏离了设计、为什么**。

## 1. 目标与约束

从一个 Kafka topic（28 partition）消费，做一次纯函数转换，把同一份结果发到 5 个 Solace topic。

| # | 需求 | 量化 |
|---|---|---|
| 1 | 够快 | 峰值 10k msg/s，平均 1 KB，端到端 P99 ≤ 2 s |
| 2 | Solace at-least-once | 允许重复，下游按去重键去重 |
| 3 | Kafka partition 内有序 | 同一 partition 的消息按相同顺序**到达每个** Solace topic；重试不跳过 |
| 4 | 单 destination 隔离 | 一个 Solace topic 消费能力下降时，尽量不影响其他 4 个 |
| 5 | 无持久化中间件 | 不用 DB / Redis；纯 Java，跑在 k8s |

环境事实：

- Spring Boot 3.4.13、Java 21、spring-kafka 3.3.11、`solace-jms-spring-boot-starter` 5.4.2（`sol-jms-jakarta` 10.27.3）
- app → Solace RTT **100 ms**（跨 region），出向带宽 ≥ 400 Mbps
- 2 个 pod，每 pod 2 GB 内存；不开 HPA
- Kafka 侧**不能**接受 5 倍读放大（带宽限制）
- Solace destination 降级的历史经验：停 1 min 后重试，仍失败再停 1 min
- 允许在 destination 长期拒绝时丢弃该 destination 的消息，不需要 DLT

## 2. 调研得到的关键事实

这些事实直接决定了下面的设计，不是可选项。

| 事实 | 来源 | 影响 |
|---|---|---|
| Solace JMS **不实现 JMS 2.0**：没有 `send(Message, CompletionListener)` | docs.solace.com JMS supported environments | 没有异步 publish ack，只能同步 `send()` 或事务 |
| 非事务 PERSISTENT 发送的 publish window **固定为 1** | docs.solace.com Message Delivery Properties | 每条一次 broker round trip：100 ms RTT → 每个串行流 10 msg/s |
| 事务 session 内的 PERSISTENT 发送走 `sendADWindowSize`（默认 255）的滑动窗口，只有 `commit()` 是一次 round trip | `SolConnectionFactory` javadoc；Solace community | 事务是达到吞吐的唯一途径 |
| 本地事务默认最多 **256** 条消息；改上限是 Controlled Availability 功能，Solace Cloud 不可改 | Using Local Transactions | `app.batch.size ≤ 256`，取 200 |
| client-profile `max-transacted-sessions` 默认 **10，按 connection 计** | CLI reference / Cloud REST doc | 决定连接布局：每 connection 最多 10 个 transacted session |
| 事务内 over quota 在 **`commit()`** 抛 `TransactionRolledBackException`，broker 回滚整笔事务，一条都不入 queue；queue 的 `reject-msg-to-sender-on-discard`=disabled 时改为**静默丢弃、commit 成功** | Publishing Messages in Transacted Sessions | 失败检测点是 commit；broker 配置必须 enabled |
| `allow-transacted-sessions` 在 appliance / Solace Cloud 新建 profile 上默认 **disabled** | Configuring Client Profiles | 上线前置条件（RUNBOOK §2.1） |
| spring-kafka `AckMode.MANUAL` 下 `Acknowledgment.acknowledge()` 可从**任意线程**调用，容器在下一次 poll 前提交 | `Acknowledgment` javadoc、`ContainerProperties.AckMode` | sender 线程可以直接 ack 水位 |
| `pausePartition()` / `resumePartition()` 线程安全；暂停期间容器继续 poll，排队的 ack 照常提交 | `MessageListenerContainer` javadoc | 有界窗口可以用 pause 实现反压 |
| 默认模式下对**已收回** partition 的 ack 不会被过滤，会照常提交并可能覆盖新 owner 的 offset | spring-kafka 3.3.x 源码 `ListenerConsumer.addOffset` | revoke 后必须自行屏蔽 ack |
| 5.4.2 starter 的 `SupportedProperty` 里没有非 JNDI 的 `Solace_JMS_ReconnectRetries` 等 key；重连参数是 `SolConnectionFactory` 的 setter | `javap` 反编 `sol-jms-jakarta-10.27.3.jar` | 在 `SolaceConfig` 用 setter 配置，不在 yml |

## 3. 设计决策

### 3.1 扇出拓扑：单 consumer group + 内存窗口

单 consumer group 下，一个 partition 只有**一个** offset 数字。要满足需求 2，offset 只能提交到「5 个 destination 全部确认」的位置：

```
commit offset = min(confirmed[P][D1..D5])
```

所以每个 partition 需要一个内存里的窗口，保存已消费但未被全部 destination 确认的 record。窗口必须有界（否则 OOM），满了就 `pausePartition`。

**否决的备选方案**

| 方案 | 否决原因 |
|---|---|
| 5 个 consumer group，每 destination 一个（Kafka 自己维护 5 份进度，隔离无时限） | Kafka 读放大 5 倍，带宽约束不允许 |
| 单 group 读 + 每 destination 进度用 `AdminClient.alterConsumerGroupOffsets` 写到独立 group id + 降级时 recovery consumer 重读追赶（稳态 1x 带宽，隔离无时限） | recovery consumer 与实时流的交接点要同时保证不重复、不乱序，每 destination 一套状态机。降级处置粒度是「1 min 重试、3 次后丢弃」，用不到无时限隔离 |

**接受的后果**：隔离是有时限的 —— `隔离时长 = 窗口深度 / 单 partition msg/s`，之后该 partition 对全部 5 个 destination 暂停，直到慢的 destination 恢复或进入 OPEN 丢弃。

### 3.2 发送模式：每 (partition, destination) 一个 transacted session

算术：非事务 PERSISTENT 每条一次 round trip，100 ms RTT → 10 msg/s per 串行流；28 个 partition 严格有序 → 全局上限 **280 msg/s**，峰值 10k，差 36 倍。事务 session：`batch.size / (RTT + linger)` = 200 / 0.2 s ≈ **1000 msg/s** per 流，需求 357，余量 2.8 倍。

事务边界与 Kafka 提交天然对齐：**send × N → JMS commit → 标记该 destination 已确认 → 水位前进 → Kafka ack**。JMS commit 成功而进程在 Kafka commit 前死掉 → 重放 → 重复（at-least-once）。

为什么不是「一个 partition 一个 session、5 个 producer 共一笔事务」：D5 的 commit 失败会把 D1–D4 的发送一起回滚，隔离归零。

### 3.3 有序性：单线程串行 + 整批原序重发

每个 (partition, destination) 恰好一个线程、一个 session。commit 失败时 broker 已回滚整批，线程按原顺序重发同一批。不跳过、不并行，所以每 partition 的并行度就是 destination 数（5），吞吐靠 28 个 partition 横向铺开。

### 3.4 隔离：每 destination 一个熔断器，跨 partition 共享

Solace over quota 是对整个 topic 的，不是对某个 Kafka partition 的，所以熔断器按 destination 共享。

```
CLOSED ─commit 失败─▶ RETRYING：停 1 min → 重试同一批，最多 3 次（同一轮内多个 partition 的失败只计一次）
                         ├─ 任一次成功 ─▶ CLOSED
                         └─ 全部失败 ─▶ OPEN
OPEN：5 min 内该 destination 的消息（含窗口里积压的）直接标记已确认并丢弃，水位照常前进
OPEN ─到期─▶ HALF_OPEN：实时批次试发 ─成功─▶ CLOSED ／ ─失败─▶ OPEN（再 5 min）
```

「1 min × 3」来自用户既有系统的运行经验；「OPEN 丢弃、不写 DLT、恢复后从当前消息继续」是用户的明确选择（可容忍这种情况的丢失，优于让 28 个 partition 全部停住）。

RETRYING 期间被拒的 destination 的消息留在窗口里等重试，这就是窗口填满的来源；进入 OPEN 时立刻丢弃积压，水位马上前进，暂停立刻解除 —— 这是 OPEN 状态能解除反压的原因。

### 3.5 窗口深度与内存

RETRYING 最长 3 min，完整覆盖需要 357 msg/s × 180 s × 14 partition × 1.3 KB ≈ **1.2 GB**，2 GB pod 装不下。三个选项里用户选了 **(c)**：保持 2 GB、保持 1 min 间隔，窗口按内存能给的设（`high=33000` ≈ 600 MB / pod），填满后该 partition 暂停消费，D1–D4 在该 partition 上最多停 ~90 s。需求 4 写的是「尽量」；要完全不停，pod 内存 4 GB + `high=70000`。

### 3.6 连接布局：每 partition 一个 JMS Connection × 5 个 transacted session

最初设计是「每 destination 一个 Connection × 14 个 session」，理由是 TCP 窗口和 socket 隔离。调研发现 `max-transacted-sessions` 默认 10 / connection，14 会撞墙。改为每 partition 一个 Connection：每 connection 5 个 session，无需改 broker；每 socket 只承担 50 MB/s ÷ 14 ≈ 3.6 MB/s，100 ms RTT 下在途 360 KB；partition 被收回时直接关掉对应 Connection，生命周期对齐。「socket 隔离」的担心本来就是多余的：over quota 时 broker 回 NACK 而不是停止读 socket。

### 3.7 offset 提交与 rebalance

- `AckMode.MANUAL`。水位前进时对水位 record 调 `acknowledge()`（Kafka 提交是累积的，只 ack 最高的一条）。
- partition 被收回：`onPartitionsRevokedBeforeCommit` 里标记窗口 revoked、清空、关闭该 partition 的 Connection。之后不再对该 partition ack —— 否则会覆盖新 owner 的 offset（事实表第 10 行）。新 owner 从水位重放，D1–D4 收到最多 `high` 条重复。不等窗口排空，因为最坏要等 3 min。
- 优雅停机：`BridgeListener` 的 lifecycle phase 比 Kafka 容器高 1，先执行：`container.pause()` → 等 ≤ `shutdown-drain` 让在途 commit 完成并 ack → 容器停止、提交排队的 ack → unsubscribe 触发 revoke → worker 关闭。
- `auto.offset.reset=latest`：已提交 offset 过期时从最新开始，不重放 7 天。

### 3.8 去重键

JMS property `kafka_partition`、`kafka_offset`、`kafka_timestamp`、`kafka_key`。下游按 (partition, offset) 去重，用不用是下游的事。

### 3.9 部署

2 pod × 14 partition，不开 HPA（每次副本数变化都是一次 rebalance，所有 partition 短暂停止）。每 pod 2 个 consumer 线程（只做 transform + 入队）+ 70 个 sender 线程（全部 Solace IO）。

## 4. 架构与数据流

```
Kafka consumer 线程 (×2)                          sender 线程 (×70 = 14 partition × 5 destination)
─────────────────────                              ──────────────────────────────────────────────
poll → ConsumerRecord
  → Transformer.apply()          PartitionWindow[P]
  → new Pending(payload, ack) ──▶ inOrder deque ──────────────── 水位：队首连续「5 位全 1」的 record → ack()
                                 queue[D1] ──▶ DestinationSender p{P}-D1 ─┐
                                 queue[D2] ──▶ DestinationSender p{P}-D2 ─┤ 每个：取批 → breaker.awaitDecision()
                                 queue[D3] ──▶ ...                        ├──▶ SEND：send×N → commit → confirm(D)
                                 queue[D4] ──▶ ...                        │    失败：rollback → breaker.onFailure() → 重发同批
                                 queue[D5] ──▶ DestinationSender p{P}-D5 ─┘    DISCARD：confirm(D) 不发送
  size ≥ high → pausePartition(P)                  size ≤ low → resumePartition(P)
```

| 类 | 职责 |
|---|---|
| `BridgeListener` | `@KafkaListener` 入口；`ConsumerAwareRebalanceListener` 驱动 worker 创建/关闭；`SmartLifecycle` 优雅停机 |
| `PartitionWindow` | 一个 partition 的窗口：commit 顺序 deque + 每 destination 一个队列；水位 ack；pause/resume；revoke |
| `Pending` | 一条 record + 5 个确认位 |
| `PartitionWorker` | 一个 partition 拥有的一切：Connection、5 个 Session/Producer、5 个线程、窗口 |
| `DestinationSender` | 一个 (partition, destination) 流的循环 |
| `CircuitBreaker` | 每 destination 的状态机 |
| `Transformer` | 纯函数，待替换 |
| `KafkaConfig` / `SolaceConfig` / `AppProperties` | Kafka 错误处理器 bean、Solace 工厂调参、配置校验。listener 容器工厂用 Boot 自动配置的：`ack-mode` 来自 `application.yaml`，rebalance listener 是容器里唯一的 `ConsumerAwareRebalanceListener` bean（`BridgeListener`） |
| `BridgeMetrics` | Micrometer 指标 |

## 5. 容量算术（配置改动时重新算）

| 量 | 公式 | 当前值 |
|---|---|---|
| 每流吞吐上限 | `batch.size / (RTT + linger)` | 200 / 0.2 s ≈ 1000 msg/s（需求 357） |
| 每 pod 窗口内存 | `window.high × 1.3 KB × partitions/pod` | 33000 × 1.3 KB × 14 ≈ 600 MB（heap 1.2 GB） |
| 隔离时长 | `window.high / (峰值 / 28)` | 33000 / 357 ≈ 92 s |
| 出向带宽 | `峰值 × 消息大小 × destination 数` | 10k × 1 KB × 5 = 50 MB/s ≈ 400 Mbps |
| 每 pod JMS 资源 | `partitions/pod` 个 Connection，`× destination 数` 个 transacted session | 14 Connection、70 session |

## 6. 实现对设计的偏离

设计阶段逐条确认过的结论，实现时改动或补充的地方。

| # | 设计阶段的结论 | 实现 | 原因 |
|---|---|---|---|
| 1 | 实现点 8：「重连交给 Solace JMS 的 `reconnectRetries`」；初稿把 `connectRetries` 也设为 -1（无限） | `connectRetries=0`、`connectTimeoutInMillis=10000`；`reconnectRetries=-1` 保留 | `createConnection()` 在 **Kafka consumer 线程**上执行（`onPartitionsAssigned` / `onRecord`）。Solace 不可达时无限重试会阻塞该线程超过 `max.poll.interval.ms`（5 min），consumer 被踢出 group，引发持续 rebalance。改为 10 s 内失败，由 `DefaultErrorHandler` 每 5 s 重新 seek 到同一条记录重试。已建立连接的重连在 Solace API 自己的线程上，不受影响。code review 发现。 |
| 2 | `onPartitionsAssigned` 为每个分配到的 partition 创建 worker | 第一个失败后 `return`，其余留给 `onRecord` 按需创建 | 同一个 broker，其余必然同样失败；每次失败消耗一个 10 s 连接超时，14 个就是 140 s，全在 consumer 线程上。 |
| 3 | Q15：HALF_OPEN「用**下一批**实时消息试发」 | OPEN 到期后，各 partition 当时就绪的批次都会发出（未串行化为单条 probe）；任一成功 → CLOSED，任一失败 → OPEN | 串行化需要额外的 probing 状态和其他 sender 的等待逻辑。下游未恢复时代价只是恢复瞬间多几次 rollback（每 partition 最多一次）；下游已恢复时反而更快。 |
| 4 | 实现点 2：revoke 时「提交当前水位」 | 不显式 `commitSync`；已入队的 ack 由容器在 `onPartitionsRevokedBeforeCommit` **之后**自动提交（spring-kafka 的 `commitPendingAcks`） | 效果相同，少一次手工 commit 及其错误处理；调研已确认容器的调用顺序。 |
| 5 | 设计未涉及 `Transformer.apply` 抛异常 | 记录跳过：以「5 位全 1」状态入窗口（`addSkipped`），水位按顺序越过它；`bridge.transform.failed` +1、ERROR 日志带 partition/offset | 直接 `acknowledge()` 会越过前面未确认的 record，违反 at-least-once；不处理则 `DefaultErrorHandler` 无限重试，一条坏记录停住整个 partition。 |
| 6 | 设计未涉及 Kafka tombstone（`value == null`） | 发送空 body 的 `BytesMessage`，properties 照常携带 partition/offset/key | `writeBytes(null)` 抛 NPE，会被 sender 当成 Solace 故障计入熔断器，3 次后 OPEN，5 min 内丢弃该 destination 全部消息。 |
| 7 | Q9 早期推荐：按 error code 分流 —— `spool.overquota` 无限重试，其他 `JMSException` N 次后丢弃 | Q15 确认的统一状态机：所有 commit 失败同等处理 | 用户在 Q15 确认了统一状态机。分类要依赖 Solace error code 字符串；且事务 commit 里的「其他异常」（重连中、broker 切换）同样可能是瞬时的，分类收益不明确。 |
| 8 | 设计未涉及 consumer group 名的来源 | `@KafkaListener(idIsGroup = false)`，group 来自 `spring.kafka.consumer.group-id` | 集成测试暴露 spring-kafka 默认用 listener `id` 做 group id。不改则 `KAFKA_GROUP_ID` 环境变量无效，线上 group 会叫 `bridge`。 |
| 9 | 实现点 3：`shutdown-drain` 2 s、grace 30 s | 加启动校验 `shutdown-drain ≤ 25 s` | `BridgeListener.stop()` 的等待发生在一个 Spring lifecycle phase 内，`spring.lifecycle.timeout-per-shutdown-phase` 默认 30 s；超过它 Spring 会在 drain 中途并行停止 Kafka 容器，phase 顺序失效。code review 发现。 |
| 10 | 设计未涉及 worker 创建中途失败 | 线程启动也放进 try；失败时 stop 已启动线程、interrupt、关闭 Connection 后再抛 | `computeIfAbsent` 丢弃抛异常的映射结果，没人能再调 `close()`，Connection 和线程会泄漏。code review 发现。 |
| 11 | yml 里用 `solace.jms.apiProperties.Solace_JMS_ReconnectRetries` 等 key 配重连 | 改为 `SolaceConfig` 里对 `SolConnectionFactory` 调 setter | `javap` 确认 `SupportedProperty` 只有 `Solace_JMS_JNDI_*` 前缀的重连 key，非 JNDI 场景下那些 key 无效。 |
| 12 | 实现点 2：revoke 时关闭该 partition 的 Connection（初稿在 consumer 线程上同步 `close()` 并逐个 `join` sender 线程） | `connection.close()` 在独立 daemon 线程上执行；不再 `join` sender 线程 | rebalance 回调在 **Kafka consumer 线程**上运行。sender 卡在 `commit()` 时要靠 `close()` 解锁，而 `close()` 自己可能等待失联的 socket；顺序 `join(1000)` × 5 线程 × 7 partition 最坏 35 s，超过 lifecycle phase 超时和 `terminationGracePeriodSeconds`，consumer 会被踢出 group 或 pod 被 SIGKILL。第二轮 code review 发现。 |
| 13 | 第 9 条的校验把 Spring 默认值 30 s 写死为 `≤ 25 s` | `BridgeListener` 构造时读取 `spring.lifecycle.timeout-per-shutdown-phase` 的实际值，要求 `shutdown-drain + 5 s ≤ 该值` | 写死只在默认值未被覆盖时成立；有人为加快发布把 phase 超时改小，校验就失效。第二轮 code review 发现。 |
| 14 | Q15 状态机「同一轮内多个 partition 的失败只计一次」，初稿用时间比较去重（失败时刻早于 `notBefore` 即视为同一轮） | `awaitDecision()` 返回带轮次号的 `Permit`，`onFailure(round)` 只在轮次仍是当前轮时计数 | 时间比较假定失败的 `commit()` 在 `retry-wait` 内返回；socket 挂死时 commit 可能几分钟后才抛异常，会被算作新一轮，提前进入 OPEN。轮次号与时间无关。第二轮 code review 发现。 |
| 15 | 初稿自定义 `ConcurrentKafkaListenerContainerFactory` bean，在代码里固定 `AckMode.MANUAL` 和 rebalance listener，使其不能被配置覆盖 | 改用 Boot 自动配置的容器工厂：`spring.kafka.listener.ack-mode: manual` 写在 `application.yaml`，rebalance listener 由 Boot 注入唯一的 `ConsumerAwareRebalanceListener` bean，`DefaultErrorHandler` 作为 `CommonErrorHandler` bean | 用户选择 Boot 原生接法（少一个类，团队更熟悉）。放弃的保护：环境变量 `SPRING_KAFKA_LISTENER_ACK_MODE` 可以改掉 ack 模式而无报错；再出现第二个 `ConsumerAwareRebalanceListener` bean 时 Boot 会静默地两个都不注入。两者都在 yaml 注释和本表标明。 |

设计阶段内部就修订过的点（不算偏离，列出便于追溯）：连接布局 5×14 → 14×5（§3.6）；Q9 推荐从「无限重试」改为「N 次后丢弃」，原因是单 group 结构下无限重试会让 28 个 partition 全停。

## 7. 明确接受的取舍

- 一个 destination 降级时，同一 partition 上其他 destination 最多停 ~92 s（§3.5）。无时限隔离需要 per-destination 进度持久化，被「无外部存储 + 1x Kafka 带宽」共同排除。
- OPEN 期间该 destination 丢消息，不写 DLT，不自动补发。
- at-least-once 带来重复，最多 `window.high` 条 per destination per partition（rebalance / 重启时）。
- Solace **整体**不可达超过 3 min 会让 5 个熔断器全部 OPEN → 全部丢弃（RUNBOOK §5.2 给了停 app 保数据的操作）。
- 熔断器状态、窗口内容都在内存里，重启即失，从 CLOSED 重新开始。
- HALF_OPEN 不是严格的单条 probe（§6 第 3 条）。
- `connection.close()` 在独立线程上执行，不阻塞 consumer 线程；若它在网络中断时挂起，会占用一个线程和一个 Connection 直到 socket 超时（有界，未对真实 broker 实测）。

## 8. 测试

| 测试 | 验证的性质 |
|---|---|
| `PartitionWindowTest` | 水位只在全部 destination 确认后前进；乱序确认等待队首；只 ack 水位 record；pause/resume 阈值；skipped 记录按序越过；revoke 后不再 ack；1 个生产线程 + 3 个确认线程并发 20k 条时 ack 严格单调且到达末尾（重复 5 次） |
| `CircuitBreakerTest` | 状态机全部转移；同一轮内多个 sender 的失败只计一次；迟到的上一轮失败不计入、不会把 HALF_OPEN 打回 OPEN；RETRYING 真实阻塞并被 `onSuccess` 唤醒 |
| `DestinationSenderTest` | commit 失败 → 整批按原顺序重发、rollback 一次、只 ack 最后一条；重试耗尽 → 丢弃、水位仍前进、breaker OPEN；`stop()` 期间 commit 抛异常不计入熔断器、不 ack |
| `BridgeIntegrationTest` | 嵌入式 Kafka（KRaft，2 partition）+ 模拟事务语义的 JMS：d2 回滚两次后 3 个 destination 全部收到全部 offset，group offset 到 log end；d3 永久拒绝 → OPEN → 丢弃，d1/d2 照常，group offset 仍到 log end |

**JMS 层是对 Mockito 模拟的事务 session 测的，没有连过真实 Solace。** 首次对接真 broker 时要验证：`TransactionRolledBackException` 确实在 `commit()` 抛出；14 Connection × 5 transacted session 能建起来（RUNBOOK §2.1）。

本机 JDK 25 时 surefire 需要 `-Dnet.bytebuddy.experimental=true`（已在 pom），JDK 21 不需要。

## 9. 文件导航

```
src/main/java/com/example/kafkasolacebridge/   BridgeListener · PartitionWindow · Pending · PartitionWorker · DestinationSender
                                   CircuitBreaker · Transformer · AppProperties · KafkaConfig · SolaceConfig · BridgeMetrics
src/main/resources/application.yaml 全部可调参数，注释里有算术
src/test/java/com/example/kafkasolacebridge/   上表 4 个测试 + FakeSolace（模拟事务 JMS）
k8s/deployment.yaml                ConfigMap + Deployment（2 replicas、2Gi、heap 60%、grace 30s）
RUNBOOK.md                         broker 前置条件、retention 命令、指标告警、故障时间线、重放操作
```
