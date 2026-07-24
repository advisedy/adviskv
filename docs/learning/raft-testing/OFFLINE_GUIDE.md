# AdvisKV Raft 测试框架离线阅读教程

> 适用版本：当前工作区中已经拆分 `raft_protocol_test` 与 `raft_seeded_test` 的版本。
>
> 目标：即使没有网络、不能随时提问，也能独立读懂 AdvisKV 的 Raft 协议测试、seed 调度测试和持久化崩溃测试。
>
> 前置要求：会读基础 C++，知道类、函数、`std::vector`、`std::map`、`std::optional`、`std::variant` 的大致含义即可。不要求提前读过 etcd/raft，也不要求能背 Raft 论文。

---

## 0. 先说清楚：你到底要学会什么

读完这份教程，你应该能完成下面这些事情：

1. 解释为什么测试不直接启动 gRPC 服务，而是直接调用 `RaftCore`。
2. 解释 `RaftEffects` 是什么，以及它为什么是测试框架的关键接口。
3. 画出一次 RequestVote 和一次 AppendEntries 的完整消息往返。
4. 用 `pending_`、`deliver()`、`drop()`、`duplicate()`、`isolate()` 手工构造网络故障。
5. 解释协议测试、seed 测试、持久化测试分别证明什么、不证明什么。
6. 解释相同 seed 为什么能够复现相同的消息调度。
7. 解释 `RaftSafetyMonitor` 为什么既检查当前状态，也保存历史状态。
8. 解释 crash point 如何在真实文件写入流程中杀死子进程。
9. 随便拿到一个测试时，能按照“初始状态、刺激、消息顺序、断言、协议意义、测试边界”六步拆解。

最重要的一句话：

> 这套框架不是在证明 Raft 永远正确，而是在一个可控制、可复现的模型里，主动构造高风险场景，并检查 AdvisKV 当前实现有没有违反预期。

---

## 1. 飞机上的四小时阅读顺序

不要从第一个文件第一行一路读到最后一行。那样很容易在大量结构体和辅助函数中迷路。

建议按下面的节奏阅读。

### 第 1 小时：建立地图

依次阅读本教程：

1. 第 2 章：三层测试框架总览。
2. 第 3 章：最少必要的 Raft 概念。
3. 第 4 章：`RaftCore` 与 `RaftEffects`。
4. 第 5 章：基础 harness 的数据结构。
5. 第 6 章：一条消息如何完整走一圈。

这一小时不要看复杂测试。目标只是回答：

```text
RaftCore 产生的消息放在哪里？
测试如何决定消息何时送达？
收到请求后，响应又是怎样生成的？
```

### 第 2 小时：读确定性协议测试

依次阅读：

1. 第 8 章：选举测试。
2. 第 9 章：日志复制测试。
3. 第 10 章：成员变更与 snapshot。
4. 第 11 章：ReadIndex 与 recovering。

建议同时打开这些源码：

```text
test/storage/raft/core/raft_protocol_election_test.cpp
test/storage/raft/core/raft_protocol_replication_test.cpp
test/storage/raft/core/raft_protocol_membership_snapshot_test.cpp
test/storage/raft/core/raft_protocol_read_recovery_test.cpp
```

### 第 3 小时：读 seed 测试

依次阅读：

1. 第 12 章：scheduler。
2. 第 13 章：SafetyMonitor。
3. 第 14 章：完整 seeded partition 场景。

同时打开：

```text
test/storage/raft/core/raft_test_scheduler.h
test/storage/raft/core/raft_test_scheduler.cpp
test/storage/raft/core/raft_seeded_schedule_test.cpp
```

### 第 4 小时：读持久化、做练习

依次阅读：

1. 第 15 章：crash point。
2. 第 16 章：PersistEngine 崩溃测试。
3. 第 17 章：Replica 崩溃恢复测试。
4. 第 21 章：练习题。
5. 第 22 章：答案。

如果时间不够，优先做标有“必须会”的练习。

---

## 2. 整套测试框架的地图

当前框架可以理解成三层。

```text
第一层：确定性协议测试
  直接驱动 RaftCore
  手工控制 tick、投票、AppendEntries、response、snapshot
  不启动真实进程，不走 gRPC，不写真实磁盘

第二层：seeded 消息调度测试
  复用第一层的 RaftTestCluster
  用 seed 决定消息的投递顺序、丢弃和复制
  每一步都执行 SafetyMonitor

第三层：持久化和真实 Replica 崩溃测试
  使用真实 PersistEngine、真实文件、真实 ReplicaManager
  在关键位置 _exit(137)
  重启后检查 WAL、hard state、snapshot、KV 是否恢复
```

对应文件如下。

| 层次 | 主要文件 | 一句话职责 |
|---|---|---|
| 基础协议模型 | `raft_test_harness.h/.cpp` | 节点、内存状态、消息队列、网络分区、apply、snapshot |
| 选举测试 | `raft_protocol_election_test.cpp` | 投票、term、日志新旧、split vote、learner |
| 复制测试 | `raft_protocol_replication_test.cpp` | proposal、quorum、冲突日志、迟到 response |
| 成员与快照 | `raft_protocol_membership_snapshot_test.cpp` | learner、remove、snapshot、compact |
| 读与恢复 | `raft_protocol_read_recovery_test.cpp` | ReadIndex、recovering 状态 |
| seed 调度器 | `raft_test_scheduler.h/.cpp` | 伪随机选择、丢包、复制、trace |
| seed 场景 | `raft_seeded_schedule_test.cpp` | 固定业务故事 + 随机消息时序 + safety oracle |
| 文件持久化 | `persist_crash_recovery_test.cpp` | WAL record、rename、snapshot 文件原子性 |
| Replica 恢复 | `replica_crash_recovery_test.cpp` | commit/apply/snapshot 边界的真实组件重建 |

三个 CMake target 分别是：

```bash
./build/release/bin/test/raft_protocol_test
./build/release/bin/test/raft_seeded_test
./build/release/bin/test/persist_crash_recovery_test
./build/release/bin/test/replica_crash_recovery_test
```

这里持久化层拆成两个可执行文件，是因为它有两个观察尺度：

```text
PersistEngine 级别：文件有没有被写坏？
Replica 级别：整个 Raft + 状态机重启后，业务值还能不能读出来？
```

---

## 3. 阅读测试前必须知道的最少 Raft 概念

这一章不追求完整讲完 Raft，只讲读测试必需的概念。

### 3.1 三种角色

每个 Raft 节点有三种主要角色：

```text
FOLLOWER   跟随别人，不主动复制日志
CANDIDATE  发起选举，向其他 voter 要票
LEADER     接受写入，把日志复制给 follower
```

正常情况下，大多数时间是：

```text
节点 0：LEADER
节点 1：FOLLOWER
节点 2：FOLLOWER
```

### 3.2 term 是什么

`term` 可以理解成“第几届领导班子”。

```text
term 1：节点 0 当 leader
term 2：节点 0 失联，节点 1 当 leader
term 3：又发生一次选举
```

重要规则：

1. 节点的 `current_term` 不能倒退。
2. 同一个 Raft group 的同一个 term，不能合法产生两个不同 leader。
3. 节点看到合法成员发来的更高 term 消息，通常要更新 term 并退回 follower。

### 3.3 日志是什么

每次写操作先变成 `LogEntry`。

```text
index=1, term=1, op=NONE        leader 当选时的 no-op
index=2, term=1, op=PUT k1=v1  客户端写入
index=3, term=1, op=DEL k2     客户端删除
```

`index` 是日志位置，`term` 是这条日志由哪一届 leader 产生。

同一个 index 最终只能对应一个已提交内容。但在日志尚未提交时，不同节点同一个 index 暂时出现不同内容是可能的，新 leader 会覆盖旧 leader 的未提交冲突后缀。

### 3.4 `last_log_index`、`commit_index`、`last_applied`

这是最容易混淆的三个数字。

假设一个节点当前有 10 条日志：

```text
last_log_index = 10
commit_index   = 8
last_applied   = 6
```

含义是：

```text
1..10：本地已经拥有
1..8 ：已经被认为 committed，不应再被覆盖
1..6 ：已经真正执行到 KV 状态机
7..8 ：已经 committed，但后台 apply 还没执行
9..10：只有日志，还没有 committed
```

因此正常情况下：

```text
last_applied <= commit_index <= last_log_index
```

如果还有 snapshot，则：

```text
snapshot_index <= last_applied <= commit_index <= last_log_index
```

这些量中：

- `current_term` 不能回退。
- `snapshot_index` 不能回退。
- `last_applied` 不能回退。
- `commit_index` 不能回退。
- `last_log_index` 不一定单调，因为未提交的冲突后缀可以被截断。

### 3.5 quorum 是什么

quorum 就是 voter 多数派。

| voter 数 | quorum |
|---:|---:|
| 1 | 1 |
| 2 | 2 |
| 3 | 2 |
| 4 | 3 |
| 5 | 3 |

learner 不参与选举 quorum，也不参与写入提交 quorum。

例如 3 voter + 1 learner：

```text
合法 quorum = 3 个 voter 中至少 2 个
leader 自己 + learner = 不够
leader 自己 + 任意 1 个 voter = 足够
```

### 3.6 no-op 为什么存在

AdvisKV 的 leader 当选后会在当前 term 追加一条 `NONE` 日志。

作用可以先简单理解为：

1. 证明新 leader 能在当前 term 获得多数派。
2. 让旧 term 已复制但未提交的日志，随着当前 term 日志一起被安全提交。
3. ReadIndex 在当前 term 没有 committed entry 时拒绝服务，避免新 leader 尚未确认领导权就提供线性一致读。

### 3.7 snapshot 是什么

日志越来越长以后，可以把已经 apply 的 KV 状态打包成 snapshot。

```text
snapshot index = 100
snapshot term  = 5
snapshot KV    = 执行完 1..100 后的完整 KV
后续 WAL       = 101, 102, 103 ...
```

如果 follower 落后太多，leader 已经删除了旧日志，就不能再从 index 1 一条条补，只能先发送 snapshot，再继续补 snapshot 后的增量日志。

### 3.8 membership 中的 voter、learner、non-member

```text
VOTER       可以竞选，可以投票，计入 quorum
LEARNER     只追数据，不竞选，不计入 quorum
NON_MEMBER  不再属于 group
```

AdvisKV 的成员添加大致是：

```text
先 ADD_LEARNER
    ↓
learner 追日志
    ↓
追平后 PROMOTE_VOTER
```

### 3.9 recovering 是 AdvisKV 自己的状态

`recovering` 不是 Raft 论文的标准角色，而是 AdvisKV 为恢复流程增加的状态。

recovering 节点：

- 不应该发起选举。
- 不应该投票。
- 不应该接受客户端 proposal。
- 需要通过 AppendEntries 或 snapshot 追到恢复目标后，才能回到 ready。

---

## 4. `RaftCore` 与 `RaftEffects`：理解整个框架的钥匙

### 4.1 `RaftCore` 是什么

`RaftCore` 可以近似理解成一台“Raft 状态机”。

你给它一个输入：

```text
tick
RequestVote request
RequestVote response
AppendEntries request
AppendEntries response
客户端 proposal
snapshot response
```

它会做两类事情：

1. 修改自己的内存状态，例如 role、term、日志、commit index。
2. 生成需要由外部执行的副作用，也就是 `RaftEffects`。

### 4.2 为什么不让 `RaftCore` 直接发 RPC、写磁盘

如果 `RaftCore` 内部直接：

```text
调用 gRPC
打开文件
启动线程
等待网络
```

测试就很难精确控制消息顺序。

现在的设计是：

```text
RaftCore 只说“我想做什么”
外层 ReplicaLoop 或测试 harness 决定“什么时候真正做”
```

这就是 `RaftEffects` 的意义。

### 4.3 `RaftEffects` 四个字段

定义在：

```text
src/storage/model/model.h
```

结构可以简化成：

```cpp
struct RaftEffects {
    optional<RaftMeta> hard_state;
    vector<LogEntry> entries_to_append;
    optional<vector<LogEntry>> entries_to_rewrite;
    vector<RaftMessage> messages;
};
```

分别表示：

| 字段 | 含义 |
|---|---|
| `hard_state` | term 或 voted_for 变化，需要持久化 |
| `entries_to_append` | 新日志，需要追加到 WAL |
| `entries_to_rewrite` | 冲突日志出现，需要重写 WAL |
| `messages` | 需要发送给其他节点的 Raft 请求 |

例如节点 0 变成 candidate，可能产生：

```text
hard_state = term 1, voted_for node0
messages   = RequestVote to node1, RequestVote to node2
```

例如 leader 接受一次 PUT，可能产生：

```text
entries_to_append = [index 2, PUT k=v]
messages = AppendEntries to node1, AppendEntries to node2
```

### 4.4 生产环境与测试环境如何消费 effects

生产环境中：

```text
RaftCore
  ↓ 产生 RaftEffects
ReplicaLoop::run_step
  ↓
persist_raft_effects       写 hard state / WAL
  ↓
message_dispatcher        发真实 RPC
```

测试环境中：

```text
RaftCore
  ↓ 产生 RaftEffects
RaftTestCluster::accept_effects
  ↓
把 effects.messages 转成 Envelope
  ↓
放入 pending_，暂时不发送
```

测试之所以能够乱序、丢包、复制消息，核心原因就是消息先进入了 `pending_`，而不是立即送到对端。

### 4.5 第一处重要边界

协议 harness 不会真正持久化 `hard_state`、`entries_to_append` 和 `entries_to_rewrite`。

它保存 `last_effects` 供断言查看，但主要负责把 `effects.messages` 送进模拟网络。

这不是遗漏，而是分层：

```text
协议 harness：验证状态转换和消息协议
持久化测试：验证文件与崩溃恢复
```

不要拿协议 harness 的通过结果去声称 WAL 一定可靠。

---

## 5. 基础 harness 的所有重要数据结构

源码：

```text
test/storage/raft/core/raft_test_harness.h
test/storage/raft/core/raft_test_harness.cpp
```

### 5.1 `TestNodeId` 与真实 `ReplicaID`

测试里同时存在两种节点标识。

```cpp
using TestNodeId = int32_t;
```

`TestNodeId` 是测试数组下标：

```text
0、1、2
```

`ReplicaID` 是生产代码使用的真实逻辑标识：

```text
table_id + shard_index + replica_seq
```

测试用 `TestNodeId` 是为了写起来简单：

```cpp
cluster.core(0)
cluster.isolate(2)
```

发送给 `RaftCore` 的请求仍然使用真实 `ReplicaID`，通过下面两个函数转换：

```cpp
ReplicaID replica_id(TestNodeId node) const;
TestNodeId find_node(const ReplicaID& replica_id) const;
```

### 5.2 `RaftTestNodeSpec`：节点出生说明书

它用于构造各种特殊初始状态。

```cpp
struct RaftTestNodeSpec {
    PeerMember self;
    vector<PeerMember> initial_voters;
    RaftMeta hard_state;
    vector<LogEntry> entries;
    optional<vector<RaftMember>> membership;
    optional<RaftTestSnapshot> snapshot;
    map<Key, Value> kv;
    bool recovering;
    int32_t election_ticks;
    int32_t heartbeat_ticks;
};
```

你可以把它理解为：

```text
创建这个节点时：
它是谁？
最初有哪些 voter？
当前 term 和 voted_for 是什么？
磁盘恢复出了哪些日志？
当前成员视图是什么？
有没有 snapshot？
状态机中有哪些 KV？
是不是 recovering？
多久选举超时？
多久发一次心跳？
```

普通三节点测试不需要手写这些字段，直接使用：

```cpp
RaftTestCluster cluster = RaftTestCluster::voters(3);
```

只有日志新旧、recovering、成员视图不一致等特殊测试，才直接构造 `RaftTestNodeSpec`。

### 5.3 为什么测试时钟是固定的

生产环境的选举 timeout 通常要随机化，防止多个 follower 同时竞选。

测试中 `add_node()` 注入：

```cpp
timing.next_election_timeout = [election_ticks]() {
    return election_ticks;
};
```

这使得“第 10 个 tick 必然竞选”可以稳定复现。

否则同一个测试今天第 8 次 tick 竞选，明天第 13 次 tick 竞选，就会不稳定。

### 5.4 `RaftTestSnapshot`

协议层 snapshot 不写真实文件，只保存内存镜像：

```cpp
struct RaftTestSnapshot {
    LogIndex index;
    Term term;
    vector<RaftMember> members;
    map<Key, Value> kv;
};
```

也就是：

```text
snapshot 的位置和 term
snapshot 当时的成员配置
snapshot 当时的完整 KV 状态
```

### 5.5 `Node`：测试框架中的一个节点

`RaftTestCluster` 内部的 `Node` 包含：

```cpp
struct Node {
    PeerMember self;
    unique_ptr<RaftCore> core;
    map<Key, Value> kv;
    map<LogIndex, RaftTestSnapshot> snapshots;
    RaftEffects last_effects;
    int32_t election_ticks;
};
```

注意这里没有：

```text
gRPC Server
真实线程池
PersistEngine
真实 WAL 文件
真实网络 socket
```

所以第一层非常快，几十个协议测试只需要几毫秒。

### 5.6 为什么 request 和 response 都要放进 `variant`

生产代码的 `RaftMessage` 只表示主动发出的 request：

```text
RequestVote
AppendEntries
InstallSnapshot
```

但测试要控制完整往返，所以 response 也必须成为可排队消息。

因此测试定义了六种 payload：

```text
VoteRequestPayload
VoteResponsePayload
AppendRequestPayload
AppendResponsePayload
SnapshotRequestPayload
SnapshotResponsePayload
```

再用：

```cpp
using RaftTestPayload = std::variant<...>;
```

装进统一的 `RaftTestEnvelope`。

### 5.7 `RaftTestEnvelope`：模拟网络中的信封

每条消息都被包装成：

```cpp
struct RaftTestEnvelope {
    TestMessageId id;
    TestNodeId from;
    TestNodeId to;
    RaftTestPayload payload;
};
```

类比真实快递：

```text
id       快递单号
from     发件节点
to       收件节点
payload  信封里的 RequestVote / AppendEntries / response
```

`describe()` 会把信封转换为 trace 文本，例如：

```text
#12 0->2 append-request term=3 prev=8:2 entries=4 commit=7
```

含义：

```text
消息 12
节点 0 发给节点 2
term 3
要求 follower 在 prev index 8 / term 2 后追加 4 条日志
leader 当前 commit 到 7
```

### 5.8 `pending_`：整个框架最重要的容器

```cpp
std::vector<RaftTestEnvelope> pending_;
```

它就是模拟网络。

消息进入 `pending_` 后，不会自动到达对方。测试可以选择：

```cpp
cluster.deliver(id);    // 现在送达
cluster.drop(id);       // 丢掉
cluster.duplicate(id);  // 复制一份
cluster.take(id);       // 取出来但不处理
cluster.deliver_all();  // 按队列顺序全部处理
```

能够控制 `pending_`，就能够构造 Raft 中的大部分消息时序问题。

---

## 6. 一条 RequestVote 消息如何完整走一圈

这一章必须完全看懂。后面的 AppendEntries 和 snapshot 都是同一个模式。

假设三节点集群中，节点 0 超时竞选。

### 6.1 创建集群

```cpp
RaftTestCluster cluster = RaftTestCluster::voters(3);
```

内部大致发生：

```text
创建 member0、member1、member2
为每个 member 创建一个 RaftCore
所有节点初始都是 follower、term 0
所有节点都认为 0/1/2 是 voter
```

### 6.2 触发竞选

```cpp
cluster.campaign(0);
```

`campaign(0)` 并不是直接调用私有的 `become_candidate()`，而是不断：

```cpp
tick(0);
```

直到节点 0 的 term 增加。

这样测试走的是生产逻辑的选举 timeout 路径，不需要暴露“强制变 candidate”的测试专用入口。

### 6.3 `tick()` 调用 `RaftCore`

```cpp
void RaftTestCluster::tick(TestNodeId id) {
    RaftEffects effects;
    node(id).core->tick(effects);
    accept_effects(id, std::move(effects));
}
```

在第 10 个 tick，节点 0：

```text
FOLLOWER -> CANDIDATE
term 0 -> term 1
先给自己一票
生成发往节点 1、2 的 RequestVote
```

### 6.4 RequestVote 先进入 pending，不立即送达

`accept_effects(0, effects)` 遍历 `effects.messages`，调用：

```cpp
request_envelope(0, message)
```

将生产 `RaftMessage` 转成测试 `RaftTestEnvelope`，再：

```cpp
enqueue(envelope)
```

这时队列大概是：

```text
#1 0->1 vote-request term=1 log=0:0
#2 0->2 vote-request term=1 log=0:0
```

节点 1、2 还完全没有收到消息。

### 6.5 投递 request

```cpp
cluster.deliver(1);
```

内部步骤：

```text
take #1，从 pending_ 删除
检查 0->1 链路是否 blocked
调用 deliver_unblocked
识别 payload 是 VoteRequestPayload
调用 node1.core.handle_request_vote(...)
收集 node1 产生的 effects
生成 node1 -> node0 的 VoteResponse
把 response 放回 pending_
```

此时 pending 可能是：

```text
#2 0->2 vote-request
#3 1->0 vote-response granted=true
```

### 6.6 投递 response

```cpp
cluster.deliver(3);
```

内部调用：

```cpp
node0.core.handle_vote_response(
    from=node1,
    response={term=1, granted=true},
    effects
);
```

三 voter 的 quorum 是 2。节点 0 已经有自己的票，再得到节点 1 的票，就可以成为 leader。

成为 leader 后：

```text
追加当前 term 的 no-op
为 follower 生成 AppendEntries
```

所以投递一次 VoteResponse 后，pending 中又可能出现 AppendEntries。

### 6.7 `deliver_all()` 为什么可能处理很多轮

```cpp
cluster.deliver_all();
```

不是只处理调用时已经存在的消息。

因为处理 request 会生成 response，处理 response 又可能生成新的 AppendEntries，所以它会循环直到 `pending_` 为空，或者超过 `max_events`。

完整链路可能是：

```text
VoteRequest
  -> VoteResponse
    -> leader 当选
      -> AppendEntries(no-op)
        -> AppendResponse
          -> leader 推进 commit
```

### 6.8 画成一张图

```text
测试代码             RaftTestCluster          node0 RaftCore       node1 RaftCore
   |                       |                        |                    |
   | campaign(0)           |                        |                    |
   |---------------------->| tick x 10              |                    |
   |                       |----------------------->| become candidate   |
   |                       |<-----------------------| effects.messages   |
   |                       | enqueue VoteRequest    |                    |
   |                       |                        |                    |
   | deliver(request)      |                        |                    |
   |---------------------->|-------------------------------------------->|
   |                       |                        |   handle vote       |
   |                       |<--------------------------------------------|
   |                       | enqueue VoteResponse   |                    |
   |                       |                        |                    |
   | deliver(response)     |                        |                    |
   |---------------------->|----------------------->| handle response    |
   |                       |                        | become leader      |
   |                       |<-----------------------| AppendEntries      |
```

---

## 7. harness 的操作手册

这一章可以在忘记某个 API 时回来查。

### 7.1 推进时间

```cpp
cluster.tick(0);      // 只推进节点 0 一次
cluster.tick_all();   // 所有节点各推进一次，但不自动 deliver pending
cluster.campaign(0);  // 推进节点 0，直到它发起下一 term 竞选
```

注意：`tick_all()` 只产生消息，不自动投递消息。

### 7.2 发起 proposal

```cpp
auto result = cluster.propose(
    0,
    ProposeParam::write(WriteOpType::PUT, "k", "v")
);
```

返回：

```text
status：是不是 leader、是不是 recovering、参数是否合法
index ：成功时新日志的 index，失败时通常是 -1
```

`propose()` 不代表已经 committed。它只代表 leader 接受 proposal 并生成复制消息。

### 7.3 查询消息

```cpp
cluster.pending()
cluster.message_ids(kind, from, to)
cluster.first_message(kind, from, to)
cluster.pending_count(kind, from, to)
```

例如：

```cpp
auto id = cluster.first_message(
    RaftTestMessageKind::APPEND_REQUEST,
    0,
    2
);
```

含义是找第一条节点 0 发往节点 2 的 AppendEntries request。

### 7.4 操作消息

```cpp
cluster.deliver(id);    // 投递并执行
cluster.drop(id);       // 删除，模拟丢包
cluster.duplicate(id);  // 保留原消息，再复制一条新 id 的消息
cluster.take(id);       // 从 pending 取出，调用方自己检查
cluster.fail(id, error);// 模拟发送失败回调
cluster.drop_all();     // 清空所有 pending
```

`take()` 与 `drop()` 的区别：

```text
take：我要拿到 Envelope，查看里面的 request 参数
drop：我只想模拟消息丢失，不关心内容
```

### 7.5 网络分区

```cpp
cluster.cut(0, 1);     // 只阻断 0 -> 1
cluster.isolate(0);    // 双向阻断 0 与所有其他节点
cluster.heal(0, 1);    // 恢复一条有向链路
cluster.heal_all();    // 恢复所有链路
```

被阻断的消息在 `deliver()` 时会被记录为：

```text
blocked-drop
```

然后直接丢弃，而不是留在网络中等恢复。

因此当前 partition 模型更接近：

```text
分区期间发出的包全部丢失
```

不是：

```text
分区期间包一直排队，恢复后突然全部到达
```

### 7.6 apply

```cpp
cluster.apply(0);
cluster.apply_all();
```

它会取出 `commit_index` 以内但尚未 applied 的日志：

```text
PUT -> 写入 Node::kv
DEL -> 从 Node::kv 删除
配置变更 -> 调用 apply_config_entry
最后推进 last_applied
```

协议层的 `Node::kv` 是一个简单 `std::map`，用于验证所有节点最终状态机是否一致。

### 7.7 compact

```cpp
cluster.compact(node, index);
```

当前 harness 要求：

```text
index 必须等于当前 last_applied
```

然后：

```text
RaftCore truncate_log(index)
保存当前 members
保存当前 KV
记录 RaftTestSnapshot
```

为什么不能 compact 到尚未 applied 的 index？

因为 snapshot 保存的是状态机结果。如果日志还没执行，状态机就不包含那条日志的效果，snapshot 会说谎。

### 7.8 trace

```cpp
cluster.trace()
scheduler.trace()
```

测试失败时一定优先看 trace，而不是只看最后一个 `EXPECT_EQ`。

trace 能回答：

```text
哪条消息先到？
哪条消息被丢了？
节点什么时候产生 response？
旧 response 是否在新 term 到达？
```

---

## 8. 如何阅读选举测试

源码：

```text
test/storage/raft/core/raft_protocol_election_test.cpp
```

### 8.1 先认识文件里的辅助函数

`vote_request(...)`：构造一个 RequestVote 参数。

`enqueue_vote_request(...)`：直接向模拟网络塞一条 RequestVote。它不要求发送方真的已经成为 candidate，因此适合精确构造边界场景。

`enqueue_vote_response(...)`：直接塞一条 VoteResponse。它可以构造重复响应、旧 term 响应，甚至未知 ReplicaID 的响应。

`enqueue_heartbeat(...)`：构造 entries 为空的 AppendEntries，也就是心跳。

`take_vote_response(...)`：从 pending 中取出 response，只检查内容，不把 response 再投递给 candidate。

`log_ending_at(index, term)`：快速给节点制造指定末尾 index/term 的日志，用于投票时比较日志新旧。

### 8.2 必须会：固定 timeout 选出 leader

测试：

```text
FixedTimeoutElectsLeaderWithQuorum
```

它分别用 1、3、5 个节点运行。

初始状态：

```text
所有节点 follower
所有节点 term 0
所有日志为空
选举 timeout 固定为 10 tick
```

操作：

```cpp
for (int tick = 0; tick < 9; ++tick)
    cluster.tick(0);
```

前 9 次不应该竞选。

第 10 次：

```cpp
cluster.tick(0);
cluster.deliver_all();
```

完整效果：

```text
node0 变 candidate，term=1，自投一票
node0 给其他 voter 发 RequestVote
其他 voter 返回 VoteResponse
node0 得到 quorum，变 leader
node0 追加 term=1/index=1 的 no-op
no-op 复制并提交
```

最终断言不只看 role，还看：

```text
term == 1
last_log_index == 1
last_log_term == 1
commit_index == 1
```

这比只写 `EXPECT_TRUE(is_leader())` 更强，因为它同时验证了 leader 当选后的 no-op 行为。

### 8.3 必须会：一个 term 只能投给一个 candidate

测试：

```text
GrantsOneVotePerTermAndRepeatsVoteForSameCandidate
```

节点 2 依次收到：

```text
node0 term=1 要票 -> 同意
node1 term=1 要票 -> 拒绝
node0 term=1 重试 -> 仍然同意
```

为什么第三次仍然同意？

网络可能丢失第一次响应。candidate 重试同一 term 的请求时，follower 应该重复确认自己已经投过的同一个 candidate，不能因为“已经投过”就一律拒绝。

### 8.4 日志新旧比较为什么先看 term

测试：

```text
ComparesLastTermBeforeLastIndex
```

规则是：

```text
先比较 last_log_term
term 大的日志更新
只有 term 相同，才比较 last_log_index
```

例如：

```text
本地：last term=2, index=2
candidate：last term=1, index=100
```

candidate 虽然更长，但最后 term 更旧，仍然不能得到票。

反过来：

```text
本地：last term=2, index=100
candidate：last term=3, index=1
```

candidate 的最后 term 更新，可以得到票。

参数化测试把六种组合都列出来，比手写六个重复测试更容易审查。

### 8.5 split vote 如何被手工制造

测试：

```text
SplitVoteConvergesInTheNextTerm
```

四个节点，node0 和 node1 同时竞选。

手工投递：

```text
node2 投 node0
node3 投 node1
node0 和 node1 各自保留自己的票
刻意丢弃能打破平局的交叉消息
```

最终 term 1：

```text
node0 两票
node1 两票
quorum=3
没有 leader
```

再让 node0 发起 term 2 竞选，完整投递消息，最终 node0 成为 leader。

这个测试说明 harness 的价值：真实网络很难稳定制造恰好 2:2，但手工 pending queue 可以每次都制造成功。

### 8.6 选举测试完整目录

| 测试 | 它主要验证什么 |
|---|---|
| `FixedTimeoutElectsLeaderWithQuorum` | 固定 timeout、1/3/5 节点 quorum、leader no-op |
| `GrantsOneVotePerTermAndRepeatsVoteForSameCandidate` | 每 term 一票、同 candidate 重试幂等 |
| `ComparesLastTermBeforeLastIndex` | candidate 日志新旧判断 |
| `DuplicateVoteResponseDoesNotIncreaseQuorum` | 同一 voter 的重复 response 不能重复计票 |
| `IgnoresStaleMessagesAndStepsDownForHigherTermVoter` | 忽略旧 term，合法高 term 使 candidate 退位 |
| `CandidateFallsBackOnSameTermAppendEntries` | candidate 收到同 term leader 心跳要退为 follower |
| `HeartbeatResetsElectionTimeout` | 心跳重置完整选举计时 |
| `SplitVoteConvergesInTheNextTerm` | 平票后下一 term 收敛 |
| `LearnerAndNonMemberDoNotCampaign` | learner、non-member 不能竞选 |
| `VoterRejectsLearnerAndNonMemberCandidates` | 非 voter 的 RequestVote 被拒绝 |

### 8.7 六个 disabled 测试是什么意思

当前协议 target 会显示 6 个 disabled tests，其中选举文件有 4 个：

```text
DISABLED_LearnerCanVoteForKnownVoter
DISABLED_LearnerVoteResponseDoesNotCountTowardQuorum
DISABLED_UnknownVoteResponseDoesNotCountTowardQuorum
DISABLED_UnknownHigherTermVoteResponseDoesNotChangeElection
```

`DISABLED_` 不是“已经通过但暂时不跑”，而是：

```text
测试描述了期望行为
当前生产实现不满足
默认测试运行时跳过
```

其中最需要警惕的是 candidate 对 VoteResponse 来源的校验：learner 或未知节点的同意票不应被计入 voter quorum。

面试时不能说“所有 etcd 场景都通过”。准确说法是：

```text
新版测试补出了若干已知缺陷，其中部分尚未修复，使用 disabled case 明确记录。
```

---

## 9. 如何阅读日志复制测试

源码：

```text
test/storage/raft/core/raft_protocol_replication_test.cpp
```

### 9.1 最常用辅助函数：一次 AppendEntries 往返

```cpp
void deliver_append_round_trip(cluster, leader, follower)
```

它做两步：

```text
1. 找 leader -> follower 的 APPEND_REQUEST 并投递
2. 找 follower -> leader 的 APPEND_RESPONSE 并投递
```

为什么不能只投递 request？

因为 follower 收到日志后，leader 还不知道复制成功。只有 response 回到 leader，leader 才会更新该 follower 的 match/next index，并尝试推进 commit index。

### 9.2 leader proposal 不等于 committed

测试：

```text
LeaderProposalBuildsAppendEntriesAndFollowerRejectsProposal
```

先在 follower 上 proposal：

```text
status = NOT_LEADER
index = -1
```

再在 leader 上 proposal：

```text
leader 本地追加 index=2 的 PUT
effects.entries_to_append 包含这条日志
pending 中出现发往两个 follower 的 AppendEntries
```

此时只证明 proposal 被 leader 接受，不代表已经达到多数派。

### 9.3 必须会：quorum 什么时候推进 commit

测试：

```text
CommitsOnlyAfterVoterQuorumAcknowledges
```

以 5 节点为例：

```text
leader 自己保存日志                 1 份
node1 response                      2 份，不够
node2 response                      3 份，达到 quorum
commit_index 推进
```

为什么代码中的 `required_follower_acks = node_count / 2`？

因为 leader 自己已经算一份：

```text
1 节点：需要 0 个 follower ack
3 节点：需要 1 个 follower ack
5 节点：需要 2 个 follower ack
```

### 9.4 follower 为什么不能自己猜 commit

测试：

```text
CommitPropagatesOnLaterHeartbeat
```

可能发生：

```text
node2 收到了日志
但它收到 request 时，leader_commit 还是旧值
之后 node1 response 到达 leader，leader 才真正 commit
```

node2 不能因为“我也有这条日志”就自行认为 committed。leader 需要在后续心跳中携带新的 `leader_commit`，node2 才推进 commit index。

### 9.5 follower 如何处理冲突日志

AppendEntries 带两个关键前置字段：

```text
prev_log_index
prev_log_term
```

leader 的意思是：

```text
“你必须在 prev_log_index 位置拥有 prev_log_term，
如果这一点成立，再把 entries 接到后面。”
```

如果前缀对不上，follower 拒绝。

如果前缀对得上，但后续某个 index term 冲突：

```text
保留共同前缀
从第一个冲突位置删除旧后缀
追加 leader 的新后缀
```

`LeaderBacksOffUntilDivergentFollowerConverges` 验证 leader 收到拒绝后，会逐步回退 follower 的 `next_index`，直到找到共同前缀。

### 9.6 必须会：旧 leader 未提交日志被覆盖

测试：

```text
NewLeaderOverwritesUncommittedOldLeaderEntry
```

故事如下：

```text
1. node0 是 term1 leader
2. 隔离 node0
3. node0 接受 old=uncommitted，但没有多数派，不能 commit
4. node1、node2 形成多数派，node1 在新 term 当 leader
5. node1 提交 new=committed
6. 恢复 node0 网络
7. node1 把新日志复制给 node0
8. node0 的 old 未提交后缀被覆盖
```

最终：

```text
KV 中没有 old
KV 中有 new
node0 日志与 node1 一致
node0 已退为 follower
```

这个测试同时覆盖：网络分区、旧 leader 仍接受 proposal、重新选举、冲突覆盖、最终状态机一致。

### 9.7 必须会：为什么旧 term 日志不能直接按数量提交

测试：

```text
PreviousTermEntriesCommitOnlyWithCurrentTermEntry
```

初始所有节点都有 term1 的 index1、2，但这些日志没有被记录为 committed。

node0 在 term2 当选，并追加 term2/index3 的 no-op。

测试先伪造一个 response，说 follower 已经拥有到 index2：

```text
index2 看起来已经出现在多数派
但它属于旧 term1
leader 不能只因为数量够了，就直接把 commit_index 推到2
```

因此第一次断言：

```text
commit_index == 0
```

然后 follower 确认 term2/index3：

```text
当前 term 日志达到多数派
leader 可以 commit index3
index1、2 也随之被间接提交
```

最终：

```text
commit_index == 3
```

这是 Raft 中非常容易被写错的规则，也是面试高频追问。

### 9.8 为什么要测重复和迟到 response

真实网络/RPC 层可能出现：

```text
重试导致重复 response
旧请求的 response 比新请求更晚到
旧 term response 在节点重新当 leader 后才到
```

因此有这些测试：

```text
DuplicateSuccessAndStaleRejectDoNotRegressProgress
DelayedPreviousTermResponseIsIgnoredByNewLeader
OutOfOrderAppendIsRejectedThenSucceedsAfterPrefix
HigherTermAppendResponseForcesLeaderToStepDown
```

核心原则：

```text
成功过的复制进度不能被旧 reject 回退
旧 term response 不能污染新 term 的 progress
高 term 合法 response 必须让旧 leader 退位
```

### 9.9 复制测试完整目录

| 测试 | 验证目标 |
|---|---|
| `NewLeaderAppendsCurrentTermNoopAndBroadcastsIt` | leader no-op 与首次广播 |
| `LeaderProposalBuildsAppendEntriesAndFollowerRejectsProposal` | leader/follower proposal 行为 |
| `CommitsOnlyAfterVoterQuorumAcknowledges` | 1/3/5 节点 commit quorum |
| `CommitPropagatesOnLaterHeartbeat` | leader_commit 后续传播 |
| `MinorityCannotCommitAProposal` | 少数派不能提交 |
| `AcceptsMatchingPrefixAndRewritesConflicts` | follower 追加与冲突覆盖参数表 |
| `FollowerRejectsGapAndPreviousTermMismatch` | 拒绝日志空洞与错误 prev term |
| `LeaderBacksOffUntilDivergentFollowerConverges` | next index 回退寻找共同前缀 |
| `LaggingFollowerCatchesUpAfterPartitionHeals` | 分区恢复后增量追日志 |
| `NewLeaderOverwritesUncommittedOldLeaderEntry` | 新 leader 覆盖旧未提交后缀 |
| `PreviousTermEntriesCommitOnlyWithCurrentTermEntry` | Figure 8/current-term commit 规则 |
| `DuplicateSuccessAndStaleRejectDoNotRegressProgress` | 重复/迟到 response 不回退 progress |
| `DelayedPreviousTermResponseIsIgnoredByNewLeader` | 旧 term response 隔离 |
| `OutOfOrderAppendIsRejectedThenSucceedsAfterPrefix` | 乱序 AppendEntries |
| `HigherTermAppendResponseForcesLeaderToStepDown` | 更高 term response 使 leader 退位 |
| `CapsAppendBatchAndReplicatesRemainingEntries` | 单次复制批量上限与后续追赶 |

---

## 10. 成员变更与 snapshot 测试

源码：

```text
test/storage/raft/core/raft_protocol_membership_snapshot_test.cpp
```

### 10.1 learner 加入的完整故事

测试：

```text
LearnerCatchesUpAndIsAutomaticallyPromoted
```

大致流程：

```text
leader 提议 ADD_LEARNER
配置变更日志复制并 apply
新节点作为 learner 加入
learner 追上已有日志
leader 发现 learner ready
leader 再提议 PROMOTE_VOTER
所有节点 apply 后看到它是 voter
```

这里要区分两个动作：

```text
日志 committed：配置变更已经达成共识
日志 applied：本节点的 membership 视图真正更新
```

### 10.2 learner 为什么不能算写 quorum

测试：

```text
LearnerAcknowledgementDoesNotCountTowardWriteQuorum
```

即使 learner 已经复制成功，也不能替代 voter 的确认。否则 leader + learner 可能在 voter 多数派不可达时提交写入，破坏安全性。

### 10.3 为什么重复成员变更要幂等

控制面可能因为超时重试同一个请求：

```text
ensure_add_learner(X)
网络超时
再次 ensure_add_learner(X)
```

第二次不能再追加一条重复配置日志，应该返回 OK 并沿用 pending change。

但如果 A 的成员变更尚未完成，此时请求另一个不同成员变更 B，应返回 RETRY，避免并行配置变化。

对应测试：

```text
RepeatedAddIsIdempotentAndDifferentChangeRetries
RepeatedRemoveIsIdempotentAndDifferentChangeRetries
NewLeaderRecognizesUnappliedConfigEntry
```

### 10.4 remove member 后发生什么

```text
RemovedFollowerReceivesNoMessagesAndSmallerQuorumCommits
RemovingLeaderMakesItANonMemberAndStopsTicks
```

验证点：

1. removed 节点变成 `NON_MEMBER`。
2. leader 不再向 removed 节点复制。
3. quorum 按新的 voter 集合计算。
4. 如果被删除的是 leader 自己，它必须退位。
5. non-member 后续 tick 不能再次发起竞选或心跳。

### 10.5 snapshot request 为什么携带完整 image

生产 `InstallSnapshotParam` 主要描述：

```text
index、term、offset、data、done
```

协议 harness 为了避免真实文件和分块传输，额外在 `SnapshotRequestPayload` 中带：

```text
members
完整 KV map
```

所以 deliver snapshot request 时，harness 会：

```text
prepare_install_snapshot
commit_install_snapshot
恢复 Node::kv
保存 RaftTestSnapshot
生成 SnapshotResponse
```

这验证的是 snapshot 协议状态，不是磁盘分块传输可靠性。

### 10.6 必须会：落后 follower 先 snapshot 再追增量

测试：

```text
LaggingFollowerInstallsSnapshotThenReceivesIncrementalLog
```

故事：

```text
node2 长期落后
leader 已 apply 多条日志并 compact
旧日志前缀已经不存在
leader 不能再从 node2 的 next index 正常发 AppendEntries
leader 改发 snapshot
node2 安装 snapshot，恢复 members 和 KV
leader 再发送 snapshot 后产生的新日志
node2 最终与 leader 收敛
```

### 10.7 snapshot inflight 是什么

snapshot 可能很大，发送尚未完成时，不应该每个 tick 都给同一个 follower 再启动一份 snapshot。

因此 leader 为 follower 记录：

```text
inflight_snapshot_index
```

对应测试：

```text
AllowsOnlyOneInflightSnapshotPerFollower
SendFailureClearsInflightAndAllowsRetry
SuccessUsesActuallySentIndexAfterLeaderCompactsAgain
PreviousTermResponseDoesNotChangeNewLeaderProgress
```

这几项共同验证：

```text
发送中不重复启动
发送失败后允许重试
response 必须对应当时真正发出的 snapshot
旧 term response 不能污染新 leader progress
```

### 10.8 snapshot watermark

如果 follower 说：

```text
这个 snapshot 我已经有了，甚至我已经到更高位置
```

leader 可以用 response 中的 `snapshot_watermark` 推进该 follower 的复制进度，而不是从很旧的位置重新尝试。

对应：

```text
AlreadyExistAdvancesProgressToReturnedWatermark
```

### 10.9 为什么 compact 不能超过 applied

测试：

```text
CompactCannotMoveBeyondAppliedIndex
```

如果日志 index 1 已 committed，但还没 apply：

```text
commit_index = 1
last_applied = 0
```

这时不能 compact 到 1，因为 KV 状态机还没有 index1 的效果。先 apply，再 compact 才合法。

### 10.10 成员与 snapshot 测试目录

| 测试 | 验证目标 |
|---|---|
| `LearnerCatchesUpAndIsAutomaticallyPromoted` | learner 加入、追平、提升 |
| `LearnerAcknowledgementDoesNotCountTowardWriteQuorum` | learner 不参与提交 quorum |
| `RepeatedAddIsIdempotentAndDifferentChangeRetries` | add 重试幂等、异类变更互斥 |
| `RepeatedRemoveIsIdempotentAndDifferentChangeRetries` | remove 重试幂等、异类变更互斥 |
| `NewLeaderRecognizesUnappliedConfigEntry` | 新 leader 识别尚未 apply 的配置日志 |
| `RemovedFollowerReceivesNoMessagesAndSmallerQuorumCommits` | 删除后停止复制并调整 quorum |
| `RemovingLeaderMakesItANonMemberAndStopsTicks` | leader 删除自己后退位 |
| `DISABLED_PromotionVisibilitySkewElectsBeforeLearnerAppliesConfig` | 提升配置可见性偏差的已知问题 |
| `LaggingFollowerInstallsSnapshotThenReceivesIncrementalLog` | snapshot 后增量追赶 |
| `LearnerCatchesUpThroughLeaderSnapshot` | learner 通过 snapshot 追赶 |
| `AllowsOnlyOneInflightSnapshotPerFollower` | 单 follower 单份在途 snapshot |
| `SendFailureClearsInflightAndAllowsRetry` | 失败清理并重试 |
| `SuccessUsesActuallySentIndexAfterLeaderCompactsAgain` | response 对应发送时 index |
| `AlreadyExistAdvancesProgressToReturnedWatermark` | 已存在 snapshot 的 progress 推进 |
| `HigherTermResponseForcesLeaderToStepDown` | 高 term snapshot response 使 leader 退位 |
| `FollowerRejectsSnapshotCoveredByCommitIndex` | follower 拒绝过期 snapshot |
| `RecoveringFollowerRetainsMatchingSuffixAndRestoresMembers` | 恢复 snapshot 且保留匹配后缀 |
| `CompactCannotMoveBeyondAppliedIndex` | compact 边界 |
| `PreviousTermResponseDoesNotChangeNewLeaderProgress` | 旧 snapshot response 隔离 |

---

## 11. ReadIndex 与 recovering 测试

源码：

```text
test/storage/raft/core/raft_protocol_read_recovery_test.cpp
```

### 11.1 新 leader 为什么不能立刻提供 ReadIndex

测试：

```text
NewLeaderRejectsReadBeforeCurrentTermEntryCommits
```

`become_uncommitted_leader()` 只投递足以让 node0 当选的投票消息，不投递后续 no-op 复制。

此时：

```text
node0 role = leader
node0 term = 1
node0 commit_index = 0
当前 term no-op 尚未 commit
```

调用 `build_append_entries_for_read()` 应返回 `NOT_YET_COMMIT`。

原因是 node0 虽然收到了选票，但还没有通过当前 term 的 committed entry 确认自己仍能联系多数派并建立安全读屏障。

### 11.2 no-op 提交后 ReadIndex 返回什么

测试：

```text
CommittedLeaderReturnsReadIndexAndBuildsQuorumProbe
```

投递一次 follower AppendEntries 往返，使 no-op 在 3 节点中达到 2 份并提交。

然后 ReadIndex 返回：

```text
read_index = 当前 commit index
read_term  = 当前 term
effects.messages = 发往其他 voter 的 AppendEntries probe
```

### 11.3 recovering 节点为什么被限制

测试：

```text
RecoveringNodeDoesNotCampaignVoteOrPropose
```

它连续 tick 20 次仍不能竞选；收到 RequestVote 不能同意；客户端 proposal 返回 `IS_RECOVERING`。

这是为了避免状态残缺节点参与多数派决定。

### 11.4 recovering 如何结束

两条路径：

```text
AppendEntriesFinishesRecoveryAfterLogCatchesUp
SnapshotFinishesRecoveryAndRestoresState
```

第一条通过完整日志追赶，第二条通过 snapshot 跨过恢复目标。

当前还有一个 disabled 场景：

```text
DISABLED_HeartbeatWithoutRecoveryDataKeepsNodeRecovering
```

它指出当前实现可能仅凭携带较高 `leader_commit` 的空心跳提前结束 recovering，而没有真正获得相应日志或 snapshot。

---

## 12. Seed scheduler：随机的到底是什么

源码：

```text
test/storage/raft/core/raft_test_scheduler.h
test/storage/raft/core/raft_test_scheduler.cpp
```

### 12.1 它不是随机生成整个 Raft 世界

当前 scheduler 不会随机：

```text
节点数量
客户端操作类型
什么时候创建 snapshot
什么时候增删成员
什么时候让任意节点崩溃重启
```

它随机的是 `pending_` 中消息的处理方式：

```text
下一条选哪条消息
是否丢弃
是否复制一份
否则正常投递
```

所以准确名称是：

```text
seeded message scheduler
```

不是完整的 model checker。

### 12.2 `RaftTestSchedulePolicy`

```cpp
struct RaftTestSchedulePolicy {
    uint32_t drop_one_in;
    uint32_t duplicate_one_in;
};
```

例如：

```cpp
RaftTestSchedulePolicy lossy{5, 4};
```

含义：

```text
大约 1/5 概率丢弃选中消息
如果没有丢弃，大约 1/4 概率额外复制一份
无论是否复制，原消息都会正常投递
```

`{0, 3}` 表示不丢包，但大约 1/3 概率复制，配合随机选择 pending 位置会产生乱序。

### 12.3 同 seed 为什么能复现

调度器内部维护：

```cpp
uint64_t seed_;
uint64_t state_;
```

`next_random()` 使用确定性的整数运算生成下一随机数。

同样的：

```text
初始 seed
调用 choose/one_in 的次数
每次 upper_bound
pending 队列内容
```

会产生同样的选择序列。

因此：

```text
同样 seed + 同样代码路径 = 同样调度
```

如果代码改动导致多生成了一条消息，后续 `pending_.size()` 和随机调用次数可能变化，trace 就可能与旧版本不同。seed 保证当前版本内可复现，不代表跨任意代码版本保持完全相同轨迹。

### 12.4 `step()` 逐行翻译

伪代码：

```text
如果 pending 为空：返回 false

随机选择一条 pending 消息 selected

如果命中 drop：
    删除 selected
    dropped++
否则：
    如果命中 duplicate：
        复制 selected，得到新 id
        duplicated++
    deliver(selected)
    delivered++

steps++
返回 true
```

注意复制分支：

```text
复制消息留在 pending 中
原消息本轮仍然投递
```

因此重复 response 可能在未来某一步才到达。

### 12.5 `drain()`

`drain()` 一直调用 `step()`，直到 pending 为空。

`max_steps` 防止：

```text
复制消息不断产生
消息处理又不断产生新消息
测试陷入无限循环
```

超过限制时，异常中会包含 seed、scheduler trace 和 cluster trace。

### 12.6 `stats_`

记录：

```text
steps
delivered
dropped
duplicated
```

它主要用于失败时理解当前 seed 实际注入了多少故障，不是协议正确性判断本身。

---

## 13. SafetyMonitor：每一步都要检查什么

源码位置：

```text
test/storage/raft/core/raft_seeded_schedule_test.cpp
class RaftSafetyMonitor
```

### 13.1 为什么不能只检查最终收敛

假设测试最后三个节点日志一致，并不代表中间没有发生过：

```text
同一个 term 出现两个 leader
已经 committed 的日志短暂被覆盖
commit index 先前进再回退
```

这些安全性问题即使最后“看起来恢复了”，也已经违反 Raft。

所以 scheduler 每投递一步消息，就调用：

```cpp
safety.check(cluster)
```

### 13.2 第一组：单节点当前状态关系

```text
snapshot_index <= last_applied <= commit_index <= last_log_index
```

如果不满足，例如 `last_applied > commit_index`，说明状态机执行了尚未 committed 的日志。

### 13.3 第二组：单节点历史单调性

monitor 为每个 `TestNodeId` 保存上一次：

```text
term
snapshot_index
last_applied
commit_index
```

下一次检查时，这四个值不能变小。

为什么不保存 `last_log_index` 单调性？

因为 follower 的未提交冲突后缀可以合法被截断，`last_log_index` 有可能减小。

### 13.4 第三组：已 committed entry 的历史承诺

```cpp
std::map<LogIndex, LogEntry> committed_by_index_;
```

某个 index 第一次被观察到 committed 时，保存完整 `LogEntry`。

以后任何节点在相同 committed index 上出现不同内容，立即失败。

这防止出现：

```text
昨天 index5 committed 为 A
今天所有节点都把 index5 改成 B
当前节点之间虽然一致，但历史承诺被破坏
```

### 13.5 第四组：Election Safety

```cpp
std::map<Term, TestNodeId> leader_by_term_;
```

只要观察到一个 leader，就记录：

```text
term -> node
```

以后同 term 如果出现另一个 node 作为 leader，立即失败。

它保存的是历史，不只是当前同时存在的 leader。

### 13.6 第五组：节点间共同 committed 日志一致

对任意两个节点：

```text
共同 commit 范围 = min(left.commit, right.commit)
可见起点 = max(left.snapshot, right.snapshot) + 1
```

在双方都可见、双方都认为 committed 的范围内，每个 LogEntry 必须完全相同。

### 13.7 monitor 自己也有测试

```text
RejectsCommitAndAppliedRegressionAcrossChecks
RejectsReplacementOfPreviouslyCommittedEntry
```

第一条先给 monitor 看一个进度较高的单节点集群，再给它看同一节点进度更低的集群，确认它报告 `regressed state`。

第二条先提交 `history-key=first`，再给同一个 monitor 看相同 index 上提交 `history-key=replacement` 的集群，确认它报告 committed entry changed。

测试 oracle 也要测试，否则 oracle 自己写错时，seed 测试会产生虚假的安全感。

### 13.8 SafetyMonitor 仍然不是什么

它不是形式化证明，也没有检查所有可能性质。例如当前没有完整做到：

```text
对任意并发客户端历史做线性一致性判定
验证 snapshot 内部 KV 的历史摘要永不改变
验证磁盘恢复后的历史单调性
穷举所有消息顺序
```

---

## 14. 完整 seed 场景逐步拆解

入口：

```cpp
run_partition_scenario(uint64_t seed)
```

### 14.1 初始化

```cpp
RaftTestCluster cluster = RaftTestCluster::voters(3, 760);
RaftTestScheduler scheduler(seed);
RaftSafetyMonitor safety;
```

两个 policy：

```cpp
reordered{0, 3}; // 不丢，可能复制，随机选择带来乱序
lossy{5, 4};     // 可能丢，可能复制，也会乱序
```

### 14.2 第一任 leader

```text
node0 campaign
用 reordered 调度完整选举消息
断言 node0 成为 leader
```

为什么选举开始时不用 lossy？

使用不丢包策略能更快建立初始 leader，同时仍然覆盖重复和乱序。后面的写入与切主才使用 lossy。

### 14.3 第一批 16 个写入

每次：

```text
node0 propose before-i=value-i
随机只推进 0..3 步消息
每一步执行 SafetyMonitor
```

这会让多个 proposal 的 AppendEntries 和 response 同时堆在 pending 中，从而自然产生跨请求乱序，而不是每条写入都完全处理完再写下一条。

写完后：

```text
drain pending，但允许丢包
不断 tick leader，直到 leader 自己 commit 到 last_log
heal 网络并可靠收敛所有节点
```

### 14.4 隔离旧 leader

```cpp
cluster.isolate(0);
```

从现在起 node0 与 node1/node2 双向消息都被丢弃。

### 14.5 node1 竞选

```cpp
elect_with_loss(... node1 ...)
```

因为投票消息也可能被丢，函数最多尝试 32 个 term，直到 node1 获得 node2 的票成为 leader。

每次尝试失败后，node1 下次 `campaign()` 会进入更高 term。

### 14.6 第二批 16 个写入

node1 写：

```text
after-0 ... after-15
```

流程与第一批相同，仍然注入丢包、复制和乱序。

### 14.7 恢复并收敛

`converge_cluster()` 内部先：

```cpp
cluster.heal_all();
```

然后反复：

```text
tick 新 leader
随机乱序但不丢包地 drain
apply_all
SafetyMonitor
检查所有节点日志、commit、KV 是否一致
```

最终还断言旧 node0 已经退为 follower。

### 14.8 固定 seed 集合

当前 CI 默认跑 20 个固定 seed。

这不代表只允许这 20 个。发现失败 seed 后可以：

```bash
env ADVISKV_RAFT_SCHEDULE_SEED=12345 \
  ./build/release/bin/test/raft_seeded_test \
  --gtest_filter=RaftSeededSchedulerTest.ReplaySeedFromEnvironment
```

### 14.9 为什么要测试“相同 seed 相同 trace”

```text
SameSeedReplaysIdenticalMessageSchedule
```

它创建两个完全相同的集群，使用同一个 seed 独立调度，然后比较：

```text
scheduler trace
cluster trace
每个节点 role
每个节点 term
每个节点日志
每个节点 commit
```

如果这个测试不通过，失败 seed 就没有可靠回放价值。

### 14.10 Seed 层一句话总结

> 固定测试负责精确验证一个已知反例；seed 测试负责在一个固定业务故事中探索更多消息交错，并用历史 safety oracle 每步检查。

---

## 15. Crash point：怎样在代码执行到一半时杀死进程

源码：

```text
src/common/crash_injection.h
```

核心函数：

```cpp
testhook::crash_point("某个名字");
```

### 15.1 正常构建中它什么都不做

只有定义了：

```text
ADVISKV_ENABLE_CRASH_TEST
```

crash point 才会检查环境变量。

`src/storage/CMakeLists.txt` 只给：

```text
adviskv_storage_test_lib
```

定义这个宏，普通 `adviskv_storage_lib` 不启用。

所以生产二进制不会因为用户设置了同名环境变量而 `_exit(137)`。

### 15.2 如何选择一个 crash point

测试子进程设置：

```cpp
setenv(
    "ADVISKV_ENABLE_CRASH_POINT",
    "replica.apply.after_state_machine_before_progress",
    1
);
```

生产路径执行到：

```cpp
testhook::crash_point(
    "replica.apply.after_state_machine_before_progress"
);
```

字符串完全相等，就执行：

```cpp
::_exit(137);
```

### 15.3 为什么用 `_exit`，不是正常 return

正常 return 会执行：

```text
析构函数
close
缓冲区清理
后续错误处理
```

这不像突然崩溃。

`_exit` 立即终止进程，不执行 C++ 栈展开，适合模拟进程在某个精确窗口突然死亡。

### 15.4 `ASSERT_EXIT` 在做什么

测试结构：

```cpp
ASSERT_EXIT(
    {
        // 子进程代码
        // 设置 crash point
        // 执行待测操作
    },
    ::testing::ExitedWithCode(137),
    ""
);
```

可以理解成：

```text
父测试进程
  ├─ 启动一个子进程执行大括号中的代码
  ├─ 子进程在 crash point 以 137 退出
  ├─ 父进程确认退出码确实是 137
  └─ 父进程继续打开子进程留下的文件并验证恢复
```

如果 crash point 没有触发，子进程会走到代码里预留的其他退出码，测试失败。这能防止“测试以为自己崩在关键点，实际上根本没走到那里”。

### 15.5 这种 crash 测试模拟了什么

它模拟的是：

```text
进程崩溃
```

它没有完整模拟：

```text
整机断电
磁盘控制器丢失已确认写入
文件系统损坏
扇区撕裂的所有真实模式
```

特别是“write 完成但没有 fsync”的数据，在另一个进程中通常仍可从操作系统 page cache 读到；真正断电后则未必存在。

因此面试时应说“process crash recovery”，不要扩大成“证明断电不丢数据”。

---

## 16. PersistEngine 崩溃测试

源码：

```text
test/storage/persist/persist_crash_recovery_test.cpp
```

它只关注文件格式与原子发布，不启动完整 Replica。

### 16.1 测试夹具做什么

`PersistCrashRecoveryTest::SetUp()` 为每个测试创建独立目录。

`TearDown()` 删除目录。

`make_engine()` 使用固定 `ReplicaID` 和这个目录创建 `PersistEngine`。

每个 crash 测试的基本模式：

```text
父进程先写入稳定旧状态
子进程尝试写新状态并在指定点退出
父进程重新创建 PersistEngine
检查恢复看到的是哪种状态
```

### 16.2 WAL framed record 为什么需要多个 crash 点

一条 WAL record 大致包含：

```text
length
checksum / CRC
payload
```

如果进程分别死在：

```text
只写完 length
只写完 length + CRC
payload 只写了一部分
payload 完整写完
fsync 完成
```

恢复逻辑应该只解析完整 record，不能把半条 payload 当成合法日志。

测试：

```text
RecoveryKeepsOnlyCompleteFramedRecords
```

参数表：

| case | crash point | 预期 |
|---|---|---|
| `AfterLength` | 长度写完 | 只恢复旧完整日志，标记需要恢复尾部 |
| `AfterCrc` | CRC 写完 | 同上 |
| `DuringPayload` | payload 部分写 | 同上 |
| `AfterPayload` | 完整 record 已写 | 能读到新 record |
| `AfterFsync` | record 已 fsync | 能读到新 record |

这个测试验证“解析不会接受半条 record”，不是完整的磁盘断电语义。

### 16.3 hard state 为什么要原子替换

`RaftMeta` 包含：

```text
current_term
voted_for
```

如果更新过程中得到：

```text
新 term + 旧 voted_for
```

可能破坏每 term 一票等选举安全性。

因此保存方式是：

```text
写 path.tmp
fsync(tmp)
rename(tmp, path)
fsync(parent directory)
```

测试：

```text
ReopenObservesOldOrNewHardState
```

期望恢复只能看到：

```text
完整旧状态
或
完整新状态
```

不能看到字段混搭或无法解析的中间状态。

### 16.4 WAL rewrite 为什么也要 old-or-new

日志 compact 或冲突覆盖时，可能需要重写整个 WAL。

测试：

```text
RecoveryObservesEntireOldOrNewWal
```

旧 WAL：

```text
term1/index3
term1/index4
```

新 WAL：

```text
term2/index3
term2/index4
```

崩溃恢复不能得到：

```text
旧 index3 + 新 index4
```

只能是整份旧或整份新。

### 16.5 snapshot 文件为什么也要 old-or-new

snapshot 同时包含：

```text
apply index / term
members
KV 数量
所有 KV
```

如果 metadata 是新版本但 KV 是旧版本，或者相反，恢复状态就会自相矛盾。

测试：

```text
ReopenObservesEntireOldOrNewSnapshot
```

它同时断言：

```text
snapshot apply_index
完整 KV 列表
```

而不是只看文件能不能打开。

### 16.6 PersistEngine 层完整目录

| 测试 | 验证目标 |
|---|---|
| `RecoveryKeepsOnlyCompleteFramedRecords` | WAL 半写 record 不被接受 |
| `ReopenObservesOldOrNewHardState` | term/vote 原子发布 |
| `RecoveryObservesEntireOldOrNewWal` | WAL rewrite 原子替换 |
| `ReopenObservesEntireOldOrNewSnapshot` | snapshot metadata 与 KV 同版本 |

---

## 17. Replica 崩溃恢复测试

源码：

```text
test/storage/replica/replica_crash_recovery_test.cpp
```

这一层比 PersistEngine 更高。它不只检查文件内容，还会：

```text
创建 ReplicaManager
启动 tick
等待单节点成为 leader
执行真实 put 或 snapshot install
崩溃
用同一目录 recover
重新选主
通过 Replica::get 读取最终值
```

### 17.1 为什么使用单节点 group

这些测试重点是本地崩溃窗口，不是网络 quorum。

单节点 group 可以：

```text
快速、确定性地选主
proposal 立即满足 quorum
避免真实 RPC 干扰 crash 时序
```

网络复制正确性已经由第一层测试。

### 17.2 `assert_recovered_value()` 做什么

```text
用旧目录创建新的 ReplicaManager
manager.recover()
找到恢复出来的 replica
启动 tick
等待重新成为 leader
get(key)
比较 value
```

它验证的是业务可观察结果，不只是内部 index。

### 17.3 必须会：commit 后、apply 前崩溃

测试：

```text
CommittedWalReplaysAfterCrashBeforeApply
```

crash point：

```text
replica.raft_step.after_persist_before_send
```

路径：

```text
RaftCore 接受 put
产生日志与 effects
ReplicaLoop 持久化 effects
日志已经在 WAL
尚未继续发送/完成后续流程
进程退出
```

重启后恢复应重放 committed WAL，最终 `commit-crash=durable` 可读。

这个测试验证：持久化日志不会因为当时尚未完成 apply 而永久丢失。

### 17.4 必须会：状态机执行后、progress 推进前崩溃

测试：

```text
ApplyReplaysAfterCrashBeforeProgressAdvance
```

`ReplicaApplier::apply_kv_log_entry` 顺序：

```text
state_machine.apply(entry)
crash point
raft_core.advance_last_applied(entry.index)
```

崩溃发生在中间：

```text
KV 已经写入内存状态机
last_applied 尚未推进
```

进程重启后内存状态机消失，恢复逻辑根据持久化进度重新 apply 该日志。

PUT 对相同 key/value 重放是幂等的，最终值仍然正确。

这里测试的是最终状态恢复，不等于证明所有未来可能加入的状态机操作都天然幂等。

### 17.5 安装远端 snapshot 的两个窗口

测试：

```text
RebuildRestoresPublishedSnapshot
```

两个参数化 crash point：

```text
after_persist_before_restore
after_restore_before_raft
```

正常顺序：

```text
接收完整 snapshot 文件
加载 snapshot metadata
恢复 KV state machine
把 snapshot index/term/members 发布给 RaftCore
```

第一个窗口：

```text
文件已经完整持久化
内存状态机还没恢复
```

第二个窗口：

```text
文件已持久化
内存状态机已恢复
RaftCore 还没发布 snapshot progress
```

两种情况下进程重启，都应该从完整持久化 snapshot 重建，而不是依赖已经丢失的内存状态。

### 17.6 本地生成 snapshot 后、Raft compact 前崩溃

测试：

```text
LocalSnapshotRecoversAfterCrashBeforeRaftPublish
```

它先写接近 snapshot 阈值的 1000 条日志。

正常顺序：

```text
状态机达到 snapshot 阈值
PersistEngine 写 snapshot
crash point
RaftCore 发布 snapshot / compact 日志
```

崩溃时 snapshot 文件已经存在，但 RaftCore 尚未在内存中更新 snapshot index。

重启后必须识别磁盘 snapshot：

```text
snapshot_index == 1000
最后一条 KV 仍可读取
```

### 17.7 Replica 层完整目录

| 测试 | 验证目标 |
|---|---|
| `CommittedWalReplaysAfterCrashBeforeApply` | 持久化后、apply 前重启不丢写 |
| `ApplyReplaysAfterCrashBeforeProgressAdvance` | apply 与 progress 中间崩溃可重放 |
| `RebuildRestoresPublishedSnapshot` | 安装 snapshot 两个内存窗口的恢复 |
| `LocalSnapshotRecoversAfterCrashBeforeRaftPublish` | 本地 snapshot 文件领先 RaftCore 时恢复 |

---

## 18. 拿到任何测试都能用的六步阅读法

不要从每一行 C++ 语法开始。先在纸上写六个标题。

### 第一步：初始状态

回答：

```text
几个节点？
谁是 voter/learner？
初始 term？
每个节点有哪些日志？
commit/applied/snapshot 到哪里？
谁是 leader？
有没有分区或 recovering？
```

### 第二步：刺激

测试向系统做了什么？

```text
tick？
proposal？
注入 RequestVote？
投递 AppendEntries？
隔离节点？
触发 crash point？
```

### 第三步：消息顺序

逐条列出真正 deliver 的消息。

特别注意：

```text
enqueue 不等于 deliver
request deliver 后通常只生成 response
response 还要再次 deliver，leader 才能看到
drop_all 可能刻意删除正常流程消息
```

### 第四步：状态变化

每投递一步，写下关键变化：

```text
role
term
log
next/match index
commit
applied
membership
KV
```

### 第五步：断言

区分断言类型：

```text
过程断言：少于 quorum 时 commit 不能前进
结果断言：最终日志和 KV 收敛
拒绝断言：stale/invalid 消息不能改变状态
历史断言：已经发生的 leader/commit 不能被推翻
```

### 第六步：它证明了什么，没证明什么

例如一个内存 snapshot 测试：

```text
证明：RaftCore 的 snapshot index、members、progress 处理符合该场景预期
不证明：磁盘 snapshot 文件一定抗断电
```

建议使用下面的模板做笔记：

```text
测试名：

1. 初始状态：
2. 输入/刺激：
3. 消息顺序：
4. 状态变化：
5. 核心断言：
6. 对应规则：
7. 没覆盖什么：
8. 如果删除某个断言，会漏掉什么 bug：
```

---

## 19. 当前框架明确没有覆盖什么

知道边界和知道能力同样重要。

### 19.1 协议功能边界

当前没有完整实现或验证：

```text
PreVote
CheckQuorum
leader transfer
joint consensus
完整自动 rebalance
拜占庭节点
```

### 19.2 状态空间边界

固定协议测试只覆盖人工列出的场景。

seed 测试目前只有一个主要业务故事：

```text
node0 leader 写入
隔离 node0
node1 当 leader 写入
恢复并收敛
```

它没有随机生成成员变更、snapshot、任意节点 crash/restart 的组合。

### 19.3 一致性验证边界

ReadIndex 测试验证了：

```text
新 leader 当前 term entry 未 commit 时拒绝
commit 后生成 quorum probe
```

但没有使用线性一致性历史检查器验证大量并发客户端操作的所有历史。

### 19.4 持久化边界

crash point 主要模拟进程 `_exit`，不是完整 power-loss 模型。

没有覆盖所有：

```text
真实磁盘坏块
目录项丢失
fsync 实现差异
多文件事务原子性
磁盘空间耗尽
权限错误
I/O 长时间卡住
```

### 19.5 已知 disabled 场景

当前 6 个 disabled test 是明确的未完成项，不能被“64 个协议测试通过”掩盖。

---

## 20. 运行、筛选和调试命令

这些命令均不需要网络，前提是当前 `build/release` 已经构建完成。

### 20.1 运行三层测试

```bash
./build/release/bin/test/raft_protocol_test --gtest_color=no
./build/release/bin/test/raft_seeded_test --gtest_color=no
./build/release/bin/test/persist_crash_recovery_test --gtest_color=no
./build/release/bin/test/replica_crash_recovery_test --gtest_color=no
```

### 20.2 列出测试，不执行

```bash
./build/release/bin/test/raft_protocol_test --gtest_list_tests
./build/release/bin/test/raft_seeded_test --gtest_list_tests
```

### 20.3 只运行一个测试

```bash
./build/release/bin/test/raft_protocol_test \
  --gtest_filter=RaftReplicationProtocolTest.NewLeaderOverwritesUncommittedOldLeaderEntry
```

### 20.4 运行某一组

```bash
./build/release/bin/test/raft_protocol_test \
  --gtest_filter='RaftElectionProtocolTest.*'
```

### 20.5 运行 disabled 测试观察当前失败

```bash
./build/release/bin/test/raft_protocol_test \
  --gtest_also_run_disabled_tests \
  --gtest_filter='*LearnerVoteResponseDoesNotCountTowardQuorum'
```

注意：这是为了理解已知缺陷，预期可能失败。

### 20.6 回放任意 seed

```bash
env ADVISKV_RAFT_SCHEDULE_SEED=12345 \
  ./build/release/bin/test/raft_seeded_test \
  --gtest_filter=RaftSeededSchedulerTest.ReplaySeedFromEnvironment
```

### 20.7 重复运行一个测试

```bash
./build/release/bin/test/raft_seeded_test \
  --gtest_filter='FixedSeeds/RaftSeededNetworkTest.*' \
  --gtest_repeat=20 \
  --gtest_break_on_failure
```

### 20.8 查看源码位置

```bash
rg -n 'NewLeaderOverwritesUncommittedOldLeaderEntry' test/storage/raft/core
rg -n 'RaftTestCluster::deliver' test/storage/raft/core
rg -n 'replica.apply.after_state_machine_before_progress' src test
```

---

## 21. 离线练习题

先不要看第 22 章答案。最好真的在纸上写。

### 练习 1，必须会：区分四个 index

某节点：

```text
snapshot_index = 20
last_applied   = 25
commit_index   = 28
last_log_index = 31
```

回答：

1. 哪些日志已经包含在 snapshot？
2. 哪些日志已执行但不在 snapshot？
3. 哪些日志 committed 但尚未 apply？
4. 哪些日志尚未 committed？

### 练习 2，必须会：走一遍 VoteRequest

三节点中 node0 调用 `campaign(0)`。请写出从 `RaftCore::tick()` 到 node1 的 VoteResponse 进入 pending 的完整函数链。

### 练习 3：enqueue 与 deliver

下面代码执行后，node1 是否已经处理 RequestVote？为什么？

```cpp
cluster.enqueue(RaftTestEnvelope{
    0, 0, 1, VoteRequestPayload{request}
});
```

### 练习 4，必须会：quorum

5 voter + 2 learner 的 group，leader 自己有日志，收到 1 个 voter 和 2 个 learner 的成功响应。能否 commit？还差几个 voter response？

### 练习 5：旧 leader 日志

旧 leader 被隔离后写入 index5=A，没有 quorum。新 leader 在 index5 写入 B 并提交。网络恢复后 index5 应该是什么？为什么不违反“日志不能被覆盖”？

### 练习 6，必须会：Figure 8

新 leader 在 term3 发现 term2/index8 的日志已经位于多数派。它能否只根据这个事实直接把 commit index 推到8？应该等什么？

### 练习 7：blocked 消息

调用 `cluster.isolate(0)` 后，原来 pending 中 node0 发给 node1 的消息再被 `deliver()`，框架如何处理？网络恢复后它会自动重新出现吗？

### 练习 8：重复 response

为什么 `duplicate(response_id)` 后还要分别 deliver 原 response 和副本，才能真正测试幂等？

### 练习 9，必须会：seed 随机了什么

列出当前 scheduler 会随机的三件事，以及不会随机的四件事。

### 练习 10：相同 seed 仍可能跨版本不同

为什么同一个 seed 在代码改动后，可能产生不同 trace？

### 练习 11，必须会：历史 oracle

为什么只比较“当前所有节点 committed 日志相同”仍可能漏掉已经 committed 的 A 被所有节点依次替换成 B？

### 练习 12：为什么不检查 last_log 单调

SafetyMonitor 为什么检查 commit/applied 单调，却不要求 last_log_index 永远增加？

### 练习 13：协议 snapshot 与磁盘 snapshot

`RaftTestSnapshot` 与 `PersistEngine::write_snapshot` 的测试目标有什么区别？

### 练习 14，必须会：apply crash

状态机已经执行 index10，但 `last_applied` 仍是9时进程崩溃。重启后为什么可能重放 index10？当前 PUT 状态机为什么通常能承受？

### 练习 15：原子替换

为什么 hard state 和 snapshot 应使用“写临时文件、fsync、rename、fsync 目录”，而不是直接覆盖原文件？

### 练习 16：测试拆解

任选一个协议测试，用第 18 章模板写完整八项笔记。

### 练习 17：找薄弱断言

任选一个测试，假设删掉最后一个 `EXPECT_EQ`，说明哪种 bug 可能从此不再被发现。

### 练习 18：面试口述

不用看教程，用 90 秒解释这套测试为什么分成协议、seed、持久化三层。

---

## 22. 练习答案

### 答案 1

```text
1..20  已进入 snapshot
21..25 已执行，但尚未进入 snapshot
26..28 已 committed，尚未 apply
29..31 本地存在，但尚未 committed
```

### 答案 2

```text
cluster.campaign(0)
  -> cluster.tick(0)
    -> node0.core.tick(effects)
      -> become_candidate(effects)
        -> effects.messages 加入 RequestVote
    -> cluster.accept_effects(0, effects)
      -> request_envelope(0, message)
      -> enqueue(VoteRequest Envelope)

测试 deliver request
  -> cluster.deliver(id)
    -> take(id)
    -> deliver(envelope)
    -> deliver_unblocked(envelope)
      -> node1.core.handle_request_vote(...)
      -> accept_effects(node1, effects)
      -> enqueue(VoteResponse Envelope)
```

注意 VoteResponse 不是生产 `RaftEffects.messages` 自动生成的生产消息类型，而是 harness 在处理 request 后根据 `RequestVoteResult` 包装出来的测试 response。

### 答案 3

没有。`enqueue()` 只把消息放进 `pending_`。必须调用 `deliver(id)` 或 `deliver_all()`，node1 才会执行 `handle_request_vote()`。

### 答案 4

5 voter 的 quorum 是3。leader 自己 + 1 voter = 2 个 voter 副本，2 learner 不计入 quorum，所以不能 commit。还差1个 voter response。

### 答案 5

最终应为 B。A 从未 committed，因此可以被新 leader 的日志覆盖。“不能覆盖”约束的是 committed 日志，不是任意写入本地日志文件的未提交后缀。

### 答案 6

不能。leader 不能只按多数派数量直接提交旧 term 的日志。它应等待当前 term3 的一条日志，例如当选时 no-op，在多数派复制成功；提交当前 term 日志时，可以连带提交之前的 term2/index8。

### 答案 7

`deliver()` 检查链路被 blocked 后，记录 `blocked-drop` 并丢弃 Envelope。`heal_all()` 只恢复链路，不会复活已丢失消息。以后需要 leader tick 或其他动作重新生成请求。

### 答案 8

`duplicate()` 只是多创建一个 pending Envelope。只有两个 response 都真正 deliver 到 leader，才能验证 `record_vote_granted_from` 或 follower progress 更新逻辑不会重复计数。

### 答案 9

会随机：

```text
从 pending 选择哪条消息
是否丢弃
是否复制
```

不会随机：

```text
节点数量
客户端写入序列
主动隔离哪个 leader
是否执行成员变更/snapshot/crash restart
```

后三项中任写四项即可。

### 答案 10

随机序列虽然相同，但 `choose(upper_bound)` 的 upper bound 来自当前 pending 大小。如果代码改动多产生或少产生消息，pending 内容、随机函数调用次数和上界都会变化，后续调度自然不同。

### 答案 11

如果 monitor 没有历史：

```text
先让各节点 commit 从5错误回退到4
依次把 index5 的 A 改成 B
再把 commit 推回5
```

最终节点之间仍一致为 B。保存 `committed_by_index_[5]=A` 后，B 第一次出现就会被抓住；保存 commit 历史还能更早抓住 5->4 的回退。

### 答案 12

commit、applied、snapshot 都代表已经形成的不可逆进度。last_log 包含未提交后缀，新 leader 发现冲突时可以截断该后缀，因此 last_log_index 可能合法减小。

### 答案 13

`RaftTestSnapshot` 是内存协议模型，关注 index/term/members/progress/KV 恢复后的 Raft 行为。`PersistEngine::write_snapshot` 使用真实文件，关注 snapshot 文件是否原子发布、metadata 与 KV 是否属于同一完整版本。

### 答案 14

持久化恢复记录仍认为只 apply 到9，所以会把 committed index10 再交给状态机。当前 PUT 对相同 key 写相同 value，重复执行结果相同，因此最终状态正确。若未来状态机操作具有非幂等外部副作用，就需要额外的 exactly-once 或去重设计，不能仅靠这个测试推断安全。

### 答案 15

直接覆盖时崩溃可能破坏唯一旧文件，留下半份新内容。临时文件方案保留完整旧文件，只有新文件完整写入并 fsync 后才通过 rename 切换名字；目录 fsync 用于提高目录项更新的持久性。恢复因此更容易得到完整旧版本或完整新版本。

### 答案 16

没有唯一标准答案，但必须明确区分 enqueue 与 deliver，并写出测试没有覆盖什么。只复述测试注释但说不出消息顺序，不算完成。

### 答案 17

没有唯一答案。合格答案需要指出一个具体 bug。例如删除 `EXPECT_EQ(commit_index, 0)`，测试可能只验证最终 commit 到3，却无法发现 leader 曾错误地直接提交旧 term 日志。

### 答案 18

可参考下面的口述：

> 第一层直接驱动 RaftCore，用可控消息队列精确构造选举、复制、冲突、成员变更和 snapshot 场景，优点是快、确定、失败容易定位。第二层复用同一个消息模型，用 seed 随机改变消息投递顺序并注入丢包、重复和分区，每一步用历史 safety monitor 检查 term、commit、leader 和 committed entry。第三层不用内存模型，改用真实 PersistEngine、ReplicaManager 和文件，在 WAL、apply、snapshot 的关键窗口杀死子进程并重建。三层分别解决协议分支覆盖、时序组合覆盖和真实持久化恢复，任何一层通过都不能代替另外两层。

---

## 23. 面试时怎样诚实描述这套测试

可以这样说：

> 项目最初的测试是跟着功能逐步补的，后来我担心人工枚举不完整，所以参考 Raft 论文和成熟实现的公开测试场景，对 AdvisKV 已实现的功能重新整理了确定性协议测试。测试直接驱动 RaftCore，通过消息队列精确控制 RequestVote、AppendEntries 和 snapshot 的投递、丢失、重复与分区。
>
> 固定场景之外，我增加了 seeded message scheduler，在一个包含写入、旧 leader 隔离、重新选举和恢复收敛的故事里随机调度消息，并在每一步检查 term、commit、applied、同 term 单 leader和 committed entry 历史不变。
>
> 协议模型不验证磁盘，所以我另外使用真实文件和子进程 crash point 测 WAL record、hard state、snapshot 原子发布，以及 commit-before-apply、apply-before-progress 等 Replica 恢复窗口。
>
> 这些测试提高了当前 crash-stop 和消息故障模型下的信心，但不是形式化证明。目前仍有 disabled 的已知协议问题，也没有覆盖完整并发历史线性一致性和真实断电模型。

不要说：

```text
“我复刻了 etcd 所有测试，所以 Raft 肯定没问题。”
“随机 seed 穷举了所有消息顺序。”
“crash test 证明断电绝不丢数据。”
“64 个协议测试全部通过。”  // 还有 6 个 disabled
```

---

## 24. 最终知识地图

```text
                         +------------------+
                         |    RaftCore      |
                         | 状态 + 纯协议逻辑 |
                         +--------+---------+
                                  |
                             RaftEffects
                                  |
              +-------------------+-------------------+
              |                                       |
              v                                       v
   +----------------------+                 +----------------------+
   | RaftTestCluster      |                 | ReplicaLoop          |
   | 内存消息模型          |                 | 持久化 + 真实 RPC     |
   +----------+-----------+                 +----------+-----------+
              |                                        |
       pending Envelope                           PersistEngine
              |                                        |
    +---------+----------+                       crash points
    |                    |                              |
    v                    v                              v
固定协议测试       RaftTestScheduler             子进程退出并重建
精确反例           随机顺序/丢失/重复              文件与业务值验证
    |                    |
明确断言           RaftSafetyMonitor
                         |
                 当前不变量 + 历史承诺
```

如果你只能记住五句话，就记住：

1. `RaftCore` 产生 `RaftEffects`，自己不直接发测试网络消息。
2. `pending_` 就是可控制的模拟网络，enqueue 不等于 deliver。
3. 固定协议测试精确构造反例，seed 测试探索更多消息交错。
4. SafetyMonitor 必须保存历史，因为最终一致不能证明中间从未破坏安全性。
5. 协议模型不能代替真实持久化，所以 crash 测试单独使用文件和 Replica 重建。

---

## 25. 术语速查

| 术语 | 大白话解释 |
|---|---|
| term | 第几届 leader 任期 |
| role | follower/candidate/leader |
| voter | 可以竞选、投票并计入 quorum 的成员 |
| learner | 只追数据、暂不计入 quorum 的成员 |
| quorum | voter 多数派 |
| LogEntry | 一条需要复制并按顺序执行的操作 |
| no-op | 不改 KV，但用于当前 term 提交和领导权确认的空操作 |
| last_log_index | 本地最后一条日志位置 |
| commit_index | 已经确认不能再覆盖的位置 |
| last_applied | 已经执行到状态机的位置 |
| snapshot_index | 已被 snapshot 覆盖的日志位置 |
| next_index | leader 下次准备从哪里给某 follower 发日志 |
| match_index | leader 确认某 follower 已复制到哪里 |
| hard state | 必须持久化的 term 与 voted_for |
| RaftEffects | RaftCore 希望外层执行的持久化和消息副作用 |
| Envelope | 测试模拟网络中的 request/response 信封 |
| pending | 尚未被测试决定如何处理的消息队列 |
| harness | 直接驱动 RaftCore 的测试环境 |
| scheduler | 用 seed 决定 pending 消息处理顺序的调度器 |
| oracle | 判断测试结果是否违反预期的检查逻辑 |
| crash point | 执行到特定位置时强制退出子进程的测试钩子 |
| process crash | 进程突然退出，不等同于整机断电 |

---

## 26. 飞机落地前的自测标准

如果下面十项至少能闭卷回答八项，这次学习就有效：

1. `RaftEffects` 四个字段分别是什么？
2. RequestVote request 和 response 如何进入 `pending_`？
3. `deliver_append_round_trip` 为什么必须投递两次？
4. 3 voter + 1 learner 的写 quorum 怎么算？
5. 为什么旧 leader 的未提交日志可以被覆盖？
6. 为什么旧 term 日志不能只靠多数派数量直接提交？
7. scheduler 随机什么、不随机什么？
8. SafetyMonitor 为什么要保存历史 committed entry？
9. protocol snapshot 与 PersistEngine snapshot 测试有什么区别？
10. `_exit(137)` 测到了进程崩溃，但为什么不能直接声称抗断电？

如果某一项不会，不要重新从头读。直接回到对应章节：

| 不会的问题 | 回看章节 |
|---|---|
| 1 | 第 4 章 |
| 2、3 | 第 6、7 章 |
| 4 | 第 3、10 章 |
| 5、6 | 第 9 章 |
| 7 | 第 12、14 章 |
| 8 | 第 13 章 |
| 9、10 | 第 15 至 17 章 |
