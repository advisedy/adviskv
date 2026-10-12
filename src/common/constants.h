#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace adviskv {

////////////////////////////////////////////////////////////
// File
////////////////////////////////////////////////////////////

// 分片
// key 映射到这 K_HASH_BUCKET_COUNT 个哈希桶
inline constexpr uint32_t K_HASH_BUCKET_COUNT = 65536;

// 文件默认权限
inline constexpr uint32_t K_DEFAULT_FILE_MODE = 0644;

// 读写检查的上限
inline constexpr int32_t K_MAX_FRAMED_PAYLOAD_BYTES = 64 * 1024 * 1024;

// Meta 的 JSON 数据最大大小
inline constexpr int64_t K_MAX_META_JSON_BYTES = 256 * 1024 * 1024;

// 文件写入先攒到 K_FILE_WRITE_BUFFER_BYTES 的缓冲里
inline constexpr std::size_t K_FILE_WRITE_BUFFER_BYTES = 64 * 1024;

// 校验快照正文时，每次读的字节大小
inline constexpr std::size_t K_SNAPSHOT_VERIFY_BUFFER_BYTES = 64 * 1024;

////////////////////////////////////////////////////////////
// Raft
////////////////////////////////////////////////////////////
// 副本之间发一次 Raft RPC 的最长等待时间
inline constexpr int32_t K_DEFAULT_RAFT_RPC_TIMEOUT_MS = 1000;

// 打算创建快照的日志条数阈值
inline constexpr int32_t K_DEFAULT_RAFT_SNAPSHOT_LOG_LIMIT = 1000;

// Raft推动一次tick的时间间隔
inline constexpr int32_t K_RAFT_TICK_INTERVAL_MS = 20;

// Leader 触发一次心跳的tick数
inline constexpr int32_t K_RAFT_HEARTBEAT_INTERVAL_TICKS = 3;

// 触发一次选举的tick的随机的min
inline constexpr int32_t K_RAFT_ELECTION_TIMEOUT_MIN_TICKS = 15;

// 触发一次选举的tick的随机的max
inline constexpr int32_t K_RAFT_ELECTION_TIMEOUT_MAX_TICKS = 30;

// 一次追加日志的 RPC 最多带多少条日志
inline constexpr std::size_t K_RAFT_MAX_APPEND_ENTRIES_PER_MESSAGE = 256;

// 发送快照时，每块的最大字节大小
inline constexpr std::size_t K_RAFT_SNAPSHOT_CHUNK_BYTES = 1 * 1024 * 1024;  // 1MB

// 写操作等待结果的最长时间
inline constexpr int32_t K_REPLICA_PROPOSAL_COMMIT_TIMEOUT_MS = 1000;

// 读操作等待结果最长时间
inline constexpr int32_t K_REPLICA_READ_TIMEOUT_MS = 2000;

// 每批处理的写操作数量上限
inline constexpr std::size_t K_REPLICA_MAX_PROPOSAL_BATCH_SIZE = 64;

// 每批apply的日志条数上限
inline constexpr std::size_t K_REPLICA_INLINE_APPLY_BATCH_SIZE = 256;

// 每个副本发送 Raft RPC 的 worker 线程数
inline constexpr std::size_t K_REPLICA_RAFT_RPC_WORKER_COUNT = 8;

////////////////////////////////////////////////////////////
// Meta
////////////////////////////////////////////////////////////

// 资源池默认名称
inline constexpr const char* K_DEFAULT_RESOURCE_POOL = "default";

// Meta 默认的监听地址
inline constexpr const char* K_DEFAULT_META_LISTEN_HOST = "127.0.0.1";

// Meta 做一次 reconcile 的时间间隔
inline constexpr int32_t K_META_RECONCILE_INTERVAL_MS = 3000;

// 每批检查心跳的节点数上限
inline constexpr std::size_t K_META_HEARTBEAT_CHECK_BATCH_SIZE = 16;

// 节点没上报心跳多久后会被标成 SUSPECT
inline constexpr int64_t K_META_HEARTBEAT_SUSPECT_TIMEOUT_MS = 10 * 1000;

// 节点没上报心跳多久后会被标成 OFF LINE
inline constexpr int64_t K_META_HEARTBEAT_OFFLINE_TIMEOUT_MS = 30 * 1000;

// Meta 刚开始启动时，暂不按心跳超时把节点标成 SUSPECT 或 OFF LINE 的时间
inline constexpr int64_t K_META_HEARTBEAT_STARTUP_GRACE_MS = 30 * 1000;

////////////////////////////////////////////////////////////
// Storage
////////////////////////////////////////////////////////////

// Storage 向 Meta 上报心跳的时间间隔
inline constexpr int32_t K_DEFAULT_NODE_AGENT_HEARTBEAT_INTERVAL_MS = 3000;

// Storage 向 Meta 注册的时间间隔
inline constexpr int32_t K_DEFAULT_NODE_AGENT_REGISTER_INTERVAL_MS = 30 * 1000;

// Storage 首次向 Meta 注册失败后的重试间隔
inline constexpr int32_t K_DEFAULT_NODE_AGENT_FIRST_SYNC_RETRY_MS = 1000;

// Storage 向 Meta 发一次注册或心跳 RPC 的最长等待时间
inline constexpr int32_t K_DEFAULT_NODE_AGENT_RPC_TIMEOUT_MS = 3000;

// 是否默认开启 Storage 的测试接口
inline constexpr bool K_DEFAULT_STORAGE_TEST_API_ENABLED = false;

////////////////////////////////////////////////////////////
// SDK
////////////////////////////////////////////////////////////

// SDK 默认连接的 Meta 地址
inline constexpr const char* K_DEFAULT_SDK_META_HOST = "127.0.0.1";

// SDK 默认连接的 Meta 端口
inline constexpr int32_t K_DEFAULT_SDK_META_PORT = 50048;

// SDK 发一次 RPC 的最长等待时间
inline constexpr int32_t K_DEFAULT_SDK_RPC_TIMEOUT_MS = 1000;

// SDK 写请求的总超时
inline constexpr int32_t K_DEFAULT_SDK_WRITE_TIMEOUT_MS = 3000;

// SDK 读请求的总超时
inline constexpr int32_t K_DEFAULT_SDK_READ_TIMEOUT_MS = 3000;

// SDK 请求最多再重试多少次
inline constexpr int32_t K_DEFAULT_SDK_MAX_RETRIES = 3;

// 一个 SDK 请求里，最多主动刷新多少次路由
inline constexpr int32_t K_DEFAULT_SDK_ROUTE_MAX_RETRIES = 1;

// 第一次重试前的等待时间，后续每次翻倍
inline constexpr int32_t K_DEFAULT_SDK_RETRY_DELAY_MS = 20;

// SDK 在后台刷新分片路由的时间间隔
inline constexpr int32_t K_DEFAULT_SDK_ROUTE_SHARD_REFRESH_INTERVAL_MS = 3000;

////////////////////////////////////////////////////////////
// Log
////////////////////////////////////////////////////////////

// 是否默认把日志输出到控制台
inline constexpr bool K_DEFAULT_LOG_TO_CONSOLE = true;

// 是否默认把日志写入文件
inline constexpr bool K_DEFAULT_LOG_TO_FILE = true;

inline constexpr const char* K_DEFAULT_LOG_LEVEL = "info";

// 日志文件默认名称
inline constexpr const char* K_DEFAULT_LOG_FILE_NAME = "adviskv.log";

// 日志文件默认路径
inline constexpr const char* K_DEFAULT_LOG_FILE_PATH = "./";

// 是否默认开启 Metrics 的 HTTP 导出
inline constexpr bool K_DEFAULT_METRICS_HTTP_ENABLED = false;

// 监控 HTTP 默认的监听地址
inline constexpr const char* K_DEFAULT_METRICS_HTTP_HOST = "127.0.0.1";

// 通过 HTTP 拉取监控指标的默认路径
inline constexpr const char* K_DEFAULT_METRICS_HTTP_PATH = "/metrics";

// 监控 HTTP 等待 accept 的连接队列长度
inline constexpr int32_t K_METRICS_HTTP_LISTEN_BACKLOG = 16;

// 接收监控 HTTP 请求时使用的缓冲字节大小
inline constexpr std::size_t K_METRICS_HTTP_REQUEST_BUFFER_BYTES = 4096;

// 延迟直方图的桶上界，单位微秒
inline constexpr std::array<int64_t, 21> K_METRICS_LATENCY_BUCKETS_US = {
        10,    25,    50,     75,     100,    150,     200,
        250,   500,   750,    1000,   2000,   5000,    10000,
        20000, 50000, 100000, 250000, 500000, 1000000, std::numeric_limits<int64_t>::max(),
};

}  // namespace adviskv
