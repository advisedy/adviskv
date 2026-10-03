#pragma once

#include <cstddef>
#include <filesystem>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <unistd.h>

#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/raft/state_machine/state_machine.h"
namespace adviskv::storage {

// 负责WAL，snapshot，raft_meta的落盘
class PersistEngine {
public:
    // ===== 磁盘布局单一来源（批次 2.2）=====
    // 副本持久化目录与其中的文件名只能从这里拼装：data_dir/<table>-<shard>/
    // 下的 wal.log / raft_meta / snapshot。任何需要判断磁盘状态（如
    // Replica::init 的 has_persisted_state）或拼路径的地方都调这三个静态
    // 函数，保证与 init() 的实际布局永不分叉。
    static std::string replica_dir_name(const ReplicaID& replica_id);
    static std::filesystem::path replica_dir(const std::string& data_dir, const ReplicaID& replica_id);
    static std::filesystem::path wal_file(const std::filesystem::path& dir);
    static std::filesystem::path raft_meta_file(const std::filesystem::path& dir);
    static std::filesystem::path snapshot_file(const std::filesystem::path& dir);

    PersistEngine(const std::string& data_dir, const ReplicaID& replica_id);
    ~PersistEngine();

    Status init();
    Status close();

    Status append_wal(const LogEntry& entry);
    Status append_wal_batch(const std::vector<LogEntry>& entries);
    Status read_wal_batch(std::vector<LogEntry>& entries);
    Status rewrite_wal(const std::vector<LogEntry>& entries);

    // 和下面的truncate_wal_to_offset区分一下，这个是代码业务层这边调用的，用来截取到内存里的wal
    // 而truncate_wal_to_offset是用来截取磁盘里的wal的
    Status truncate_wal(const LogIndex& snapshot_index);

    Status save_raft_meta(const RaftMeta& meta);
    Status load_raft_meta(RaftMeta& meta) const;

    // 兼容入口：recover（Replica::recover）与测试用。实现已下沉到
    // SnapshotFileReader（打开即锁定一个文件版本），语义不变。
    Status load_snapshot_meta(SnapshotPtr& snap) const;
    Status for_each_snapshot_kv(const KvVisitor& fn) const;
    Status for_each_snapshot_request(const RequestVisitor& fn) const;

    // ===== 批次 B：快照文件版本化（方案 E §3-B 三件套）=====
    //
    // 契约（F11，不许倒置）：任何磁盘切换点都保持"快照先发布（rename 进
    // snapshot），WAL 后裁剪（SnapshotDoneEvent / RestoreInstallSnapshotCall
    // 里的 truncate_wal）"。崩溃恢复时 recover() 按 snapshot 边界过滤 WAL
    // 前缀，两个顺序反过来会在崩溃窗口里丢掉已发布快照覆盖的日志。
    //
    // 三件套：
    //   1. 远端接收用独立 session 临时文件（治 F5）：路径由会话四元组
    //      （来源 + term + index + 随机 nonce）决定，与本地写盘的
    //      snapshot.tmp / 候选文件彻底分家，任何一方都不可能截断对方。
    //   2. 发送端进块循环前 open 一次持 fd 读完（治 F6）：返回的
    //      SnapshotFileReader 持有已打开 fd，Linux inode 语义保证中途
    //      rename/删除不影响读取，天然读到同一文件版本。打开时校验文件头
    //      (index, term) 与消息参数一致（安全条件 5），不匹配返回
    //      SNAPSHOT_HEADER_MISMATCH，调用方据此取消发送并清 inflight。
    //   3. 本地 worker 只写候选文件 snapshot.<capture_index>.tmp；是否发布
    //      （rename 成 snapshot）由 loop 在 SnapshotDoneEvent 按 index 单调
    //      裁决：publish_local_snapshot 只在 index 严格更新时 rename，并删除
    //      过期候选（绝不覆盖更新版本）。
    //
    // 安全条件 6：restore 期间 metadata / KV / 请求记录必须来自同一文件版本
    // ——全部走同一个 SnapshotFileReader 的 fd 读取，禁止按逻辑路径二次
    // open（那会读到中途被替换的另一个版本）。

    // 持 fd 的快照只读句柄：全部读取走这一个 fd（安全条件 6 的载体）。
    class SnapshotFileReader {
    public:
        SnapshotFileReader() = default;
        explicit SnapshotFileReader(int fd) : fd_(fd) {}
        ~SnapshotFileReader() {
            if (fd_ != -1) {
                ::close(fd_);
            }
        }
        DISALLOW_COPY_AND_ASSIGN(SnapshotFileReader)

        bool is_open() const { return fd_ != -1; }
        int fd() const { return fd_; }

        // 读文件头（payload_len + index + term + members + kv_count）。
        // snapshot 非空时回填 metadata；只想拿 kv_count 时传 nullptr。
        Status read_header(Snapshot* snapshot, int32& kv_count) const;
        // 从当前 fd 的 kv 区逐条读 KV（restore 用，安全条件 6）。
        Status for_each_kv(int32 kv_count, const KvVisitor& fn) const;
        // 跳过 kv 区后读请求去重记录（restore 用，安全条件 6）。
        Status for_each_request(const RequestVisitor& fn) const;
        // 从 offset 读一块数据（发送端用，治 F6）。
        Status read_chunk(uint64 offset, size_t max_bytes, std::string& data, bool& eof) const;

    private:
        int fd_{-1};
    };

    // 打开当前已发布快照（snapshot 路径）的只读句柄并校验 header。期望的
    // index/term 来自发送消息参数：不匹配返回 CONDITION_NOT_MET（安全条件
    // 5），发送方按发送失败处理并清 inflight。
    Status open_snapshot_reader(LogIndex expected_index, Term expected_term,
                                std::unique_ptr<SnapshotFileReader>& reader) const;

    // 打开当前已发布快照（不校验内容）。文件不存在时 exists=false、reader
    // 为空、返回 OK；存在时打开失败返回错误。远端安装收尾（安全条件 6）
    // 与 recover/测试的兼容入口用：打开即锁定一个文件版本，读取全程持同一 fd。
    Status open_snapshot_reader_any(std::unique_ptr<SnapshotFileReader>& reader, bool& exists) const;

    // 远端接收（会话四元组决定临时文件，治 F5）。param.offset==0 开新
    // 会话（O_TRUNC）；后续块 O_APPEND 并校验文件尺寸与 offset 一致。
    Status append_snapshot_chunk(const InstallSnapshotParam& param);
    // 接收完成：把会话临时文件 rename 发布为 snapshot（先发布、后裁 WAL）。
    Status finish_snapshot_receive(const InstallSnapshotParam& param);

    // 本地快照（worker 线程调用，只写候选文件，不发布）：
    // 写 snapshot.<capture_index>.tmp。失败时调用方按故障处理。
    Status write_local_snapshot_candidate(const SnapshotData& snapshot, const std::vector<RaftMember>& members,
                                          LogIndex capture_index);
    // loop 在 SnapshotDoneEvent 上的发布裁决：候选 index 严格大于当前已发布
    // 快照 index 才 rename 候选为 snapshot；无论是否发布都删除候选文件
    // （过期候选绝不覆盖更新版本，三件套第 3 条）。返回值表示是否实际发布。
    Status publish_local_snapshot(LogIndex capture_index, bool& published);

    Status write_snapshot(const SnapshotData& snapshot, const std::vector<RaftMember>& members = {});
    Status write_snapshot(const StateMachine& state_machine, const std::vector<RaftMember>& members = {});

    struct RecoverResult {
        SnapshotPtr snapshot;
        RaftMeta raft_meta;
        std::vector<LogEntry> wal_entries;
        bool need_recover{false};
    };
    Status recover(RecoverResult& result);

private:
    struct WalReadResult {
        std::vector<LogEntry> entries;
        bool error{false};
        int64_t last_good_offset{0};
        std::string error_msg;
    };

    Status write_wal_to_disk(int fd, const LogEntry& entry);
    Status read_wal_batch_unlocked(std::vector<LogEntry>& entries) const;
    Status read_wal_from_disk(const std::string& path, WalReadResult& result) const;
    Status rewrite_wal_unlocked(const std::vector<LogEntry>& entries);
    Status truncate_wal_unlocked(const LogIndex& snapshot_index);
    static Status read_snapshot_header(int fd, Snapshot* snapshot, int32& kv_count);

    // 远端接收会话的临时文件路径：dir/snapshot.<from>.<term>.<index>.<nonce>.tmp
    // （批次 B 治 F5）。nonce 由接收侧（coordinator）生成并写入 param 内存
    // 表示；PersistEngine 只按四元组确定性地拼路径。
    std::string session_snapshot_tmp_path(const InstallSnapshotParam& param) const;

    // 向 fd 写完整快照格式（write_snapshot 与候选写共用的编码器）。
    static Status write_snapshot_to_fd(int fd, const SnapshotData& snapshot, const std::vector<RaftMember>& members);

    // 快照文件族（snapshot / snapshot.tmp）的读改写互斥：接收远端 chunk、
    // 本地快照写盘、读 chunk 发送方可能来自不同线程，原先由 Replica 的
    // persist_snapshot_mutex 互斥，现在下沉到引擎内部。
    mutable std::mutex snapshot_mutex_;

    std::string wal_path_;
    std::string raft_meta_path_;
    std::string snapshot_path_;
    // 存放数据的目录
    std::string data_dir_;
    std::string dir_path_;
    ReplicaID replica_id_;

    int wal_fd_{-1};

    mutable std::shared_mutex mutex_;
};

}  // namespace adviskv::storage
