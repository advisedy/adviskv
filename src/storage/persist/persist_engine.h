#pragma once

#include <cstddef>
#include <functional>
#include <shared_mutex>

#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/state_machine/state_machine.h"
namespace adviskv::storage {

class PersistEngine {
   public:
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

    Status load_snapshot_meta(SnapshotPtr& snap) const;
    Status for_each_snapshot_kv(const KvVisitor& fn) const;
    Status read_snapshot_chunk(uint64 offset, size_t max_bytes,
                               std::string& data, bool& eof) const;
    Status append_snapshot_chunk(const InstallSnapshotParam& param);
    Status finish_snapshot_receive(const SnapshotPtr& snap);

    Status do_snapshot(const StateMachine& state_machine);
    Status clear_wal();

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
    Status read_wal_from_disk(const std::string& path,
                              WalReadResult& result) const;
    Status rewrite_wal_unlocked(const std::vector<LogEntry>& entries);
    Status truncate_wal_unlocked(const LogIndex& snapshot_index);
    Status read_snapshot_header(int fd, Snapshot* snapshot,
                                int32& kv_count) const;

    std::string wal_path_;
    std::string raft_meta_path_;
    std::string snapshot_path_;
    std::string snapshot_tmp_path_;
    // 存放数据的目录
    std::string data_dir_;
    std::string dir_path_;
    ReplicaID replica_id_;

    int wal_fd_{-1};

    mutable std::shared_mutex mutex_;
};

}  // namespace adviskv::storage
