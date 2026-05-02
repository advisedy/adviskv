#pragma once

#include <shared_mutex>

#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/state_machine/state_machine.h"
namespace adviskv::storage {

struct RaftMeta {
    Term current_term;
    LogIndex commit_index{0};
    std::optional<ReplicaID> voted_for;
};

class PersistEngine {
   public:
    PersistEngine(const std::string& data_dir, const ReplicaID& replica_id);
    ~PersistEngine();

    Status init();
    Status close();

    Status append_wal(const LogEntry& entry);
    Status append_wal_batch(const std::vector<LogEntry>& entries);
    Status read_wal_all(std::vector<LogEntry>& entries);
    Status truncate_wal(const LogIndex& snapshot_index);

    Status save_raft_meta(const RaftMeta& meta);
    Status load_raft_meta(RaftMeta& meta) const;

    Status save_snapshot(const SnapshotPtr& snap);
    Status load_snapshot(SnapshotPtr& snap);

    Status do_snapshot(const SnapshotPtr& snap);

    struct RecoverResult {
        SnapshotPtr snapshot;
        RaftMeta raft_meta;
        std::vector<LogEntry> wal_entries;
    };
    Status recover(RecoverResult& result);

   private:
    Status write_wal_to_disk(int fd, const LogEntry& entry);
    Status read_wal_from_disk(const std::string& path,
                           std::vector<LogEntry>& entries);

    std::string wal_path_;
    std::string raft_meta_path_;
    std::string snapshot_path_;

    // std::string make_snapshot_path(LogIndex snapshot_index) const;

    // 存放数据的目录
    std::string data_dir_;
    std::string dir_path_;
    ReplicaID replica_id_;

    int wal_fd_{-1};

    mutable std::shared_mutex mutex_;
};

}  // namespace adviskv::storage