#pragma once

#include <filesystem>
#include <string>

#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/persist/raft_meta_store.h"
#include "storage/persist/snapshot_store.h"
#include "storage/persist/wal_store.h"

namespace adviskv::storage {

class PersistEngine {
public:

    static std::string replica_dir_name(const ReplicaID& replica_id);
    static std::filesystem::path replica_dir(const std::string& data_dir, const ReplicaID& replica_id);
    static std::filesystem::path wal_file(const std::filesystem::path& dir);
    static std::filesystem::path raft_meta_file(const std::filesystem::path& dir);
    static std::filesystem::path snapshot_file(const std::filesystem::path& dir);

    PersistEngine(const std::string& data_dir, const ReplicaID& replica_id);
    ~PersistEngine();

    // 建目录、开 WAL、把三个 store 的路径定下来。
    Status init();
    Status close();

    WalStore& wal() { return wal_; }
    RaftMetaStore& raft_meta() { return raft_meta_; }
    SnapshotStore& snapshot() { return snapshot_; }

    const WalStore& wal() const { return wal_; }
    const RaftMetaStore& raft_meta() const { return raft_meta_; }
    const SnapshotStore& snapshot() const { return snapshot_; }

    // recover 的产物
    struct RecoverResult {
        SnapshotPtr snapshot;
        RaftMeta raft_meta;
        std::vector<LogEntry> wal_entries;
        bool need_recover{false};
    };
    Status recover(RecoverResult& result);

private:
    std::string data_dir_;
    std::string dir_path_;
    ReplicaID replica_id_;


    WalStore wal_;
    RaftMetaStore raft_meta_;
    SnapshotStore snapshot_;
};

}  // namespace adviskv::storage
