#pragma once

#include <cstddef>
#include <functional>
#include <optional>
#include <shared_mutex>

#include "common/buffer.h"
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
    Status read_snapshot_header(int fd, Snapshot* snapshot,
                                int32& kv_count) const;
    Status write_full(int fd, const void* buf, size_t len);
    Status read_full(int fd, void* buf, size_t len) const;
    std::optional<DecodeBuffer> read_full2buffer(int fd, size_t len)const;

    template <typename T>
    Status write_value(int fd, const T& v) {
        return write_full(fd, &v, sizeof(T));
    }
    template <typename T>
    Status read_value(int fd, T& v) const {
        return read_full(fd, &v, sizeof(T));
    }
    Status write_string(int fd, const std::string& s);
    Status read_string(int fd, std::string& s) const;

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