#include "storage/persist/persist_engine.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <mutex>
#include <string>

#include "common/buffer.h"
#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/state_machine/state_machine.h"
namespace adviskv::storage {

PersistEngine::PersistEngine(const std::string& data_dir,
                             const ReplicaID& replica_id)
    : data_dir_(data_dir), replica_id_(replica_id) {}

PersistEngine::~PersistEngine() {
    Status status = close();
    if (status.fail()) {
        WARN("...");
    }
}

Status PersistEngine::init() {
    dir_path_ = data_dir_ + "/" + std::to_string(replica_id_.table_id) + "-" +
                std::to_string(replica_id_.shard_index);
    wal_path_ = dir_path_ + "/wal.log";
    raft_meta_path_ = dir_path_ + "/raft_meta";
    snapshot_path_ = dir_path_ + "/snapshot";

    mkdir(dir_path_.c_str(), 0755);

    wal_fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (wal_fd_ < 0) {
        return Status{StatusCode::ERROR, "failed to open wal file"};
    }
    return Status::OK();
}

Status PersistEngine::close() {
    if (wal_fd_ != -1) {
        ::close(wal_fd_);
        wal_fd_ = -1;
    }
    return Status::OK();
}

Status PersistEngine::append_wal(const LogEntry& entry) {
    std::unique_lock lock{mutex_};
    EncodeBuffer buf;
    /*
        Term term{0};
        LogIndex index{0};
        WriteOpType op_type;
        Key key;
        Value value;
    */
    buf.write_int64(entry.term);
    buf.write_int64(entry.index);
    buf.write_int32((int32_t)entry.op_type);
    buf.write_str(entry.key);
    buf.write_str(entry.value);
    int32_t len = buf.size();

    ::write(wal_fd_, &len, sizeof(int32_t));
    // TODO crc，这里以后补上
    ::write(wal_fd_, buf.data(), buf.size());

    return Status::OK();
}

Status PersistEngine::read_wal_all(std::vector<LogEntry>& entries) {
    // TODO
    return Status::OK();
}

Status PersistEngine::truncate_wal(const LogIndex& snapshot_index) {
    // TODO
    return Status::OK();
}

// private:
// std::string PersistEngine::make_snapshot_path(LogIndex snap_index) const {
//     return dir_path_ + "/snapshot_" + std::to_string(snap_index);
// }

Status PersistEngine::save_snapshot(const SnapshotPtr& snapshot) {
    std::string tmp_path = snapshot_path_ + ".tmp";

    int fd = open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);

    if (fd < 0) {
        return Status{StatusCode::ERROR, "fd < 0"};
    }

    EncodeBuffer buf;
    buf.write_int64(snapshot->apply_index);
    buf.write_int64(snapshot->apply_term);
    buf.write_int32((int32_t)snapshot->kvs.size());
    // TODO  感觉这里一直往buf写有点危险。
    for (const auto& [key, value] : snapshot->kvs) {
        buf.write_str(key);
        buf.write_str(value);
    }

    ::write(fd, buf.data(), buf.size());

    ::fsync(fd);
    ::close(fd);

    ::rename(tmp_path.c_str(), snapshot_path_.c_str());
    return Status::OK();
}

Status PersistEngine::load_snapshot(SnapshotPtr& snapshot) {
    // TODO
    return Status::OK();
}

Status PersistEngine::save_raft_meta(const RaftMeta& meta) {
    std::string tmp_path = raft_meta_path_ + ".tmp";

    int fd = open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);

    if (fd < 0) {
        return Status{StatusCode::ERROR, "fd < 0"};
    }

    /*
        Term current_term;
        LogIndex commit_index{0};
        std::optional<ReplicaID> voted_for;
        // term + index + table_id + shard_id + replica_id
    */
    EncodeBuffer buf;
    buf.write_int64(meta.current_term);
    buf.write_int64(meta.commit_index);
    buf.write_bool(meta.voted_for.has_value());
    if (meta.voted_for.has_value()) {
        ReplicaID replica_id = meta.voted_for.value();
        buf.write_int32(replica_id.table_id);
        buf.write_int32(replica_id.shard_index);
        buf.write_int32(replica_id.replica_index);
    }
    ::write(fd, buf.data(), buf.size());

    ::fsync(fd);
    ::close(fd);

    ::rename(tmp_path.c_str(), snapshot_path_.c_str());
}
Status PersistEngine::load_raft_meta(RaftMeta& meta) const {
    // TODO

    return Status::OK();
}

Status PersistEngine::do_snapshot(const SnapshotPtr& snap) {
    RETURN_IF_INVALID_STATUS(save_snapshot(snap))
    RETURN_IF_INVALID_STATUS(truncate_wal(snap->apply_index))
    return Status::OK();
}

}  // namespace adviskv::storage