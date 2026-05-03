#include "storage/persist/persist_engine.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/buffer.h"
#include "common/crc.h"
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
    return write_wal_to_disk(wal_fd_, entry);
    // EncodeBuffer buf;
    // /*
    //     Term term{0};
    //     LogIndex index{0};
    //     WriteOpType op_type;
    //     Key key;
    //     Value value;
    // */
    // buf.write_int64(entry.term);
    // buf.write_int64(entry.index);
    // buf.write_int32((int32_t)entry.op_type);
    // buf.write_str(entry.key);
    // buf.write_str(entry.value);
    // int32_t len = buf.size();

    // ::write(wal_fd_, &len, sizeof(int32_t));
    // // TODO crc，这里以后补上
    // ::write(wal_fd_, buf.data(), buf.size());

    // return Status::OK();
}

Status PersistEngine::append_wal_batch(const std::vector<LogEntry>& entries) {
    std::unique_lock lock{mutex_};
    for (const LogEntry& entry : entries) {
        RETURN_IF_INVALID_STATUS(append_wal(entry))
    }
    ::fsync(wal_fd_);
    return Status::OK();
}

Status PersistEngine::read_wal_batch(std::vector<LogEntry>& entries) {
    std::unique_lock lock{mutex_};
    return read_wal_from_disk(wal_path_, entries);
}

Status PersistEngine::truncate_wal(const LogIndex& snapshot_index) {
    std::vector<LogEntry> entries, remain;
    RETURN_IF_INVALID_STATUS(read_wal_batch(entries))
    for (LogEntry& entry : entries) {
        if (entry.index <= snapshot_index) continue;
        remain.push_back(std::move(entry));
    }

    std::string tmp_path = wal_path_ + ".tmp";
    int fd = open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status{StatusCode::ERROR, "fd < 0"};
    }
    for (const LogEntry& entry : remain) {
        RETURN_IF_INVALID_STATUS(write_wal_to_disk(fd, entry))
    }

    ::fsync(fd);
    ::close(fd);

    ::rename(tmp_path.c_str(), wal_path_.c_str());

    wal_fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (wal_fd_ < 0) {
        return Status::ERROR("truncate wal error");
    }

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

    int64 total_len = sizeof(int64) + sizeof(int64) + sizeof(int32);
    for (const auto& [key, value] : snapshot->kvs) {
        total_len += sizeof(int32) + key.size();
        total_len += sizeof(int32) + value.size();
    }

    EncodeBuffer buf;
    buf.write<int64>(total_len);
    buf.write<int64>(snapshot->apply_index);
    buf.write<int64>(snapshot->apply_term);
    buf.write<int32>((int32_t)snapshot->kvs.size());
    // TODO  感觉这里一直往buf写有点危险。
    for (const auto& [key, value] : snapshot->kvs) {
        buf.write<std::string>(key);
        buf.write<std::string>(value);
    }

    write_full(fd, buf.data(), buf.size());

    ::fsync(fd);
    ::close(fd);

    ::rename(tmp_path.c_str(), snapshot_path_.c_str());
    return Status::OK();
}

Status PersistEngine::load_snapshot(SnapshotPtr& snapshot) {
    // TODO

    int fd = ::open(snapshot_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::ERROR("fd < 0");
    }

    int64 total_len{0};
    RETURN_IF_INVALID_STATUS(read_full(fd, &total_len, sizeof(int64)))

    snapshot = {};
    {
        std::optional<DecodeBuffer> res = read_full2buffer(fd, total_len);
        if (!res.has_value()) return Status::ERROR();
        DecodeBuffer& buf = res.value();
        RETURN_IF_INVALID_READ(buf, snapshot->apply_index)
        RETURN_IF_INVALID_READ(buf, snapshot->apply_term)

        int32 kv_count;
        RETURN_IF_INVALID_READ(buf, kv_count)
        for (int i = 0; i < kv_count; i++) {
            Key key;
            if (bool success = buf.read<std::string>(key); !success)
                return Status::ERROR();
            Value value;
            if (bool success = buf.read<std::string>(value); !success)
                return Status::ERROR();
            snapshot->kvs.emplace_back(std::move(key), std::move(value));
        }
    }

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
    int64 total_len = sizeof(int64) + sizeof(int64) + sizeof(bool) +
                      (meta.voted_for.has_value() ? (sizeof(int32) * 3) : 0);
    EncodeBuffer buf;
    buf.write<int64>(total_len);
    buf.write<int64>(meta.current_term);
    buf.write<int64>(meta.commit_index);
    buf.write<bool>(meta.voted_for.has_value());
    if (meta.voted_for.has_value()) {
        ReplicaID replica_id = meta.voted_for.value();
        buf.write<int32>(replica_id.table_id);
        buf.write<int32>(replica_id.shard_index);
        buf.write<int32>(replica_id.replica_index);
    }
    write_full(fd, buf.data(), buf.size());

    ::fsync(fd);
    ::close(fd);

    ::rename(tmp_path.c_str(), raft_meta_path_.c_str());
}
Status PersistEngine::load_raft_meta(RaftMeta& meta) const {
    // TODO

    int fd = ::open(raft_meta_path_.c_str(), O_RDONLY);

    if (fd < 0) {
        return Status{StatusCode::ERROR, "fd < 0"};
    }

    int64 total_len = 0;
    RETURN_IF_INVALID_STATUS(read_full(fd, &total_len, sizeof(int64)))
    meta = {};
    {
        std::optional<DecodeBuffer> res = read_full2buffer(fd, total_len);
        if (!res.has_value()) return Status::ERROR();
        DecodeBuffer& buf = res.value();

        RETURN_IF_INVALID_READ(buf, meta.current_term)
        RETURN_IF_INVALID_READ(buf, meta.commit_index)
        bool vote_for{false};
        RETURN_IF_INVALID_READ(buf, vote_for)
        if (vote_for) {
            ReplicaID replica_id;
            RETURN_IF_INVALID_READ(buf, replica_id.table_id)
            RETURN_IF_INVALID_READ(buf, replica_id.shard_index)
            RETURN_IF_INVALID_READ(buf, replica_id.replica_index)
            meta.voted_for = replica_id;
        }
    }
    return Status::OK();
}

Status PersistEngine::do_snapshot(const SnapshotPtr& snap) {
    RETURN_IF_INVALID_STATUS(save_snapshot(snap))
    RETURN_IF_INVALID_STATUS(truncate_wal(snap->apply_index))
    return Status::OK();
}

// buf.len + crc + buf [term + index + op_type + key + value]
Status PersistEngine::write_wal_to_disk(int fd, const LogEntry& entry) {
    EncodeBuffer buf;
    /*
        Term term{0};
        LogIndex index{0};
        WriteOpType op_type;
        Key key;
        Value value;
    */
    buf.write<int64>(entry.term);
    buf.write<int64>(entry.index);
    buf.write<int32>((int32_t)entry.op_type);
    buf.write<std::string>(entry.key);
    buf.write<std::string>(entry.value);
    int32_t len = buf.size();

    write_full(fd, &len, sizeof(int32_t));
    // crc，这里以后补上 // 补上了
    uint32 crc_val = compute_crc32(buf.data(), buf.size());
    write_full(fd, &crc_val, sizeof(uint32));
    write_full(fd, buf.data(), buf.size());

    return Status::OK();
}

Status PersistEngine::read_wal_from_disk(const std::string& path,
                                         std::vector<LogEntry>& entries) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::ERROR("");
    }

    while (true) {
        int32 data_len;
        Status status = read_full(fd, &data_len, sizeof(int32));
        if (status.code() == StatusCode::GET_EOF) break;
        if (status.fail()) return status;

        uint32 get_crc_val;
        RETURN_IF_INVALID_STATUS(read_full(fd, &get_crc_val, sizeof(uint32)))

        std::vector<uint8_t> data(data_len);
        RETURN_IF_INVALID_STATUS(read_full(fd, data.data(), data_len))

        uint32 real_crc_val = compute_crc32(data.data(), data.size());

        if (real_crc_val != get_crc_val) {
            return Status::ERROR("1");
        }

        // buf.len + crc + buf [term + index + op_type + key + value]
        DecodeBuffer buf(data);
        LogEntry entry;
        RETURN_IF_INVALID_READ(buf, entry.term)
        RETURN_IF_INVALID_READ(buf, entry.index)
        RETURN_IF_INVALID_READ(buf, entry.op_type)
        RETURN_IF_INVALID_READ(buf, entry.key)
        RETURN_IF_INVALID_READ(buf, entry.value)
    }
    return Status::OK();
}

Status PersistEngine::write_full(int fd, const void* buf, size_t len) {
    const char* b = static_cast<const char*>(buf);
    size_t have_write_len = 0;
    while (have_write_len < len) {
        ssize_t cur_write_len =
            ::write(fd, b + have_write_len, len - have_write_len);
        if (cur_write_len <= 0) {
            return StatusCode::ERROR;
        }
        have_write_len += (size_t)cur_write_len;
    }
    return Status::OK();
}

Status PersistEngine::read_full(int fd, void* buf, size_t len) const {
    char* b = static_cast<char*>(buf);
    size_t have_read_len = 0;
    while (have_read_len < len) {
        ssize_t cur_read_len =
            ::read(fd, b + have_read_len, len - have_read_len);
        if (cur_read_len == 0 and have_read_len == 0)
            return StatusCode::GET_EOF;
        if (cur_read_len <= 0) {
            return StatusCode::ERROR;
        }
        have_read_len += cur_read_len;
    }
    return Status::OK();
}

std::optional<DecodeBuffer> PersistEngine::read_full2buffer(int fd,
                                                            size_t len) const {
    std::vector<uint8> data(len);
    Status status = read_full(fd, data.data(), data.size());
    if (status.fail() and status.code() != StatusCode::GET_EOF)
        return std::nullopt;
    DecodeBuffer buffer{data};
    return buffer;
}

}  // namespace adviskv::storage