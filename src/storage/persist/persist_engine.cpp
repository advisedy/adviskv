#include "storage/persist/persist_engine.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/buffer.h"
#include "common/crc.h"
#include "common/defer.h"
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
        LOG_WARN("...");
    }
}

Status PersistEngine::init() {
    dir_path_ = data_dir_ + "/" + std::to_string(replica_id_.table_id) + "-" +
                std::to_string(replica_id_.shard_index);
    wal_path_ = dir_path_ + "/wal.log";
    raft_meta_path_ = dir_path_ + "/raft_meta";
    snapshot_path_ = dir_path_ + "/snapshot";
    snapshot_tmp_path_ = snapshot_path_ + ".tmp";

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
}

Status PersistEngine::append_wal_batch(const std::vector<LogEntry>& entries) {
    std::unique_lock lock{mutex_};
    for (const LogEntry& entry : entries) {
        RETURN_IF_INVALID_STATUS(write_wal_to_disk(wal_fd_, entry))
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
    ::close(wal_fd_);

    ::rename(tmp_path.c_str(), wal_path_.c_str());

    wal_fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (wal_fd_ < 0) {
        return Status::ERROR("truncate wal error");
    }

    return Status::OK();
}

Status PersistEngine::read_snapshot_header(int fd, Snapshot* snapshot,
                                           int32& kv_count) const {
    int64 payload_len{0};
    Status status = read_value(fd, payload_len);
    if (status.code() == StatusCode::GET_EOF) {
        kv_count = 0;
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(status)

    LogIndex apply_index{0};
    Term apply_term{0};
    RETURN_IF_INVALID_STATUS(read_value(fd, apply_index))
    RETURN_IF_INVALID_STATUS(read_value(fd, apply_term))
    RETURN_IF_INVALID_STATUS(read_value(fd, kv_count))
    if (kv_count < 0) {
        return Status{StatusCode::ERROR, "invalid kv_count"};
    }
    if (snapshot) {
        snapshot->apply_index = apply_index;
        snapshot->apply_term = apply_term;
    }
    return Status::OK();
}

Status PersistEngine::load_snapshot_meta(SnapshotPtr& snapshot) const {
    int fd = ::open(snapshot_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            snapshot.reset();
            return Status::OK();
        }
        return Status::ERROR("fd < 0");
    }
    auto fd_guard = common::Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    if (!snapshot) {
        snapshot = std::make_shared<Snapshot>();
    }
    int32 kv_count{0};
    RETURN_IF_INVALID_STATUS(read_snapshot_header(fd, snapshot.get(), kv_count))
    snapshot->path = snapshot_path_;

    return Status::OK();
}

Status PersistEngine::for_each_snapshot_kv(const KvVisitor& fn) const {
    int fd = ::open(snapshot_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return Status::OK();
        }
        return Status::ERROR("fd < 0");
    }
    auto fd_guard = common::Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    int32 kv_count{0};
    RETURN_IF_INVALID_STATUS(read_snapshot_header(fd, nullptr, kv_count))

    for (int i = 0; i < kv_count; i++) {
        Key key;
        Value value;
        RETURN_IF_INVALID_STATUS(read_string(fd, key))
        RETURN_IF_INVALID_STATUS(read_string(fd, value))
        RETURN_IF_INVALID_STATUS(fn(key, value))
    }

    return Status::OK();
}

Status PersistEngine::read_snapshot_chunk(uint64 offset, size_t max_bytes,
                                          std::string& data, bool& eof) const {
    data.clear();
    eof = false;
    int fd = ::open(snapshot_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::ERROR("fd < 0");
    }
    auto fd_guard = common::Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        return Status::ERROR("failed to stat snapshot file");
    }
    if (offset > static_cast<uint64>(st.st_size)) {
        return Status::ERROR("snapshot chunk offset out of range");
    }

    if (::lseek(fd, static_cast<off_t>(offset), SEEK_SET) < 0) {
        return Status::ERROR("failed to seek snapshot file");
    }

    size_t read_len =
        std::min(max_bytes, static_cast<size_t>(st.st_size - offset));
    data.resize(read_len);
    if (read_len > 0) {
        RETURN_IF_INVALID_STATUS(read_full(fd, data.data(), read_len))
    }
    eof = offset + read_len >= static_cast<uint64>(st.st_size);
    return Status::OK();
}

Status PersistEngine::append_snapshot_chunk(const InstallSnapshotParam& param) {
    int flags = O_WRONLY | O_CREAT;
    flags |= (param.offset == 0) ? O_TRUNC : O_APPEND;
    int fd = ::open(snapshot_tmp_path_.c_str(), flags, 0644);
    if (fd < 0) {
        return Status::ERROR("failed to open snapshot tmp file");
    }
    auto fd_guard = common::Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        return Status::ERROR("failed to stat snapshot tmp file");
    }
    if (static_cast<uint64>(st.st_size) != param.offset) {
        return Status::ERROR("snapshot chunk offset mismatch");
    }
    if (!param.data.empty()) {
        RETURN_IF_INVALID_STATUS(
            write_full(fd, param.data.data(), param.data.size()))
    }
    ::fsync(fd);
    return Status::OK();
}

// 在快照的chunk都发送完了之后会调用这个
Status PersistEngine::finish_snapshot_receive(const SnapshotPtr& snap) {
    if (!snap) {
        return Status::ERROR("snapshot is nullptr");
    }
    if (::rename(snapshot_tmp_path_.c_str(), snapshot_path_.c_str()) != 0) {
        return Status::ERROR("failed to publish received snapshot");
    }
    snap->path = snapshot_path_;
    return Status::OK();
}

Status PersistEngine::do_snapshot(const StateMachine& state_machine) {
    // Stream snapshot without materializing all KVs.
    int fd =
        open(snapshot_tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status{StatusCode::ERROR, "failed to open snapshot tmp file"};
    }
    auto fd_guard = common::Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    LogIndex apply_index = state_machine.apply_index();
    Term apply_term = state_machine.apply_term();

    // Header placeholders.
    RETURN_IF_INVALID_STATUS(write_value<int64>(fd, 0))
    RETURN_IF_INVALID_STATUS(write_value(fd, apply_index))
    RETURN_IF_INVALID_STATUS(write_value(fd, apply_term))
    RETURN_IF_INVALID_STATUS(write_value<int32>(fd, 0))

    int32 kv_count = 0;
    Status it_status =
        state_machine.for_each_kv([&](const Key& k, const Value& v) -> Status {
            RETURN_IF_INVALID_STATUS(write_string(fd, k))
            RETURN_IF_INVALID_STATUS(write_string(fd, v))
            ++kv_count;
            return Status::OK();
        });
    RETURN_IF_INVALID_STATUS(it_status)

    off_t end_pos = ::lseek(fd, 0, SEEK_END);
    if (end_pos < 0) {
        return Status{StatusCode::ERROR, "lseek end failed"};
    }
    int64 payload_len = static_cast<int64>(end_pos) - sizeof(int64);

    if (::lseek(fd, 0, SEEK_SET) < 0) {
        return Status{StatusCode::ERROR, "lseek set failed"};
    }
    RETURN_IF_INVALID_STATUS(write_value(fd, payload_len))
    if (::lseek(fd, sizeof(int64) + sizeof(int64) + sizeof(int64), SEEK_SET) <
        0) {
        return Status{StatusCode::ERROR, "lseek kv_count failed"};
    }
    RETURN_IF_INVALID_STATUS(write_value(fd, kv_count))

    ::fsync(fd);
    // Close before rename.
    ::close(fd);
    fd = -1;

    // 1) Publish snapshot (atomic rename)
    ::rename(snapshot_tmp_path_.c_str(), snapshot_path_.c_str());
    // 2) Then truncate WAL up to snapshot index
    RETURN_IF_INVALID_STATUS(truncate_wal(apply_index))
    return Status::OK();
}

Status PersistEngine::write_string(int fd, const std::string& s) {
    int32 len = static_cast<int32>(s.size());
    RETURN_IF_INVALID_STATUS(write_value(fd, len))
    assert(len > 0);
    if (len == 0) return Status::OK();
    return write_full(fd, s.data(), static_cast<size_t>(len));
}

Status PersistEngine::read_string(int fd, std::string& s) const {
    int32 len{0};
    RETURN_IF_INVALID_STATUS(read_value(fd, len))
    if (len < 0) {
        return Status{StatusCode::ERROR, "invalid string len"};
    } else if (len == 0) {
        s.clear();
        return Status::OK();
    }
    s.resize(len);
    return read_full(fd, s.data(), static_cast<size_t>(len));
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
    return Status::OK();
}
Status PersistEngine::load_raft_meta(RaftMeta& meta) const {
    // TODO

    int fd = ::open(raft_meta_path_.c_str(), O_RDONLY);

    if (fd < 0) {
        if (errno == ENOENT) {
            meta = {};
            return Status::OK();
        }
        return Status{StatusCode::ERROR, "fd < 0"};
    }
    auto fd_guard = common::Defer([fd]() { ::close(fd); });

    meta = {};
    int64 total_len = 0;
    {
        Status status = (read_full(fd, &total_len, sizeof(int64)));
        if (status.code() == StatusCode::GET_EOF) {
            return Status::OK();
        }
        RETURN_IF_INVALID_STATUS(status)
    }
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
    buf.write(entry.term);
    buf.write(entry.index);
    buf.write((int32)entry.op_type);
    buf.write(entry.key);
    buf.write(entry.value);
    int32_t len = buf.size();

    write_full(fd, &len, sizeof(int32_t));
    // crc，这里以后补上 // 补上了
    uint32 crc_val = compute_crc32(buf.data(), buf.size());
    write_full(fd, &crc_val, sizeof(uint32));
    write_full(fd, buf.data(), buf.size());

    return Status::OK();
}

Status PersistEngine::recover(RecoverResult& result) {
    result = {};

    SnapshotPtr snap = std::make_shared<Snapshot>();
    Status snap_status = load_snapshot_meta(snap);
    if (snap_status.ok() && snap) {
        result.snapshot = snap;
    }

    RETURN_IF_INVALID_STATUS(load_raft_meta(result.raft_meta))
    RETURN_IF_INVALID_STATUS(read_wal_batch(result.wal_entries))
    return Status::OK();
}

Status PersistEngine::read_wal_from_disk(const std::string& path,
                                         std::vector<LogEntry>& entries) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            entries.clear();
            return Status::OK();
        }
        return Status::ERROR("fd < 0");
    }
    auto fd_guard = common::Defer([fd]() { ::close(fd); });

    entries.clear();
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
        int32 op_type;
        RETURN_IF_INVALID_READ(buf, op_type)
        entry.op_type = (WriteOpType)op_type;
        LOG_DEBUG("entry term:{}, index:{}, op_type:{} ", entry.term,
                  entry.index, op_type)
        RETURN_IF_INVALID_READ(buf, entry.key)
        RETURN_IF_INVALID_READ(buf, entry.value)
        LOG_DEBUG("enry key:{}, value:{}", entry.key, entry.value)
        entries.push_back(std::move(entry));
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
