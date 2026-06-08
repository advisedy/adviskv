#include "storage/persist/persist_engine.h"

#include <dirent.h>
#include <fcntl.h>
#include <fmt/format.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/buffer.h"
#include "common/crash_injection.h"
#include "common/defer.h"
#include "common/define.h"
#include "common/framed_record_codec.h"
#include "common/func.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/state_machine/state_machine.h"
namespace adviskv::storage {

static constexpr int32 MAX_WAL_ENTRY_PAYLOAD_BYTES = 64 * 1024 * 1024;
static constexpr int64 MAX_RAFT_META_PAYLOAD_BYTES = 1024 * 1024;

namespace {

class WalEntryCodec {
   public:
    using ObjectType = LogEntry;
    using LenType = int32;

    LenType max_payload_len() const { return MAX_WAL_ENTRY_PAYLOAD_BYTES; }

    void encode_payload(const ObjectType& entry, EncodeBuffer& buf) const {
        buf.write(entry.term);
        buf.write(entry.index);
        buf.write(static_cast<int32>(entry.op_type));
        buf.write(entry.key);
        buf.write(entry.value);
    }

    Status decode_payload(DecodeBuffer& buf, ObjectType& entry) const {
        int32 op_type{0};
        RETURN_IF_INVALID_READ(buf, entry.term)
        RETURN_IF_INVALID_READ(buf, entry.index)
        RETURN_IF_INVALID_READ(buf, op_type)
        RETURN_IF_INVALID_READ(buf, entry.key)
        RETURN_IF_INVALID_READ(buf, entry.value)

        if (op_type != static_cast<int32>(WriteOpType::PUT) &&
            op_type != static_cast<int32>(WriteOpType::DEL) &&
            op_type != static_cast<int32>(WriteOpType::NONE)) {
            return Status::ERROR(
                fmt::format("invalid wal op_type: {}", op_type));
        }
        entry.op_type = static_cast<WriteOpType>(op_type);
        return Status::OK();
    }
};

class RaftMetaCodec {
   public:
    using ObjectType = RaftMeta;
    using LenType = int64;

    LenType max_payload_len() const { return MAX_RAFT_META_PAYLOAD_BYTES; }

    void encode_payload(const ObjectType& meta, EncodeBuffer& buf) const {
        buf.write(meta.current_term);
        buf.write(meta.commit_index);
        buf.write<bool>(meta.voted_for.has_value());
        if (meta.voted_for.has_value()) {
            const ReplicaID& replica_id = meta.voted_for.value();
            buf.write(replica_id.table_id);
            buf.write(replica_id.shard_index);
            buf.write(replica_id.replica_index);
        }
    }

    Status decode_payload(DecodeBuffer& buf, ObjectType& meta) const {
        meta = {};
        RETURN_IF_INVALID_READ(buf, meta.current_term)
        RETURN_IF_INVALID_READ(buf, meta.commit_index)
        bool has_voted_for{false};
        RETURN_IF_INVALID_READ(buf, has_voted_for)
        if (has_voted_for) {
            ReplicaID replica_id;
            RETURN_IF_INVALID_READ(buf, replica_id.table_id)
            RETURN_IF_INVALID_READ(buf, replica_id.shard_index)
            RETURN_IF_INVALID_READ(buf, replica_id.replica_index)
            meta.voted_for = replica_id;
        }
        return Status::OK();
    }
};

}  // namespace

PersistEngine::PersistEngine(const std::string& data_dir,
                             const ReplicaID& replica_id)
    : data_dir_(data_dir), replica_id_(replica_id) {}

PersistEngine::~PersistEngine() {
    Status status = close();
    if (status.fail()) {
        LOG_WARN("...11111");
    }
}

Status PersistEngine::init() {
    if (data_dir_.empty()) {
        return Status::INVALID_ARGUMENT(
            "persist engine init error: data_dir is epmty");
    }
    if (ReplicaID& id = replica_id_;
        id.replica_index == -1 or id.shard_index == -1 or id.table_id == -1) {
        return Status::INVALID_ARGUMENT(
            fmt::format("persist engine init error: replica_id:{} invalid",
                        replica_id_.to_string()));
    }
    dir_path_ = data_dir_ + "/" + std::to_string(replica_id_.table_id) + "-" +
                std::to_string(replica_id_.shard_index);
    wal_path_ = dir_path_ + "/wal.log";
    raft_meta_path_ = dir_path_ + "/raft_meta";
    snapshot_path_ = dir_path_ + "/snapshot";
    snapshot_tmp_path_ = snapshot_path_ + ".tmp";
    LOG_DEBUG(
        "dir_path_={}, wal_path_={}, raft_meta_path_={}, snapshot_path_={}, "
        "snapshot_tmp_path_={}",
        dir_path_, wal_path_, raft_meta_path_, snapshot_path_,
        snapshot_tmp_path_);
    mkdir(dir_path_.c_str(), 0755);  // TODO 改成create_dircation

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
    ADVISKV_METRICS_TIMER("storage_persist_append_wal");
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_request");

    std::unique_lock lock{mutex_};
    Status status = write_wal_to_disk(wal_fd_, entry);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_persist_append_wal_failure");
        return status;
    }
    if (::fsync(wal_fd_) != 0) {
        ADVISKV_METRICS_COUNTER("storage_persist_append_wal_fsync_failure");
        return Status::ERROR("fsync != 0");
    }
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_success");
    return Status::OK();
}

Status PersistEngine::append_wal_batch(const std::vector<LogEntry>& entries) {
    ADVISKV_METRICS_TIMER("storage_persist_append_wal_batch");
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_request");
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_entry",
                            static_cast<int64_t>(entries.size()));

    std::unique_lock lock{mutex_};
    for (const LogEntry& entry : entries) {
        Status status = write_wal_to_disk(wal_fd_, entry);
        if (status.fail()) {
            ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_failure");
            return status;
        }
    }
    if (::fsync(wal_fd_) != 0) {
        ADVISKV_METRICS_COUNTER(
            "storage_persist_append_wal_batch_fsync_failure");
        return Status::ERROR("fsync != 0");
    }
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_success");
    return Status::OK();
}

Status PersistEngine::read_wal_batch(std::vector<LogEntry>& entries) {
    std::unique_lock lock{mutex_};
    return read_wal_batch_unlocked(entries);
}

Status PersistEngine::read_wal_batch_unlocked(
    std::vector<LogEntry>& entries) const {
    WalReadResult result;
    RETURN_IF_INVALID_STATUS(read_wal_from_disk(wal_path_, result))
    entries = std::move(result.entries);
    if (result.error) {
        return Status::ERROR(result.error_msg);
    }
    return Status::OK();
}

Status PersistEngine::truncate_wal(const LogIndex& snapshot_index) {
    std::unique_lock lock{mutex_};

    std::vector<LogEntry> entries, remain;
    RETURN_IF_INVALID_STATUS(read_wal_batch_unlocked(entries))

    for (LogEntry& entry : entries) {
        if (entry.index <= snapshot_index) continue;
        remain.push_back(std::move(entry));
    }

    std::string tmp_path = wal_path_ + ".tmp";
    int fd = open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status{StatusCode::ERROR, "fd < 0"};
    }
    auto fd_guard = Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    for (const LogEntry& entry : remain) {
        RETURN_IF_INVALID_STATUS(write_wal_to_disk(fd, entry))
    }

    if (::fsync(fd) != 0) {
        return Status::ERROR("failed to fsync wal tmp file");
    }
    if (::close(fd) != 0) {
        fd = -1;
        return Status::ERROR("failed to close wal tmp file");
    }
    fd = -1;

    if (wal_fd_ != -1) {
        if (::close(wal_fd_) != 0) {
            wal_fd_ = -1;
            return Status::ERROR("failed to close wal file before rename");
        }
        wal_fd_ = -1;
    }

    if (::rename(tmp_path.c_str(), wal_path_.c_str()) != 0) {
        return Status::ERROR("failed to rename wal tmp file");
    }
    RETURN_IF_INVALID_STATUS(func::fsync_dir(dir_path_))

    wal_fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (wal_fd_ < 0) {
        return Status::ERROR("truncate wal error");
    }

    return Status::OK();
}

Status PersistEngine::truncate_wal_to_offset(int64_t offset) {
    if (offset < 0) {
        return Status::ERROR("wal truncate offset is negative");
    }

    if (wal_fd_ < 0) {
        wal_fd_ =
            ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (wal_fd_ < 0) {
            return Status::ERROR("failed to open wal for truncate");
        }
    }

    if (::fsync(wal_fd_) != 0) {
        return Status::ERROR("failed to fsync wal before truncate");
    }

    if (::ftruncate(wal_fd_, static_cast<off_t>(offset)) != 0) {
        return Status::ERROR("failed to truncate wal to offset");
    }

    if (::fsync(wal_fd_) != 0) {
        return Status::ERROR("failed to fsync truncated wal");
    }

    return Status::OK();
}

Status PersistEngine::read_snapshot_header(int fd, Snapshot* snapshot,
                                           int32& kv_count) const {
    int64 payload_len{0};
    Status status = func::read_value(fd, payload_len);
    if (status.code() == StatusCode::GET_EOF) {
        kv_count = 0;
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(status)

    LogIndex apply_index{0};
    Term apply_term{0};
    RETURN_IF_INVALID_STATUS(func::read_value(fd, apply_index))
    RETURN_IF_INVALID_STATUS(func::read_value(fd, apply_term))
    RETURN_IF_INVALID_STATUS(func::read_value(fd, kv_count))
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
    auto fd_guard = Defer([&fd]() {
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
    auto fd_guard = Defer([&fd]() {
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
        RETURN_IF_INVALID_STATUS(func::read_string(fd, key))
        RETURN_IF_INVALID_STATUS(func::read_string(fd, value))
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
    auto fd_guard = Defer([&fd]() {
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
        RETURN_IF_INVALID_STATUS(func::read_full(fd, data.data(), read_len))
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
    auto fd_guard = Defer([&fd]() {
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
            func::write_full(fd, param.data.data(), param.data.size()))
    }
    if (::fsync(fd) != 0) {
        return Status::ERROR("failed to fsync snapshot tmp file");
    }
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
    RETURN_IF_INVALID_STATUS(func::fsync_dir(dir_path_))

    snap->path = snapshot_path_;
    return Status::OK();
}
// 拿到了状态机去做快照，快照落盘了之后再截取wal
// TODO 这里关于lseek的内容是AI写的， 回头一定要记得看一下
Status PersistEngine::do_snapshot(const StateMachine& state_machine) {
    // Stream snapshot without materializing all KVs.
    int fd =
        open(snapshot_tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status{StatusCode::ERROR, "failed to open snapshot tmp file"};
    }
    auto fd_guard = Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    LogIndex apply_index = state_machine.apply_index();
    Term apply_term = state_machine.apply_term();

    // 这里先占位置
    RETURN_IF_INVALID_STATUS(func::write_value<int64>(fd, 0))
    RETURN_IF_INVALID_STATUS(func::write_value(fd, apply_index))
    RETURN_IF_INVALID_STATUS(func::write_value(fd, apply_term))
    RETURN_IF_INVALID_STATUS(func::write_value<int32>(fd, 0))

    int32 kv_count = 0;
    Status it_status =
        state_machine.for_each_kv([&](const Key& k, const Value& v) -> Status {
            RETURN_IF_INVALID_STATUS(func::write_string(fd, k))
            RETURN_IF_INVALID_STATUS(func::write_string(fd, v))
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
    RETURN_IF_INVALID_STATUS(func::write_value(fd, payload_len))
    if (::lseek(fd, sizeof(int64) + sizeof(int64) + sizeof(int64), SEEK_SET) <
        0) {
        return Status{StatusCode::ERROR, "lseek kv_count failed"};
    }
    RETURN_IF_INVALID_STATUS(func::write_value(fd, kv_count))

    if (::fsync(fd) != 0) {
        return Status::ERROR("failed to fsync snapshot tmp file");
    }
    // Close before rename.
    if (::close(fd) != 0) {
        fd = -1;
        return Status::ERROR("failed to close snapshot tmp file");
    }
    fd = -1;

    // 1) Publish snapshot (atomic rename)
    if (::rename(snapshot_tmp_path_.c_str(), snapshot_path_.c_str()) != 0) {
        return Status::ERROR("failed to rename snapshot tmp file");
    }
    RETURN_IF_INVALID_STATUS(func::fsync_dir(dir_path_))

    // 2) Then truncate WAL up to snapshot index
    testhook::crash_point("do_snapshot.after_write_snapshot");
    RETURN_IF_INVALID_STATUS(truncate_wal(apply_index))
    return Status::OK();
}

Status PersistEngine::save_raft_meta(const RaftMeta& meta) {
    std::string tmp_path = raft_meta_path_ + ".tmp";

    int fd = open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);

    if (fd < 0) {
        return Status{StatusCode::ERROR, "fd < 0"};
    }
    auto fd_guard = Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    RETURN_IF_INVALID_STATUS(
        FramedRecord<RaftMetaCodec>::encode_to_fd(fd, meta))

    if (::fsync(fd) != 0) {
        return Status::ERROR("failed to fsync raft meta tmp file");
    }
    if (::close(fd) != 0) {
        fd = -1;
        return Status::ERROR("failed to close raft meta tmp file");
    }
    fd = -1;

    if (::rename(tmp_path.c_str(), raft_meta_path_.c_str()) != 0) {
        return Status::ERROR("failed to rename raft meta tmp file");
    }
    RETURN_IF_INVALID_STATUS(func::fsync_dir(dir_path_))

    return Status::OK();
}
Status PersistEngine::load_raft_meta(RaftMeta& meta) const {
    int fd = ::open(raft_meta_path_.c_str(), O_RDONLY);

    if (fd < 0) {
        if (errno == ENOENT) {
            meta = {};
            return Status::OK();
        }
        return Status{StatusCode::ERROR, "fd < 0"};
    }
    auto fd_guard = Defer([fd]() { ::close(fd); });

    Status status = FramedRecord<RaftMetaCodec>::decode_from_fd(fd, meta);
    if (status.code() == StatusCode::GET_EOF) {
        meta = {};
        return Status::OK();
    }
    return status;
}

// buf.len + crc + buf [term + index + op_type + key + value]
Status PersistEngine::write_wal_to_disk(int fd, const LogEntry& entry) {
    return FramedRecord<WalEntryCodec>::encode_to_fd(fd, entry);
}

Status PersistEngine::recover(RecoverResult& result) {
    std::unique_lock lock{mutex_};
    result = {};

    {
        result.snapshot = std::make_shared<Snapshot>();
        Status status = load_snapshot_meta(result.snapshot);
        RETURN_IF_INVALID_STATUS(status)
    }

    RETURN_IF_INVALID_STATUS(load_raft_meta(result.raft_meta))

    const LogIndex original_commit_index = result.raft_meta.commit_index;
    WalReadResult wal_read_result;
    RETURN_IF_INVALID_STATUS(read_wal_from_disk(wal_path_, wal_read_result))

    LogIndex snapshot_index =
        result.snapshot ? result.snapshot->apply_index : 0;

    // 这里要处理一下这个entries，需要把快照之前的给去掉
    // 原因是因为，在快照落盘之后，截断WAL日志的落盘的之前，可能崩溃，那WAL的落盘文件就没有更改。
    // 所以我们在读取的时候手动这里再截取一下
    func::ad_erase_if(wal_read_result.entries,
                      [snapshot_index](const LogEntry& entry) {
                          return entry.index <= snapshot_index;
                      });

    bool wal_gap_after_snapshot = false;
    if (!wal_read_result.entries.empty() &&
        wal_read_result.entries.front().index != snapshot_index + 1) {
        wal_gap_after_snapshot = true;
    }

    LogIndex local_last_good_index = snapshot_index;
    if (!wal_gap_after_snapshot) {
        if (!wal_read_result.entries.empty()) {
            local_last_good_index = wal_read_result.entries.back().index;
        }
        result.wal_entries = std::move(wal_read_result.entries);
    } else {
        result.wal_entries.clear();
    }

    result.wal_recovery.last_good_index = local_last_good_index;
    result.wal_recovery.last_good_offset = wal_read_result.last_good_offset;
    result.wal_recovery.original_commit_index = original_commit_index;
    result.wal_recovery.recovery_target_commit_index = original_commit_index;

    if (!wal_read_result.error) {
        if (wal_gap_after_snapshot or
            local_last_good_index <
                original_commit_index) {  // 有可能读完了， 但是后面全少了
            LOG_WARN(
                "wal is complete but behind committed index during recover, "
                "last_good_index={}, commit_index={}",
                local_last_good_index, original_commit_index);
            result.wal_recovery.action = WalRecoveryAction::NEED_RAFT_CATCHUP;
            result.raft_meta.commit_index = local_last_good_index;
        } else {
            // 这里是正常路线，没有问题
            result.wal_recovery.action = WalRecoveryAction::NONE;
        }
        return Status::OK();
    }

    LOG_WARN(
        "wal corrupted during recover, reason={}, last_good_index={}, "
        "last_good_offset={}, commit_index={}",
        wal_read_result.error_msg, local_last_good_index,
        wal_read_result.last_good_offset, original_commit_index);

    RETURN_IF_INVALID_STATUS(
        truncate_wal_to_offset(wal_read_result.last_good_offset))

    if (local_last_good_index >= original_commit_index) {
        result.wal_recovery.action = WalRecoveryAction::TRUNCATED_UNCOMMITTED;
    } else {
        result.wal_recovery.action = WalRecoveryAction::NEED_RAFT_CATCHUP;
        result.raft_meta.commit_index = local_last_good_index;
    }
    return Status::OK();
}

Status PersistEngine::read_wal_from_disk(const std::string& path,
                                         WalReadResult& result) const {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            LOG_DEBUG("errno == ENOENT");
            result = {};
            return Status::OK();
        }
        return Status::ERROR("fd < 0");
    }
    auto fd_guard = Defer([fd]() { ::close(fd); });

    result = {};
    LogIndex prev_index{0};
    bool has_prev_index{false};

    while (true) {
        const int64 record_start_offset = result.last_good_offset;

        LogEntry entry;
        size_t consumed_bytes{0};
        Status read_status = FramedRecord<WalEntryCodec>::decode_from_fd(
            fd, entry, consumed_bytes);
        if (read_status.code() == StatusCode::GET_EOF) {
            struct stat st{};
            if (::fstat(fd, &st) == 0 &&
                static_cast<int64>(st.st_size) > result.last_good_offset) {
                result.error = true;
                result.error_msg = "partial wal record";
            }
            break;
        }
        if (read_status.code() == StatusCode::PARTIAL_READ) {
            result.error = true;
            result.error_msg = "partial wal record";
            break;
        }
        if (read_status.fail()) {
            result.error = true;
            result.error_msg = read_status.msg().empty()
                                   ? "wal record decode failure"
                                   : read_status.msg();
            break;
        }
        LOG_DEBUG("entry term:{}, index:{}, op_type:{} ", entry.term,
                  entry.index, static_cast<int32>(entry.op_type))
        if (has_prev_index && entry.index != prev_index + 1) {
            result.error = true;
            result.error_msg =
                fmt::format("wal index is not continuous, prev={}, current={}",
                            prev_index, entry.index);
            break;
        }
        LOG_DEBUG("enry key:{}, value:{}", entry.key, entry.value)
        result.entries.push_back(std::move(entry));
        prev_index = result.entries.back().index;
        has_prev_index = true;
        result.last_good_index = prev_index;
        result.last_good_offset = record_start_offset + consumed_bytes;
    }
    return Status::OK();
}

}  // namespace adviskv::storage
