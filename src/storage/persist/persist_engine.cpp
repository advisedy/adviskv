#include "storage/persist/persist_engine.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <fmt/format.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/buffer.h"
#include "common/crash_injection.h"
#include "common/defer.h"
#include "common/define.h"

#include "common/framed_record_codec.h"
#include "common/func.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/raft/state_machine/state_machine.h"
namespace adviskv::storage {

namespace {

constexpr int32 K_MAX_WAL_ENTRY_PAYLOAD_BYTES = 64 * 1024 * 1024;
constexpr int64 K_MAX_RAFT_META_PAYLOAD_BYTES = 1024 * 1024;

void encode_replica_id(const ReplicaID& replica_id, EncodeBuffer& buf) {
    buf.write(replica_id.table_id);
    buf.write(replica_id.shard_index);
    buf.write(replica_id.replica_seq);
}

Status decode_replica_id(DecodeBuffer& buf, ReplicaID& replica_id) {
    RETURN_IF_INVALID_READ(buf, replica_id.table_id)
    RETURN_IF_INVALID_READ(buf, replica_id.shard_index)
    RETURN_IF_INVALID_READ(buf, replica_id.replica_seq)
    return Status::OK();
}

void encode_endpoint(const Endpoint& endpoint, EncodeBuffer& buf) {
    buf.write(endpoint.ip);
    buf.write(endpoint.port);
}

Status decode_endpoint(DecodeBuffer& buf, Endpoint& endpoint) {
    RETURN_IF_INVALID_READ(buf, endpoint.ip)
    RETURN_IF_INVALID_READ(buf, endpoint.port)
    return Status::OK();
}

void encode_peer_member(const PeerMember& member, EncodeBuffer& buf) {
    buf.write(member.node_id);
    encode_replica_id(member.replica_id, buf);
    encode_endpoint(member.endpoint, buf);
}

Status decode_peer_member(DecodeBuffer& buf, PeerMember& member) {
    RETURN_IF_INVALID_READ(buf, member.node_id)
    RETURN_IF_INVALID_STATUS(decode_replica_id(buf, member.replica_id))
    RETURN_IF_INVALID_STATUS(decode_endpoint(buf, member.endpoint))
    return Status::OK();
}

Status write_replica_id_to_fd(int fd, const ReplicaID& replica_id) {
    RETURN_IF_INVALID_STATUS(func::write_value(fd, replica_id.table_id))
    RETURN_IF_INVALID_STATUS(func::write_value(fd, replica_id.shard_index))
    RETURN_IF_INVALID_STATUS(func::write_value(fd, replica_id.replica_seq))
    return Status::OK();
}

Status read_replica_id_from_fd(int fd, ReplicaID& replica_id) {
    RETURN_IF_INVALID_STATUS(func::read_value(fd, replica_id.table_id))
    RETURN_IF_INVALID_STATUS(func::read_value(fd, replica_id.shard_index))
    RETURN_IF_INVALID_STATUS(func::read_value(fd, replica_id.replica_seq))
    return Status::OK();
}

Status write_endpoint_to_fd(int fd, const Endpoint& endpoint) {
    RETURN_IF_INVALID_STATUS(func::write_string(fd, endpoint.ip))
    RETURN_IF_INVALID_STATUS(func::write_value(fd, endpoint.port))
    return Status::OK();
}

Status read_endpoint_from_fd(int fd, Endpoint& endpoint) {
    RETURN_IF_INVALID_STATUS(func::read_string(fd, endpoint.ip))
    RETURN_IF_INVALID_STATUS(func::read_value(fd, endpoint.port))
    return Status::OK();
}

Status write_peer_member_to_fd(int fd, const PeerMember& member) {
    RETURN_IF_INVALID_STATUS(func::write_string(fd, member.node_id))
    RETURN_IF_INVALID_STATUS(write_replica_id_to_fd(fd, member.replica_id))
    RETURN_IF_INVALID_STATUS(write_endpoint_to_fd(fd, member.endpoint))
    return Status::OK();
}

Status read_peer_member_from_fd(int fd, PeerMember& member) {
    RETURN_IF_INVALID_STATUS(func::read_string(fd, member.node_id))
    RETURN_IF_INVALID_STATUS(read_replica_id_from_fd(fd, member.replica_id))
    RETURN_IF_INVALID_STATUS(read_endpoint_from_fd(fd, member.endpoint))
    return Status::OK();
}

Status write_raft_member_to_fd(int fd, const RaftMember& member) {
    RETURN_IF_INVALID_STATUS(write_peer_member_to_fd(fd, member.peer))
    RETURN_IF_INVALID_STATUS(func::write_value<int32>(fd, static_cast<int32>(member.member_type)))
    return Status::OK();
}

Status read_raft_member_from_fd(int fd, RaftMember& member) {
    RETURN_IF_INVALID_STATUS(read_peer_member_from_fd(fd, member.peer))
    int32 member_type{0};
    RETURN_IF_INVALID_STATUS(func::read_value(fd, member_type))
    RETURN_IF_INVALID_CONDITION(is_valid_enum_value<RaftMemberType>(member_type), "invalid raft member type")
    member.member_type = static_cast<RaftMemberType>(member_type);
    return Status::OK();
}

// WAL 是外部输入（可能损坏 / 被篡改），入口处必须做一次枚举范围检查。
bool is_valid_wal_op_type(int32 op_type) { return is_valid_enum_value<WriteOpType>(op_type); }

class WalEntryCodec {
public:
    using ObjectType = LogEntry;
    using LenType = int32;

    LenType max_payload_len() const { return K_MAX_WAL_ENTRY_PAYLOAD_BYTES; }

    void encode_payload(const ObjectType& entry, EncodeBuffer& buf) const {
        buf.write(entry.term);
        buf.write(entry.index);
        buf.write(static_cast<int32>(entry.op_type));
        buf.write(entry.key);
        buf.write(entry.value);
        buf.write(entry.expected);
        encode_peer_member(entry.config_member, buf);
        encode_replica_id(entry.config_replica_id, buf);
        buf.write(entry.request_id);
    }

    Status decode_payload(DecodeBuffer& buf, ObjectType& entry) const {
        int32 op_type{0};
        RETURN_IF_INVALID_READ(buf, entry.term)
        RETURN_IF_INVALID_READ(buf, entry.index)
        RETURN_IF_INVALID_READ(buf, op_type)
        RETURN_IF_INVALID_READ(buf, entry.key)
        RETURN_IF_INVALID_READ(buf, entry.value)
        RETURN_IF_INVALID_READ(buf, entry.expected)

        if (!is_valid_wal_op_type(op_type)) {
            return Status::ERROR(fmt::format("invalid wal op_type: {}", op_type));
        }
        entry.op_type = static_cast<WriteOpType>(op_type);
        RETURN_IF_INVALID_STATUS(decode_peer_member(buf, entry.config_member))
        RETURN_IF_INVALID_STATUS(decode_replica_id(buf, entry.config_replica_id))
        RETURN_IF_INVALID_READ(buf, entry.request_id)
        return Status::OK();
    }
};

class RaftMetaCodec {
public:
    using ObjectType = RaftMeta;
    using LenType = int64;

    LenType max_payload_len() const { return K_MAX_RAFT_META_PAYLOAD_BYTES; }

    void encode_payload(const ObjectType& meta, EncodeBuffer& buf) const {
        buf.write(meta.current_term);
        buf.write<bool>(meta.voted_for.has_value());
        if (meta.voted_for.has_value()) {
            encode_replica_id(meta.voted_for.value(), buf);
        }
    }

    Status decode_payload(DecodeBuffer& buf, ObjectType& meta) const {
        meta = {};
        RETURN_IF_INVALID_READ(buf, meta.current_term)
        bool has_voted_for{false};
        RETURN_IF_INVALID_READ(buf, has_voted_for)
        if (has_voted_for) {
            ReplicaID replica_id;
            RETURN_IF_INVALID_STATUS(decode_replica_id(buf, replica_id))
            meta.voted_for = replica_id;
        }
        return Status::OK();
    }
};

}  // namespace

// ===== 磁盘布局单一来源（批次 2.2）=====
// init() 与所有外部探盘（Replica::init 的 has_persisted_state）共用这一份
// 布局：data_dir/<table>-<shard>/{wal.log, raft_meta, snapshot}。
std::string PersistEngine::replica_dir_name(const ReplicaID& replica_id) {
    return std::to_string(replica_id.table_id) + "-" + std::to_string(replica_id.shard_index);
}

std::filesystem::path PersistEngine::replica_dir(const std::string& data_dir, const ReplicaID& replica_id) {
    return std::filesystem::path(data_dir) / replica_dir_name(replica_id);
}

std::filesystem::path PersistEngine::wal_file(const std::filesystem::path& dir) { return dir / "wal.log"; }

std::filesystem::path PersistEngine::raft_meta_file(const std::filesystem::path& dir) { return dir / "raft_meta"; }

std::filesystem::path PersistEngine::snapshot_file(const std::filesystem::path& dir) { return dir / "snapshot"; }

PersistEngine::PersistEngine(const std::string& data_dir, const ReplicaID& replica_id)
        : data_dir_(data_dir), replica_id_(replica_id) {}

PersistEngine::~PersistEngine() {
    Status status = close();
    if (status.fail()) {
        LOG_WARN("[PersistEngine] ~PersistEngine, close failed, status:{}", status.to_string());
    }
}

Status PersistEngine::init() {
    if (data_dir_.empty()) {
        return Status::INVALID_ARGUMENT("persist engine init error: data_dir is epmty");
    }
    if (ReplicaID& id = replica_id_; id.replica_seq == -1 or id.shard_index == -1 or id.table_id == -1) {
        return Status::INVALID_ARGUMENT(
                fmt::format("persist engine init error: replica_id:{} invalid", replica_id_.to_string()));
    }
    // 路径拼装全部走静态布局函数（磁盘布局单一来源，批次 2.2）。
    const std::filesystem::path dir = replica_dir(data_dir_, replica_id_);
    wal_path_ = wal_file(dir).string();
    raft_meta_path_ = raft_meta_file(dir).string();
    snapshot_path_ = snapshot_file(dir).string();
    dir_path_ = dir.string();
    LOG_DEBUG("[PersistEngine] init, dir_path_={}, wal_path_={}, raft_meta_path_={}, snapshot_path_={}", dir_path_,
              wal_path_, raft_meta_path_, snapshot_path_);
    try {
        std::filesystem::create_directories(dir_path_);
    } catch (const std::exception& e) {
        LOG_WARN("[PersistEngine] init, persiste create directories failed: {}", dir_path_);
    }
    // mkdir(dir_path_.c_str(), 0755);  // 改成create_dircation

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
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_entry", static_cast<int64_t>(entries.size()));

    std::unique_lock lock{mutex_};
    for (const LogEntry& entry : entries) {
        Status status = write_wal_to_disk(wal_fd_, entry);
        if (status.fail()) {
            ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_failure");
            return status;
        }
    }
    if (::fsync(wal_fd_) != 0) {
        ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_fsync_failure");
        return Status::ERROR("fsync != 0");
    }
    ADVISKV_METRICS_COUNTER("storage_persist_append_wal_batch_success");
    return Status::OK();
}

Status PersistEngine::read_wal_batch(std::vector<LogEntry>& entries) {
    std::unique_lock lock{mutex_};
    return read_wal_batch_unlocked(entries);
}

Status PersistEngine::rewrite_wal(const std::vector<LogEntry>& entries) {
    ADVISKV_METRICS_TIMER("storage_persist_rewrite_wal");
    std::unique_lock lock{mutex_};
    return rewrite_wal_unlocked(entries);
}

Status PersistEngine::read_wal_batch_unlocked(std::vector<LogEntry>& entries) const {
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
    return truncate_wal_unlocked(snapshot_index);
}

Status PersistEngine::truncate_wal_unlocked(const LogIndex& snapshot_index) {
    std::vector<LogEntry> entries, remain;
    RETURN_IF_INVALID_STATUS(read_wal_batch_unlocked(entries))

    for (LogEntry& entry : entries) {
        if (entry.index <= snapshot_index) continue;
        remain.push_back(std::move(entry));
    }

    return rewrite_wal_unlocked(remain);
}

Status PersistEngine::rewrite_wal_unlocked(const std::vector<LogEntry>& entries) {
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

    {
        ADVISKV_METRICS_TIMER("storage_persist_rewrite_wal_write");
        for (const LogEntry& entry : entries) {
            RETURN_IF_INVALID_STATUS(write_wal_to_disk(fd, entry))
        }
    }

    {
        ADVISKV_METRICS_TIMER("storage_persist_rewrite_wal_fsync");
        if (::fsync(fd) != 0) {
            return Status::ERROR("failed to fsync wal tmp file");
        }
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
    {
        ADVISKV_METRICS_TIMER("storage_persist_rewrite_wal_dir_fsync");
        RETURN_IF_INVALID_STATUS(func::fsync_dir(dir_path_))
    }

    wal_fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (wal_fd_ < 0) {
        return Status::ERROR("truncate wal error");
    }

    return Status::OK();
}

Status PersistEngine::read_snapshot_header(int fd, Snapshot* snapshot, int32& kv_count) {
    int64 payload_len{0};
    RETURN_IF_INVALID_STATUS(func::read_value(fd, payload_len))
    if (payload_len <= 0) {
        return Status::ERROR("invalid snapshot payload_len");
    }

    LogIndex apply_index{0};
    Term apply_term{0};
    RETURN_IF_INVALID_STATUS(func::read_value(fd, apply_index))
    RETURN_IF_INVALID_STATUS(func::read_value(fd, apply_term))

    int32 member_count{0};
    RETURN_IF_INVALID_STATUS(func::read_value(fd, member_count))
    if (member_count < 0) {
        return Status::ERROR("invalid snapshot member_count");
    }

    std::vector<RaftMember> members;
    members.reserve(member_count);

    for (int i = 0; i < member_count; i++) {
        RaftMember member;
        RETURN_IF_INVALID_STATUS(read_raft_member_from_fd(fd, member))
        members.push_back(std::move(member));
    }

    RETURN_IF_INVALID_STATUS(func::read_value(fd, kv_count))
    if (kv_count < 0) {
        return Status{StatusCode::ERROR, "invalid kv_count"};
    }
    if (snapshot) {
        snapshot->apply_index = apply_index;
        snapshot->apply_term = apply_term;
        snapshot->members = std::move(members);
    }
    return Status::OK();
}

std::string PersistEngine::session_snapshot_tmp_path(const InstallSnapshotParam& param) const {
    // dir/snapshot.<from_seq>.<term>.<index>.<nonce>.tmp：四元组 + nonce
    // 唯一确定一个接收会话。不同会话（不同 leader / term / index）永不
    // 共享文件，本地快照路径族也永不与之重合（批次 B 治 F5）。
    std::string nonce = param.session_nonce;
    if (nonce.empty()) {
        // 兼容无 nonce 的调用（单元测试直接构造）：退化为来源+term+index，
        // 接收侧 receive_mutex_ 保证同会话串行，仍与本地文件分家。
        nonce = "direct";
    }
    return dir_path_ + "/snapshot." + std::to_string(param.from_replica_id.replica_seq) + "." +
           std::to_string(param.term) + "." + std::to_string(param.snapshot_index) + "." + nonce + ".tmp";
}

Status PersistEngine::open_snapshot_reader_any(std::unique_ptr<SnapshotFileReader>& reader, bool& exists) const {
    reader.reset();
    exists = false;
    std::lock_guard lock(snapshot_mutex_);
    int fd = ::open(snapshot_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return Status::OK();
        }
        return Status::ERROR("failed to open snapshot for reading");
    }
    reader = std::make_unique<SnapshotFileReader>(fd);
    exists = true;
    return Status::OK();
}

Status PersistEngine::write_snapshot_to_fd(int fd, const SnapshotData& snapshot,
                                           const std::vector<RaftMember>& members) {
    // payload_len + apply_index + apply_term + member_count +
    // [member...] + kv_count + [k1, v1] + request_count + [request records]
    LogIndex apply_index = snapshot.apply_index;
    Term apply_term = snapshot.apply_term;

    RETURN_IF_INVALID_STATUS(func::write_value<int64>(fd, 0))
    RETURN_IF_INVALID_STATUS(func::write_value(fd, apply_index))
    RETURN_IF_INVALID_STATUS(func::write_value(fd, apply_term))
    RETURN_IF_INVALID_STATUS(func::write_value<int32>(fd, static_cast<int32>(members.size())))
    for (const RaftMember& member : members) {
        RETURN_IF_INVALID_STATUS(write_raft_member_to_fd(fd, member))
    }

    off_t kv_count_pos = ::lseek(fd, 0, SEEK_CUR);
    if (kv_count_pos < 0) {
        return Status::ERROR("lseek snapshot kv_count pos failed");
    }
    RETURN_IF_INVALID_STATUS(func::write_value<int32>(fd, 0))

    int32 kv_count = 0;
    for (const auto& [k, v] : snapshot.kvs) {
        RETURN_IF_INVALID_STATUS(func::write_string(fd, k))
        RETURN_IF_INVALID_STATUS(func::write_string(fd, v))
        ++kv_count;
    }

    off_t request_count_pos = ::lseek(fd, 0, SEEK_CUR);
    if (request_count_pos < 0) {
        return Status::ERROR("lseek snapshot request_count pos failed");
    }
    RETURN_IF_INVALID_STATUS(func::write_value<int32>(fd, 0))

    int32 request_count = 0;
    for (const RequestRecord& record : snapshot.request_records) {
        if (record.request_id.empty()) {
            return Status::INVALID_ARGUMENT("cannot snapshot an empty request_id");
        }
        RETURN_IF_INVALID_STATUS(func::write_string(fd, record.request_id))
        RETURN_IF_INVALID_STATUS(func::write_value<int32>(fd, static_cast<int32>(record.op)))
        RETURN_IF_INVALID_STATUS(func::write_value(fd, record.payload_hash))
        RETURN_IF_INVALID_STATUS(func::write_value<int32>(fd, static_cast<int32>(record.apply_status.code())))
        RETURN_IF_INVALID_STATUS(func::write_string(fd, record.apply_status.msg()))
        ++request_count;
    }
    ADVISKV_METRICS_COUNTER("storage_persist_snapshot_request_records_written", request_count);

    off_t end_pos = ::lseek(fd, 0, SEEK_END);
    if (end_pos < 0) {
        return Status::ERROR("lseek snapshot end failed");
    }
    ADVISKV_METRICS_COUNTER("storage_persist_snapshot_bytes_written", static_cast<int64_t>(end_pos));

    int64 payload_len = static_cast<int64>(end_pos) - sizeof(int64);  // 这个len是没有算上自己的

    if (::lseek(fd, 0, SEEK_SET) < 0) {
        return Status::ERROR("lseek snapshot payload_len failed");
    }
    RETURN_IF_INVALID_STATUS(func::write_value(fd, payload_len))

    if (::lseek(fd, kv_count_pos, SEEK_SET) < 0) {
        return Status::ERROR("lseek snapshot kv_count failed");
    }
    RETURN_IF_INVALID_STATUS(func::write_value(fd, kv_count))

    if (::lseek(fd, request_count_pos, SEEK_SET) < 0) {
        return Status::ERROR("lseek snapshot request_count failed");
    }
    RETURN_IF_INVALID_STATUS(func::write_value(fd, request_count))

    return Status::OK();
}

Status PersistEngine::load_snapshot_meta(SnapshotPtr& snapshot) const {
    // recover / 测试用的元数据读取，同样走持 fd 的 reader（安全条件 6 的
    // 载体）：打开即锁定一个文件版本，后续 rename/替换不影响本次读取。
    std::unique_ptr<SnapshotFileReader> reader;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(open_snapshot_reader_any(reader, exists))
    if (!exists) {
        snapshot.reset();
        return Status::OK();
    }

    if (!snapshot) {
        snapshot = std::make_shared<Snapshot>();
    }
    int32 kv_count{0};
    RETURN_IF_INVALID_STATUS(reader->read_header(snapshot.get(), kv_count))
    UNUSED(kv_count);
    return Status::OK();
}

Status PersistEngine::for_each_snapshot_kv(const KvVisitor& fn) const {
    if (!fn) {
        return Status::INVALID_ARGUMENT("kv visitor is empty");
    }

    std::unique_ptr<SnapshotFileReader> reader;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(open_snapshot_reader_any(reader, exists))
    if (!exists) {
        // 保持历史行为：无快照文件视为空。
        return Status::OK();
    }

    int32 kv_count{0};
    RETURN_IF_INVALID_STATUS(reader->read_header(nullptr, kv_count))
    return reader->for_each_kv(kv_count, fn);
}

Status PersistEngine::for_each_snapshot_request(const RequestVisitor& fn) const {
    if (!fn) {
        return Status::INVALID_ARGUMENT("request visitor is empty");
    }

    std::unique_ptr<SnapshotFileReader> reader;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(open_snapshot_reader_any(reader, exists))
    if (!exists) {
        return Status::OK();
    }

    // header 未预留 kv_count/request_count 的偏移，读 header 时顺带解析两者；
    // 后续格式演进时值得在 header 里显式留 off，省掉这次线性读。
    int32 kv_count{0};
    RETURN_IF_INVALID_STATUS(reader->read_header(nullptr, kv_count))
    for (int i = 0; i < kv_count; i++) {
        Key key;
        Value value;
        RETURN_IF_INVALID_STATUS(func::read_string(reader->fd(), key))
        RETURN_IF_INVALID_STATUS(func::read_string(reader->fd(), value))
    }

    return reader->for_each_request(fn);
}

Status PersistEngine::SnapshotFileReader::read_header(Snapshot* snapshot, int32& kv_count) const {
    if (fd_ == -1) {
        return Status::ERROR("snapshot reader is not open");
    }
    return PersistEngine::read_snapshot_header(fd_, snapshot, kv_count);
}

Status PersistEngine::SnapshotFileReader::for_each_kv(int32 kv_count, const KvVisitor& fn) const {
    if (fd_ == -1) {
        return Status::ERROR("snapshot reader is not open");
    }
    for (int i = 0; i < kv_count; i++) {
        Key key;
        Value value;
        RETURN_IF_INVALID_STATUS(func::read_string(fd_, key))
        RETURN_IF_INVALID_STATUS(func::read_string(fd_, value))
        RETURN_IF_INVALID_STATUS(fn(key, value))
    }
    return Status::OK();
}

Status PersistEngine::SnapshotFileReader::for_each_request(const RequestVisitor& fn) const {
    if (fd_ == -1) {
        return Status::ERROR("snapshot reader is not open");
    }
    int32 request_count{0};
    RETURN_IF_INVALID_STATUS(func::read_value(fd_, request_count))
    if (request_count < 0) {
        return Status::ERROR("invalid snapshot request_count");
    }
    for (int i = 0; i < request_count; i++) {
        RequestRecord record;
        int32 op_type{0};
        RETURN_IF_INVALID_STATUS(func::read_string(fd_, record.request_id))
        RETURN_IF_INVALID_STATUS(func::read_value(fd_, op_type))
        if (!is_valid_wal_op_type(op_type) || !is_kv_write_op(static_cast<WriteOpType>(op_type))) {
            return Status::ERROR("invalid snapshot request op_type");
        }
        record.op = static_cast<WriteOpType>(op_type);
        RETURN_IF_INVALID_STATUS(func::read_value(fd_, record.payload_hash))
        int32 apply_status{0};
        std::string apply_status_message;
        RETURN_IF_INVALID_STATUS(func::read_value(fd_, apply_status))
        RETURN_IF_INVALID_STATUS(func::read_string(fd_, apply_status_message))
        record.apply_status = Status{apply_status, apply_status_message};
        RETURN_IF_INVALID_STATUS(fn(record))
    }
    return Status::OK();
}

Status PersistEngine::SnapshotFileReader::read_chunk(uint64 offset, size_t max_bytes, std::string& data,
                                                     bool& eof) const {
    if (fd_ == -1) {
        return Status::ERROR("snapshot reader is not open");
    }
    data.clear();
    eof = false;
    // fstat 而非 stat：数据尺寸以已打开 inode 为准（治 F6 的另一半——
    // offset 边界也绑定同一文件版本）。
    struct stat st{};
    if (::fstat(fd_, &st) != 0) {
        return Status::ERROR("failed to stat snapshot file");
    }
    if (offset > static_cast<uint64>(st.st_size)) {
        return Status::ERROR("snapshot chunk offset out of range");
    }

    if (::lseek(fd_, static_cast<off_t>(offset), SEEK_SET) < 0) {
        return Status::ERROR("failed to seek snapshot file");
    }

    size_t read_len = std::min(max_bytes, static_cast<size_t>(st.st_size - offset));
    data.resize(read_len);
    if (read_len > 0) {
        RETURN_IF_INVALID_STATUS(func::read_full(fd_, data.data(), read_len))
    }
    eof = offset + read_len >= static_cast<uint64>(st.st_size);
    return Status::OK();
}

Status PersistEngine::open_snapshot_reader(LogIndex expected_index, Term expected_term,
                                           std::unique_ptr<SnapshotFileReader>& reader) const {
    reader.reset();
    // snapshot_mutex_ 只需护住 open：fd 一旦到手，inode 语义保证后续
    // rename/删除不影响读取（批次 B 治 F6 的机制基础）。
    std::lock_guard lock(snapshot_mutex_);
    int fd = ::open(snapshot_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::ERROR("failed to open snapshot for reading");
    }
    reader = std::make_unique<SnapshotFileReader>(fd);

    // 安全条件 5：发送参数与打开的文件头必须匹配。打开后立刻校验
    // (index, term)：不匹配（文件已被更新的快照替换）时取消本次发送，
    // 调用方按发送失败处理并清理 inflight。
    Snapshot actual;
    int32 kv_count{0};
    Status status = reader->read_header(&actual, kv_count);
    if (status.fail()) {
        reader.reset();
        return status;
    }
    if (actual.apply_index != expected_index || actual.apply_term != expected_term) {
        reader.reset();
        LOG_WARN(
                "[PersistEngine] snapshot header mismatch, expected:{}:{}, actual:{}:{}",
                expected_index, expected_term, actual.apply_index, actual.apply_term);
        return Status::CONDITION_NOT_MET(
                fmt::format("snapshot header mismatch, expected index:{} term:{}, actual index:{} term:{}",
                            expected_index, expected_term, actual.apply_index, actual.apply_term));
    }
    return Status::OK();
}

Status PersistEngine::append_snapshot_chunk(const InstallSnapshotParam& param) {
    std::lock_guard lock(snapshot_mutex_);
    // 批次 B 治 F5：接收会话用独立临时文件（来源+term+index+nonce 命名），
    // 与本地候选文件（snapshot.<index>.tmp）和本地写盘（snapshot.tmp）彻底
    // 分家——任何一方都不可能 O_TRUNC / rename 走对方的文件。
    const std::string session_path = session_snapshot_tmp_path(param);

    int flags = O_WRONLY | O_CREAT;
    flags |= (param.offset == 0) ? O_TRUNC : O_APPEND;
    int fd = ::open(session_path.c_str(), flags, 0644);
    if (fd < 0) {
        return Status::ERROR("failed to open snapshot session tmp file");
    }
    auto fd_guard = Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        return Status::ERROR("failed to stat snapshot session tmp file");
    }
    if (static_cast<uint64>(st.st_size) != param.offset) {
        return Status::ERROR("snapshot chunk offset mismatch");
    }
    if (!param.data.empty()) {
        RETURN_IF_INVALID_STATUS(func::write_full(fd, param.data.data(), param.data.size()))
    }
    if (::fsync(fd) != 0) {
        return Status::ERROR("failed to fsync snapshot session tmp file");
    }
    return Status::OK();
}

// 在快照的chunk都发送完了之后会调用这个
Status PersistEngine::finish_snapshot_receive(const InstallSnapshotParam& param) {
    std::lock_guard lock(snapshot_mutex_);
    // 契约（F11）：此处先发布（rename 进 snapshot），WAL 裁剪在之后的
    // RestoreInstallSnapshotCall 里进行。崩溃窗口内 snapshot 新、WAL 旧，
    // recover 按 snapshot 边界过滤 WAL 前缀，状态自洽。
    const std::string session_path = session_snapshot_tmp_path(param);
    if (::rename(session_path.c_str(), snapshot_path_.c_str()) != 0) {
        return Status::ERROR("failed to publish received snapshot");
    }
    RETURN_IF_INVALID_STATUS(func::fsync_dir(dir_path_))
    // 崩溃切换点（方案 E §3-B7，F12 进程退出模式）：远端快照已发布、WAL
    // 未裁。崩溃窗口内 snapshot 新、WAL 旧，recover 过滤 WAL 前缀后必须
    // 自洽（persist_remote_publish_crash 测试钉住）。
    testhook::crash_point("persist.after_remote_snapshot_publish");

    // snap->path = snapshot_path_;
    return Status::OK();
}

// 只把状态机内容落成 snapshot 文件，不修改 WAL。
// 批次 B 注：这是"直接发布"版本，供独立 PersistEngine 使用方（recover 工具、
// 单元测试造源快照）使用。副本运行期本地快照走 write_local_snapshot_candidate
// + publish_local_snapshot 两段（候选文件 + loop 单调裁决），不再从这里过。
Status PersistEngine::write_snapshot(const SnapshotData& snapshot, const std::vector<RaftMember>& members) {
    ADVISKV_METRICS_COUNTER("storage_persist_write_snapshot_request");
    ADVISKV_METRICS_TIMER("storage_persist_write_snapshot");

    std::lock_guard lock(snapshot_mutex_);

    LogIndex apply_index = snapshot.apply_index;
    Term apply_term = snapshot.apply_term;

    LOG_DEBUG(
            "[PersistEngine] write_snapshot, replica_id:{}, persist engine start to write snapshot, "
            "snapshot_index:{}, "
            "snapshot_term:{}",
            replica_id_.to_string(), apply_index, apply_term);

    RETURN_IF_INVALID_STATUS(func::atomic_replace_file(snapshot_path_, [&](int fd) -> Status {
        return write_snapshot_to_fd(fd, snapshot, members);
    }))

    LOG_DEBUG(
            "replica_id:{}, persist engine finish write snapshot, "
            "snapshot_index:{}, "
            "snapshot_term:{}",
            replica_id_.to_string(), apply_index, apply_term);
    return Status::OK();
}

Status PersistEngine::write_snapshot(const StateMachine& state_machine, const std::vector<RaftMember>& members) {
    SnapshotData snapshot;
    RETURN_IF_INVALID_STATUS(state_machine.capture_snapshot(snapshot))
    return write_snapshot(snapshot, members);
}

Status PersistEngine::write_local_snapshot_candidate(const SnapshotData& snapshot,
                                                     const std::vector<RaftMember>& members, LogIndex capture_index) {
    ADVISKV_METRICS_COUNTER("storage_persist_write_snapshot_request");
    ADVISKV_METRICS_TIMER("storage_persist_write_snapshot");

    // 批次 B 三件套第 3 条：本地 worker 只写候选文件
    // snapshot.<capture_index>.tmp，绝不直接 rename 发布。发布与否由 loop
    // 在 SnapshotDoneEvent 上按 index 单调裁决（见 publish_local_snapshot）。
    // 捕获 index 唯一确定候选文件：同一 index 的重复捕获天然 O_TRUNC 覆盖
    // 自己（幂等），不同 index 的候选互不干扰。
    const std::string candidate_path =
            dir_path_ + "/snapshot." + std::to_string(capture_index) + ".tmp";

    int fd = ::open(candidate_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status::ERROR("failed to open local snapshot candidate file");
    }
    auto fd_guard = Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    RETURN_IF_INVALID_STATUS(write_snapshot_to_fd(fd, snapshot, members))

    if (::fsync(fd) != 0) {
        return Status::ERROR("failed to fsync local snapshot candidate file");
    }
    if (::close(fd) != 0) {
        fd = -1;
        return Status::ERROR("failed to close local snapshot candidate file");
    }
    fd = -1;
    return Status::OK();
}

Status PersistEngine::publish_local_snapshot(LogIndex capture_index, bool& published) {
    published = false;
    std::lock_guard lock(snapshot_mutex_);

    // 单调裁决：候选 index 必须严格大于当前已发布快照 index 才发布。当前
    // 已发布快照不存在（ENOENT）时视为 index 0，任何正 index 候选都可发布。
    LogIndex current_index{0};
    {
        int fd = ::open(snapshot_path_.c_str(), O_RDONLY);
        if (fd >= 0) {
            auto fd_guard = Defer([&fd]() {
                if (fd != -1) {
                    ::close(fd);
                    fd = -1;
                }
            });
            Snapshot current;
            int32 kv_count{0};
            Status status = read_snapshot_header(fd, &current, kv_count);
            if (status.fail()) {
                // 已发布文件损坏：不能因为读不出边界就拒绝发布候选——崩溃
                // 恢复场景下损坏的旧快照正是要被新候选替换的对象。按 0 处理。
                LOG_WARN(
                        "[PersistEngine] current snapshot header unreadable, treat index as 0, status:{}",
                        status.to_string());
            } else {
                current_index = current.apply_index;
            }
        }
    }

    const std::string candidate_path =
            dir_path_ + "/snapshot." + std::to_string(capture_index) + ".tmp";

    // 无论是否发布，候选文件都要收掉（三件套第 3 条：过期候选删除，绝不
    // 覆盖更新版本）。
    auto remove_candidate = Defer([&candidate_path]() {
        std::error_code ec;
        std::filesystem::remove(candidate_path, ec);
    });

    if (capture_index <= current_index) {
        LOG_INFO(
                "[PersistEngine] skip stale local snapshot publish, candidate_index:{}, current_index:{}",
                capture_index, current_index);
        return Status::OK();
    }

    // 契约（F11）：这里 rename 发布（快照先发布），WAL 裁剪由 loop 在发布
    // 成功后另行执行（SnapshotDoneEvent 的 truncate_wal），顺序不可倒置。
    if (::rename(candidate_path.c_str(), snapshot_path_.c_str()) != 0) {
        return Status::ERROR("failed to publish local snapshot candidate");
    }
    RETURN_IF_INVALID_STATUS(func::fsync_dir(dir_path_))
    published = true;
    // 崩溃切换点（方案 E §3-B7，F12 进程退出模式）：快照已发布、WAL 未裁。
    // 崩溃窗口内 snapshot 新、WAL 旧，recover 按 snapshot 边界过滤 WAL 前缀，
    // 状态必须自洽（persist_local_publish_crash 测试钉住）。
    testhook::crash_point("persist.after_local_snapshot_publish");
    LOG_INFO(
            "[PersistEngine] local snapshot published, index:{}, previous_index:{}",
            capture_index, current_index);
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

    RETURN_IF_INVALID_STATUS(FramedRecord<RaftMetaCodec>::encode_to_fd(fd, meta))

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

    return FramedRecord<RaftMetaCodec>::decode_from_fd(fd, meta);
}

// buf.len + crc + buf [term + index + op_type + key + value + config fields]
Status PersistEngine::write_wal_to_disk(int fd, const LogEntry& entry) {
    return FramedRecord<WalEntryCodec>::encode_to_fd(fd, entry);
}

Status PersistEngine::recover(RecoverResult& result) {
    std::unique_lock lock{mutex_};
    result = {};

    {  // 填充snapshot
        result.snapshot = std::make_shared<Snapshot>();
        RETURN_IF_INVALID_STATUS(load_snapshot_meta(result.snapshot))
    }

    // 填充raft_meta
    RETURN_IF_INVALID_STATUS(load_raft_meta(result.raft_meta))

    // 读取WAL
    WalReadResult wal_read_result;
    RETURN_IF_INVALID_STATUS(read_wal_from_disk(wal_path_, wal_read_result))

    LogIndex snapshot_index = result.snapshot ? result.snapshot->apply_index : 0;

    // 这里要处理一下这个entries，需要把快照之前的给去掉
    // 原因是因为，在快照落盘之后，截断WAL日志的落盘的之前，可能崩溃，那WAL的落盘文件就没有更改。
    // 所以我们在读取的时候手动这里再截取一下
    func::ad_erase_if(wal_read_result.entries,
                      [snapshot_index](const LogEntry& entry) { return entry.index <= snapshot_index; });

    if (!wal_read_result.entries.empty() && wal_read_result.entries.front().index != snapshot_index + 1) {
        // 说明WAL的内容和快照对不上，这种情况需要把快照之后的WAL都清掉
        // 否则下次recover还会读到同样的gap。
        result.wal_entries.clear();
        result.need_recover = true;
        LOG_WARN(
                "wal gap after snapshot during recover, snapshot_index={}, "
                "first_wal_index={}",
                snapshot_index, wal_read_result.entries.front().index);
        RETURN_IF_INVALID_STATUS(rewrite_wal_unlocked(result.wal_entries))
        return Status::OK();
    }

    result.wal_entries = std::move(wal_read_result.entries);

    if (wal_read_result.error) {
        result.need_recover = true;
        LOG_WARN("wal corrupted during recover, reason={}", wal_read_result.error_msg);
        RETURN_IF_INVALID_STATUS(rewrite_wal_unlocked(result.wal_entries))
        return Status::OK();
    }

    // 即使WAL本身完整，也可能在snapshot发布后、WAL重写前崩溃。
    // 这里用snapshot index重写WAL，丢弃已经被snapshot覆盖的前缀。
    RETURN_IF_INVALID_STATUS(truncate_wal_unlocked(snapshot_index))
    LOG_DEBUG("persist engine recover ok.");
    return Status::OK();
}

Status PersistEngine::read_wal_from_disk(const std::string& path, WalReadResult& result) const {
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
        Status read_status = FramedRecord<WalEntryCodec>::decode_from_fd(fd, entry, consumed_bytes);
        if (read_status.code() == StatusCode::GET_EOF) {
            struct stat st{};
            if (::fstat(fd, &st) == 0 && static_cast<int64>(st.st_size) > result.last_good_offset) {
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
            result.error_msg = read_status.msg().empty() ? "wal record decode failure" : read_status.msg();
            break;
        }
        LOG_DEBUG("entry term:{}, index:{}, op_type:{} ", entry.term, entry.index, static_cast<int32>(entry.op_type))
        if (has_prev_index && entry.index != prev_index + 1) {
            result.error = true;
            result.error_msg = fmt::format("wal index is not continuous, prev={}, current={}", prev_index, entry.index);
            break;
        }
        LOG_DEBUG("enry key:{}, value:{}", entry.key, entry.value)
        result.entries.push_back(std::move(entry));
        prev_index = result.entries.back().index;
        has_prev_index = true;
        result.last_good_offset = record_start_offset + consumed_bytes;
    }
    return Status::OK();
}

}  // namespace adviskv::storage
