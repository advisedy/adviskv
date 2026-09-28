#include "meta/persist/meta_persist_engine.h"

#include <cerrno>
#include <filesystem>
#include <unordered_set>
#include <utility>

#include <fcntl.h>
#include <nlohmann/json.hpp>
#include <unistd.h>

#include "common/defer.h"
#include "common/define.h"
#include "common/func.h"
#include "common/shard_range.h"

namespace adviskv {
using Json = nlohmann::json;

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ReplicaID, table_id, shard_index, replica_seq)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ShardID, table_id, shard_index)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Endpoint, ip, port)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(HashRange, start, end)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ShardRange, shard_id, range)

template <typename T>
void to_json(Json& j, const IDAllocator<T>& allocator) {
    j = allocator.current_id();
}
template <typename T>
void from_json(const Json& j, IDAllocator<T>& allocator) {
    allocator = IDAllocator<T>(j.get<T>());
}
}  // namespace adviskv

namespace adviskv::meta {
// 字段只列一次，编码和解码都按结构体的层级处理。
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Database, db_id, name, zone)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(TableSpec, db_id, db_name, name, engine_type, shard_ranges, target_replica_count,
                                   resource_pool)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(TableState, desired, phase, operation_id, last_error_msg, create_ts, update_ts)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Table, table_id, spec, state)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ReplicaSpec, dc, assign_node_id, engine_type)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ReplicaState, desired, phase, update_ts)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Replica, replica_id, spec, state)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ReplicaGroup, shard_id, mode, target_replica_count, desired_members,
                                   observed_membership_term, observed_membership_leader, seq_allocator)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(MetaPersistedRecord, next_db_id, next_table_id, databases, tables, replicas,
                                   replica_groups)

namespace {
constexpr int64 K_MAX_META_JSON_BYTES = 256 * 1024 * 1024;

Status validate_record(const MetaPersistedRecord& record) {
    std::unordered_set<DatabaseID> database_ids;
    for (const auto& db : record.databases) {
        if (!database_ids.insert(db.db_id).second) return Status::ERROR("duplicate database ID");
    }

    std::unordered_set<TableID> table_ids;
    for (const auto& table : record.tables) {
        if (!table_ids.insert(table.table_id).second) return Status::ERROR("duplicate table ID");
    }

    std::unordered_set<ReplicaID, ReplicaIDHash> replica_ids;
    for (const auto& replica : record.replicas) {
        if (!replica_ids.insert(replica.replica_id).second) return Status::ERROR("duplicate replica ID");
    }

    std::unordered_set<ShardID, ShardIDHash> replica_group_ids;
    for (const auto& group : record.replica_groups) {
        if (!replica_group_ids.insert(group.shard_id).second) return Status::ERROR("duplicate replica group ID");
    }
    return Status::OK();
}

}  // namespace

MetaPersistEngine::MetaPersistEngine(const std::string& data_dir)
        : data_dir_(data_dir), meta_path_(data_dir_ + "/meta.json") {}

Status MetaPersistEngine::init() {
    if (data_dir_.empty()) return Status::INVALID_ARGUMENT("meta persist engine data_dir is empty");
    std::error_code ec;
    std::filesystem::create_directories(data_dir_, ec);
    if (ec) {
        return Status::ERROR(fmt::format("failed to create meta data dir: {}, error: {}", data_dir_, ec.message()));
    }
    init_flag_ = true;
    return Status::OK();
}

Status MetaPersistEngine::save_meta(const MetaPersistedRecord& record) {
    if (!init_flag_) return Status::NOT_INIT("meta persist engine is not init");
    RETURN_IF_INVALID_STATUS(validate_record(record))
    std::string payload;
    try {
        payload = Json(record).dump(2);
    } catch (const std::exception& e) {
        return Status::ERROR(fmt::format("invalid metadata: {}", e.what()));
    }
    if (static_cast<int64>(payload.size()) > K_MAX_META_JSON_BYTES) {
        return Status::ERROR(fmt::format("meta.json too large: {}", payload.size()));
    }

    return func::atomic_replace_file(meta_path_,
                                     [&](int fd) { return func::write_full(fd, payload.data(), payload.size()); });
}

Status MetaPersistEngine::load_meta(MetaPersistedRecord& record) {
    if (!init_flag_) return Status::NOT_INIT("meta persist engine is not init");
    record = {};
    int fd = ::open(meta_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) return Status::OK();
        return Status::ERROR(fmt::format("failed to open meta.json: {}", meta_path_));
    }
    auto fd_guard = Defer([fd]() { ::close(fd); });
    std::string payload;
    char buf[4096];
    while (true) {
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n == 0) break;
        if (n < 0) {
            if (errno == EINTR) continue;
            return Status::ERROR("failed to read meta.json");
        }
        payload.append(buf, static_cast<size_t>(n));
        if (static_cast<int64>(payload.size()) > K_MAX_META_JSON_BYTES) {
            return Status::ERROR("meta.json too large");
        }
    }
    if (payload.empty()) return Status::ERROR("meta.json is empty");
    Json root;
    try {
        root = Json::parse(payload);
        root.get_to(record);
        return validate_record(record);
    } catch (const std::exception& e) {
        return Status::ERROR(fmt::format("invalid meta.json: {}", e.what()));
    }
}

}  // namespace adviskv::meta
