#include "meta/persist/meta_persist_engine.h"

#include <algorithm>
#include <filesystem>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

#include "common/define.h"
#include "common/file.h"
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
namespace enum_codec {
using Json = nlohmann::json;

template <typename E>
struct EnumTable {
    std::unordered_map<std::string, E> by_name;
};

template <typename E>
inline E decode(const Json& j, const EnumTable<E>& table, const char* what) {
    if (!j.is_string()) {
        throw Json::type_error::create(302, fmt::format("{} must be string in meta.json", what), nullptr);
    }
    const std::string name = j.get<std::string>();
    auto it = table.by_name.find(name);
    if (it == table.by_name.end()) {
        throw Json::type_error::create(302, fmt::format("unknown {} in meta.json: {}", what, name), nullptr);
    }
    return it->second;
}

inline void encode(Json& j, const std::string& name) { j = name; }

const EnumTable<TablePhase>& table_phase_table() {
    static const EnumTable<TablePhase> table{
            {{"CREATING", TablePhase::CREATING},
             {"READY", TablePhase::READY},
             {"DELETING", TablePhase::DELETING},
             {"DELETED", TablePhase::DELETED},
             {"RESIZING", TablePhase::RESIZING}},
    };
    return table;
}

const EnumTable<ReplicaPhase>& replica_phase_table() {
    static const EnumTable<ReplicaPhase> table{
            {{"CREATING", ReplicaPhase::CREATING},
             {"READY", ReplicaPhase::READY},
             {"LOST", ReplicaPhase::LOST},
             {"ERROR", ReplicaPhase::ERROR},
             {"DELETING", ReplicaPhase::DELETING},
             {"DELETED", ReplicaPhase::DELETED}},
    };
    return table;
}

const EnumTable<ReplicaGroupMode>& group_mode_table() {
    static const EnumTable<ReplicaGroupMode> table{
            {{"BOOTSTRAP", ReplicaGroupMode::BOOTSTRAP}, {"RAFT_RECONFIG", ReplicaGroupMode::RAFT_RECONFIG}},
    };
    return table;
}

}  // namespace enum_codec

inline void to_json(Json& j, TablePhase v) {
    static const std::unordered_map<TablePhase, std::string> names{
            {TablePhase::CREATING, "CREATING"},
            {TablePhase::READY, "READY"},
            {TablePhase::DELETING, "DELETING"},
            {TablePhase::DELETED, "DELETED"},
            {TablePhase::RESIZING, "RESIZING"},
    };
    auto it = names.find(v);
    if (it == names.end()) {
        // 编码表漏了新枚举值时宁可失败在保存前，也不落一个 int 出去。
        throw Json::type_error::create(302, fmt::format("TablePhase {} has no name for meta.json", to<int64>(v)),
                                       nullptr);
    }
    enum_codec::encode(j, it->second);
}
inline void from_json(const Json& j, TablePhase& v) { v = enum_codec::decode(j, enum_codec::table_phase_table(), "TablePhase"); }

inline void to_json(Json& j, ReplicaPhase v) {
    static const std::unordered_map<ReplicaPhase, std::string> names{
            {ReplicaPhase::CREATING, "CREATING"},
            {ReplicaPhase::READY, "READY"},
            {ReplicaPhase::LOST, "LOST"},
            {ReplicaPhase::ERROR, "ERROR"},
            {ReplicaPhase::DELETING, "DELETING"},
            {ReplicaPhase::DELETED, "DELETED"},
    };
    auto it = names.find(v);
    if (it == names.end()) {
        throw Json::type_error::create(302, fmt::format("ReplicaPhase {} has no name for meta.json", to<int64>(v)),
                                       nullptr);
    }
    enum_codec::encode(j, it->second);
}
inline void from_json(const Json& j, ReplicaPhase& v) {
    v = enum_codec::decode(j, enum_codec::replica_phase_table(), "ReplicaPhase");
}

inline void to_json(Json& j, ReplicaGroupMode v) {
    static const std::unordered_map<ReplicaGroupMode, std::string> names{
            {ReplicaGroupMode::BOOTSTRAP, "BOOTSTRAP"},
            {ReplicaGroupMode::RAFT_RECONFIG, "RAFT_RECONFIG"},
    };
    auto it = names.find(v);
    if (it == names.end()) {
        throw Json::type_error::create(302, fmt::format("ReplicaGroupMode {} has no name for meta.json", to<int64>(v)),
                                       nullptr);
    }
    enum_codec::encode(j, it->second);
}
inline void from_json(const Json& j, ReplicaGroupMode& v) {
    v = enum_codec::decode(j, enum_codec::group_mode_table(), "ReplicaGroupMode");
}

// 字段只列一次，编码和解码都按结构体的层级处理。
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Database, db_id, name, zone)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(TableSpec, db_id, db_name, name, engine_type, shard_ranges, target_replica_count,
                                   resource_pool)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(TableState, phase, operation_id, last_error_msg, create_ts, update_ts)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Table, table_id, spec, state)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ReplicaSpec, dc, assign_node_id, engine_type)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ReplicaState, phase, update_ts)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Replica, replica_id, spec, state)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ReplicaGroup, shard_id, mode, target_replica_count, desired_members, seq_allocator)
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
    TableID max_table_id{-1};
    for (const auto& table : record.tables) {
        if (!table_ids.insert(table.table_id).second) return Status::ERROR("duplicate table ID");
        // 路由范围不合法的话 load 出来就没法路由了。
        RETURN_IF_INVALID_STATUS(check_shard_ranges_is_valid(table.shard_count(), table.spec.shard_ranges))
        max_table_id = std::max(max_table_id, table.table_id);
    }

    std::unordered_set<ReplicaID, ReplicaIDHash> replica_ids;
    for (const auto& replica : record.replicas) {
        if (!replica_ids.insert(replica.replica_id).second) return Status::ERROR("duplicate replica ID");
    }

    std::unordered_set<ShardID, ShardIDHash> replica_group_ids;
    for (const auto& group : record.replica_groups) {
        if (!replica_group_ids.insert(group.shard_id).second) return Status::ERROR("duplicate replica group ID");
    }

    // 水位必须高于已占用的最大 ID，不然 reopen 后会重复分配。空记录不约束。
    if (!record.tables.empty() && record.next_table_id <= max_table_id) {
        return Status::ERROR("stale next_table_id allocator");
    }
    if (!record.databases.empty()) {
        DatabaseID max_db_id{0};
        for (const auto& db : record.databases) {
            max_db_id = std::max(max_db_id, db.db_id);
        }
        if (record.next_db_id <= max_db_id) return Status::ERROR("stale next_db_id allocator");
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

    return File::atomic_replace(meta_path_, [&](const File& file) { return file.write(payload.data(), payload.size()); });
}

Status MetaPersistEngine::load_meta(MetaPersistedRecord& record) {
    if (!init_flag_) return Status::NOT_INIT("meta persist engine is not init");
    record = {};
    std::string payload;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(File::read_to_string(meta_path_, static_cast<size_t>(K_MAX_META_JSON_BYTES), payload, exists))
    // 文件不存在视为空 meta（首次启动），空文件反而是异常。
    if (!exists) return Status::OK();
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
