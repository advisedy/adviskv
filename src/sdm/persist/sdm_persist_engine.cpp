#include "sdm/persist/sdm_persist_engine.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/defer.h"
#include "common/define.h"
#include "common/framed_record_codec.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"

namespace adviskv::sdm {
namespace {

static constexpr int64 kMaxSdmMetaPayloadBytes = 256 * 1024 * 1024;

class SdmMetaCodec {
public:
    using ObjectType = SdmPersistedRecord;
    using LenType = int64;

    LenType max_payload_len() const {
        return kMaxSdmMetaPayloadBytes;
    }

    void encode_payload(const ObjectType& record, EncodeBuffer& buf) const {
        encode_record(record, buf);
    }

    Status decode_payload(DecodeBuffer& buf, ObjectType& record) const {
        record = {};
        return decode_record(buf, record);
    }

private:
    static void encode_record(const ObjectType& record, EncodeBuffer& buf) {
        buf.write(static_cast<int32>(record.tables.size()));
        for (const auto& [table_id, table] : record.tables) {
            UNUSED(table_id);
            encode_table(table, buf);
        }

        // buf.write(static_cast<int32>(record.nodes.size()));
        // for (const auto& [node_id, node] : record.nodes) {
        //     UNUSED(node_id);
        //     encode_node(node, buf);
        // }

        buf.write(static_cast<int32>(record.replicas.size()));
        for (const auto& [replica_id, replica] : record.replicas) {
            UNUSED(replica_id);
            encode_replica(replica, buf);
        }

        buf.write(static_cast<int32>(record.resource_pools.size()));
        for (const auto& [name, pool] : record.resource_pools) {
            UNUSED(name);
            buf.write(pool.name);
        }

        buf.write(static_cast<int32>(record.replica_groups.size()));
        for (const auto& [shard_id, group] : record.replica_groups) {
            UNUSED(shard_id);
            encode_replica_group(group, buf);
        }
    }

    static Status decode_record(DecodeBuffer& buf, ObjectType& record) {
        int32 table_count{0};
        RETURN_IF_INVALID_READ(buf, table_count)
        if (table_count < 0)
            return Status::ERROR("invalid table_count");
        for (int32 i = 0; i < table_count; ++i) {
            Table table;
            RETURN_IF_INVALID_STATUS(decode_table(buf, table))
            record.tables[table.table_id] = std::move(table);
        }

        // int32 node_count{0};
        // RETURN_IF_INVALID_READ(buf, node_count)
        // if (node_count < 0) return Status::ERROR("invalid node_count");
        // for (int32 i = 0; i < node_count; ++i) {
        //     Node node;
        //     RETURN_IF_INVALID_STATUS(decode_node(buf, node))
        // }

        int32 replica_count{0};
        RETURN_IF_INVALID_READ(buf, replica_count)
        if (replica_count < 0)
            return Status::ERROR("invalid replica_count");
        for (int32 i = 0; i < replica_count; ++i) {
            Replica replica;
            RETURN_IF_INVALID_STATUS(decode_replica(buf, replica))
            record.replicas[replica.replica_id] = std::move(replica);
        }

        int32 pool_count{0};
        RETURN_IF_INVALID_READ(buf, pool_count)
        if (pool_count < 0)
            return Status::ERROR("invalid pool_count");
        for (int32 i = 0; i < pool_count; ++i) {
            ResourcePool pool;
            RETURN_IF_INVALID_READ(buf, pool.name)
            record.resource_pools[pool.name] = std::move(pool);
        }

        int32 group_count{0};
        RETURN_IF_INVALID_READ(buf, group_count)
        if (group_count < 0)
            return Status::ERROR("invalid group_count");
        for (int32 i = 0; i < group_count; ++i) {
            ReplicaGroup group;
            RETURN_IF_INVALID_STATUS(decode_replica_group(buf, group))
            record.replica_groups[group.shard_id] = std::move(group);
        }

        return Status::OK();
    }

    static void encode_shard_id(const ShardID& id, EncodeBuffer& buf) {
        buf.write(id.table_id);
        buf.write(id.shard_index);
    }

    static Status decode_shard_id(DecodeBuffer& buf, ShardID& id) {
        RETURN_IF_INVALID_READ(buf, id.table_id)
        RETURN_IF_INVALID_READ(buf, id.shard_index)
        return Status::OK();
    }

    static void encode_replica_id(const ReplicaID& id, EncodeBuffer& buf) {
        buf.write(id.table_id);
        buf.write(id.shard_index);
        buf.write(id.replica_seq);
    }

    static Status decode_replica_id(DecodeBuffer& buf, ReplicaID& id) {
        RETURN_IF_INVALID_READ(buf, id.table_id)
        RETURN_IF_INVALID_READ(buf, id.shard_index)
        RETURN_IF_INVALID_READ(buf, id.replica_seq)
        return Status::OK();
    }

    static void encode_endpoint(const Endpoint& ep, EncodeBuffer& buf) {
        buf.write(ep.ip);
        buf.write(ep.port);
    }

    static Status decode_endpoint(DecodeBuffer& buf, Endpoint& ep) {
        RETURN_IF_INVALID_READ(buf, ep.ip)
        RETURN_IF_INVALID_READ(buf, ep.port)
        return Status::OK();
    }

    static void encode_table(const Table& table, EncodeBuffer& buf) {
        buf.write(table.table_id);
        buf.write(table.spec.table_name);
        buf.write(table.spec.db_id);
        buf.write(table.spec.db_name);
        buf.write(table.spec.shard_count);
        buf.write(table.spec.replica_count);
        buf.write(table.spec.resource_pool);
        buf.write(table.spec.operation_id);
        buf.write(static_cast<int32>(table.state.desired));
        buf.write(static_cast<int32>(table.state.phase));
        buf.write(table.state.last_error_msg);
        buf.write(table.state.update_ts);
    }

    static Status decode_table(DecodeBuffer& buf, Table& table) {
        table = {};
        RETURN_IF_INVALID_READ(buf, table.table_id)
        RETURN_IF_INVALID_READ(buf, table.spec.table_name)
        RETURN_IF_INVALID_READ(buf, table.spec.db_id)
        RETURN_IF_INVALID_READ(buf, table.spec.db_name)
        RETURN_IF_INVALID_READ(buf, table.spec.shard_count)
        RETURN_IF_INVALID_READ(buf, table.spec.replica_count)
        RETURN_IF_INVALID_READ(buf, table.spec.resource_pool)
        RETURN_IF_INVALID_READ(buf, table.spec.operation_id)

        int32 desired{0};
        RETURN_IF_INVALID_READ(buf, desired)
        table.state.desired = static_cast<TableDesired>(desired);

        int32 phase{0};
        RETURN_IF_INVALID_READ(buf, phase)
        table.state.phase = static_cast<TablePhase>(phase);

        RETURN_IF_INVALID_READ(buf, table.state.last_error_msg)
        RETURN_IF_INVALID_READ(buf, table.state.update_ts)
        return Status::OK();
    }

    // static Status decode_node(DecodeBuffer& buf, Node& node) {
    //     node = {};
    //     RETURN_IF_INVALID_READ(buf, node.id)
    //     RETURN_IF_INVALID_READ(buf, node.meta.resource_pool)
    //     RETURN_IF_INVALID_READ(buf, node.meta.dc)

    //     int32 status{0};
    //     RETURN_IF_INVALID_READ(buf, status)
    //     node.state.status = static_cast<NodeStatus>(status);

    //     RETURN_IF_INVALID_STATUS(decode_endpoint(buf, node.state.endpoint))
    //     RETURN_IF_INVALID_READ(buf, node.state.last_heartbeat_ts)
    //     RETURN_IF_INVALID_READ(buf, node.derived.owned_replica_count)
    //     RETURN_IF_INVALID_READ(buf, node.derived.owned_leader_count)
    //     return Status::OK();
    // }

    static void encode_replica(const Replica& replica, EncodeBuffer& buf) {
        encode_replica_id(replica.replica_id, buf);
        buf.write(replica.spec.dc);
        buf.write(replica.spec.assign_node_id);
        buf.write(static_cast<int32>(replica.spec.engine_type));
        buf.write(static_cast<int32>(replica.state.desired));
        buf.write(static_cast<int32>(replica.state.phase));
        buf.write(static_cast<int32>(replica.state.observed_raft_role));
        buf.write(static_cast<int32>(replica.state.observed_member_type));
        encode_endpoint(replica.state.observed_endpoint, buf);
        buf.write(static_cast<int32>(replica.state.observed_storage_status));
        buf.write(static_cast<int32>(replica.state.observed_no_exist ? 1 : 0));
        buf.write(replica.state.last_error_msg);
        buf.write(replica.state.update_ts);
        buf.write(replica.state.term);
    }

    static Status decode_replica(DecodeBuffer& buf, Replica& replica) {
        replica = {};
        RETURN_IF_INVALID_STATUS(decode_replica_id(buf, replica.replica_id))
        RETURN_IF_INVALID_READ(buf, replica.spec.dc)
        RETURN_IF_INVALID_READ(buf, replica.spec.assign_node_id)

        int32 engine_type{0};
        RETURN_IF_INVALID_READ(buf, engine_type)
        replica.spec.engine_type = static_cast<EngineType>(engine_type);

        int32 desired{0};
        RETURN_IF_INVALID_READ(buf, desired)
        replica.state.desired = static_cast<ReplicaDesired>(desired);

        int32 phase{0};
        RETURN_IF_INVALID_READ(buf, phase)
        replica.state.phase = static_cast<ReplicaPhase>(phase);

        int32 observed_raft_role{0};
        RETURN_IF_INVALID_READ(buf, observed_raft_role)
        RETURN_IF_INVALID_CONDITION(decode_replica_role(observed_raft_role, replica.state.observed_raft_role),
                                    "invalid observed_raft_role")

        int32 observed_member_type{0};
        RETURN_IF_INVALID_READ(buf, observed_member_type)
        RETURN_IF_INVALID_CONDITION(decode_raft_member_type(observed_member_type, replica.state.observed_member_type),
                                    "invalid observed_member_type")

        RETURN_IF_INVALID_STATUS(decode_endpoint(buf, replica.state.observed_endpoint))

        int32 observed_storage_status{0};
        RETURN_IF_INVALID_READ(buf, observed_storage_status)
        RETURN_IF_INVALID_CONDITION(
                decode_storage_replica_status(observed_storage_status, replica.state.observed_storage_status),
                "invalid observed_storage_status")

        int32 observed_no_exist{0};
        RETURN_IF_INVALID_READ(buf, observed_no_exist)
        RETURN_IF_INVALID_CONDITION(observed_no_exist == 0 || observed_no_exist == 1, "invalid observed_no_exist")
        replica.state.observed_no_exist = observed_no_exist == 1;

        RETURN_IF_INVALID_READ(buf, replica.state.last_error_msg)
        RETURN_IF_INVALID_READ(buf, replica.state.update_ts)
        RETURN_IF_INVALID_READ(buf, replica.state.term)

        return Status::OK();
    }

    static void encode_replica_group(const ReplicaGroup& group, EncodeBuffer& buf) {
        encode_shard_id(group.shard_id, buf);
        buf.write(static_cast<int32>(group.mode));
        buf.write(group.target_replica_count);
        buf.write<int32>(static_cast<int32>(group.desired_members.size()));
        for (const ReplicaID& replica_id : group.desired_members) {
            encode_replica_id(replica_id, buf);
        }
        buf.write(group.observed_membership_term);
        encode_replica_id(group.observed_membership_leader, buf);
        buf.write(group.seq_allocator.current_id());
    }

    static Status decode_replica_group(DecodeBuffer& buf, ReplicaGroup& group) {
        group = {};
        RETURN_IF_INVALID_STATUS(decode_shard_id(buf, group.shard_id))
        int32 mode{0};
        RETURN_IF_INVALID_READ(buf, mode)
        switch (to<ReplicaGroupMode>(mode)) {
            case ReplicaGroupMode::BOOTSTRAP:
            case ReplicaGroupMode::RAFT_RECONFIG:
                group.mode = to<ReplicaGroupMode>(mode);
                break;
            default:
                return Status::ERROR("invalid replica group mode");
        }
        RETURN_IF_INVALID_READ(buf, group.target_replica_count)

        int32 member_count{0};
        RETURN_IF_INVALID_READ(buf, member_count)
        if (member_count < 0) {
            return Status::ERROR("invalid replica group member_count");
        }
        group.desired_members.clear();
        group.desired_members.reserve(static_cast<size_t>(member_count));
        for (int32 i = 0; i < member_count; ++i) {
            ReplicaID replica_id;
            RETURN_IF_INVALID_STATUS(decode_replica_id(buf, replica_id))
            group.desired_members.push_back(replica_id);
        }
        RETURN_IF_INVALID_READ(buf, group.observed_membership_term)
        RETURN_IF_INVALID_STATUS(decode_replica_id(buf, group.observed_membership_leader))

        ReplicaSeq next_replica_seq{0};
        RETURN_IF_INVALID_READ(buf, next_replica_seq)
        group.seq_allocator = IDAllocator<ReplicaSeq>(next_replica_seq);
        return Status::OK();
    }
};

}  // namespace

SdmPersistEngine::SdmPersistEngine(const std::string& data_dir) : data_dir_(data_dir) {
}

SdmPersistEngine::~SdmPersistEngine() {
    close();
}

Status SdmPersistEngine::init() {
    if (data_dir_.empty()) {
        return Status::INVALID_ARGUMENT("sdm persist engine data_dir is empty");
    }

    meta_path_ = data_dir_ + "/sdm_meta";
    meta_tmp_path_ = data_dir_ + "/sdm_meta.tmp";

    // IGNORE_RESULT(::mkdir(data_dir_.c_str(), 0755));
    std::error_code ec;
    std::filesystem::create_directories(data_dir_, ec);
    if (ec) {
        return Status::ERROR(
                fmt::format("failed to create sdm persist engine data dir: {}, error: {}", data_dir_, ec.message()));
    }

    LOG_DEBUG("sdm persist engine init, data_dir_={}", data_dir_);
    init_flag_ = true;
    return Status::OK();
}

Status SdmPersistEngine::close() {
    return Status::OK();
}

Status SdmPersistEngine::save_sdm_meta(const SdmPersistedRecord& record) {
    ADVISKV_METRICS_TIMER("sdm_persist_save_sdm_meta");
    ADVISKV_METRICS_COUNTER("sdm_persist_save_sdm_meta_request");

    if (!init_flag_)
        return Status::NOT_INIT("sdm persist engine is not init");

    int fd = ::open(meta_tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status::ERROR(fmt::format("failed to open sdm meta tmp file: {}", strerror(errno)));
    }
    auto fd_guard = Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    RETURN_IF_INVALID_STATUS(FramedRecord<SdmMetaCodec>::encode_to_fd(fd, record))

    if (::fsync(fd) != 0)
        return Status::ERROR();

    if (::close(fd) != 0)
        return Status::ERROR();

    fd = -1;

    if (::rename(meta_tmp_path_.c_str(), meta_path_.c_str()) != 0) {
        return Status::ERROR(fmt::format("failed to rename sdm meta file: {}", strerror(errno)));
    }

    {
        int dir_fd = ::open(data_dir_.c_str(), O_RDONLY | O_DIRECTORY);
        if (dir_fd < 0)
            return Status::ERROR();

        if (::fsync(dir_fd) != 0)
            return Status::ERROR();
        if (::close(dir_fd) != 0)
            return Status::ERROR();
    }

    LOG_DEBUG(
            "sdm persist engine save_meta success, tables={}, replicas={}, "
            "pools={}, replica_groups={}",
            record.tables.size(), record.replicas.size(), record.resource_pools.size(), record.replica_groups.size());
    return Status::OK();
}

Status SdmPersistEngine::load_sdm_meta(SdmPersistedRecord& record) {
    if (!init_flag_)
        return Status::NOT_INIT("sdm persist engine is not init");

    record = {};

    int fd = ::open(meta_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            LOG_DEBUG("sdm meta file not found, starting with empty state");
            return Status::OK();
        }
        return Status::ERROR(fmt::format("failed to open sdm meta file: {}", strerror(errno)));
    }
    auto fd_guard = Defer([fd]() { ::close(fd); });

    RETURN_IF_INVALID_STATUS(FramedRecord<SdmMetaCodec>::decode_from_fd(fd, record))

    LOG_DEBUG(
            "sdm persist engine load_meta success, tables={}, replicas={}, "
            "pools={}, replica_groups={}",
            record.tables.size(), record.replicas.size(), record.resource_pools.size(), record.replica_groups.size());
    return Status::OK();
}

}  // namespace adviskv::sdm