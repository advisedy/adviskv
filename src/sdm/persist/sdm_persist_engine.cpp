#include "sdm/persist/sdm_persist_engine.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/defer.h"
#include "common/define.h"
#include "common/framed_record_codec.h"
#include "common/log.h"
#include "common/status.h"

namespace adviskv::sdm {
namespace {

static constexpr int64 kMaxSdmMetaPayloadBytes = 256 * 1024 * 1024;

class SdmMetaCodec {
   public:
    using ObjectType = SdmPersistedRecord;
    using LenType = int64;

    LenType max_payload_len() const { return kMaxSdmMetaPayloadBytes; }

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

        buf.write(static_cast<int32>(record.nodes.size()));
        for (const auto& [node_id, node] : record.nodes) {
            UNUSED(node_id);
            encode_node(node, buf);
        }

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

        buf.write(static_cast<int32>(record.shard_routes.size()));
        for (const auto& [shard_id, route] : record.shard_routes) {
            UNUSED(shard_id);
            encode_shard_route(route, buf);
        }
    }

    static Status decode_record(DecodeBuffer& buf, ObjectType& record) {
        int32 table_count{0};
        RETURN_IF_INVALID_READ(buf, table_count)
        if (table_count < 0) return Status::ERROR("invalid table_count");
        for (int32 i = 0; i < table_count; ++i) {
            Table table;
            RETURN_IF_INVALID_STATUS(decode_table(buf, table))
            record.tables[table.table_id] = std::move(table);
        }

        int32 node_count{0};
        RETURN_IF_INVALID_READ(buf, node_count)
        if (node_count < 0) return Status::ERROR("invalid node_count");
        for (int32 i = 0; i < node_count; ++i) {
            Node node;
            RETURN_IF_INVALID_STATUS(decode_node(buf, node))
            record.nodes[node.id] = std::move(node);
        }

        int32 replica_count{0};
        RETURN_IF_INVALID_READ(buf, replica_count)
        if (replica_count < 0) return Status::ERROR("invalid replica_count");
        for (int32 i = 0; i < replica_count; ++i) {
            Replica replica;
            RETURN_IF_INVALID_STATUS(decode_replica(buf, replica))
            record.replicas[replica.replica_id] = std::move(replica);
        }

        int32 pool_count{0};
        RETURN_IF_INVALID_READ(buf, pool_count)
        if (pool_count < 0) return Status::ERROR("invalid pool_count");
        for (int32 i = 0; i < pool_count; ++i) {
            ResourcePool pool;
            RETURN_IF_INVALID_READ(buf, pool.name)
            record.resource_pools[pool.name] = std::move(pool);
        }

        int32 route_count{0};
        RETURN_IF_INVALID_READ(buf, route_count)
        if (route_count < 0) return Status::ERROR("invalid route_count");
        for (int32 i = 0; i < route_count; ++i) {
            ShardRoute route;
            RETURN_IF_INVALID_STATUS(decode_shard_route(buf, route))
            record.shard_routes[route.shard_id] = std::move(route);
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
        buf.write(id.replica_index);
    }

    static Status decode_replica_id(DecodeBuffer& buf, ReplicaID& id) {
        RETURN_IF_INVALID_READ(buf, id.table_id)
        RETURN_IF_INVALID_READ(buf, id.shard_index)
        RETURN_IF_INVALID_READ(buf, id.replica_index)
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

    static void encode_node(const Node& node, EncodeBuffer& buf) {
        buf.write(node.id);
        buf.write(node.spec.resource_pool);
        buf.write(node.spec.dc);
        buf.write(static_cast<int32>(node.spec.status));
        encode_endpoint(node.state.endpoint, buf);
        buf.write(node.state.last_heartbeat_ts);
        buf.write(node.derived.owned_replica_count);
        buf.write(node.derived.owned_leader_count);
    }

    static Status decode_node(DecodeBuffer& buf, Node& node) {
        node = {};
        RETURN_IF_INVALID_READ(buf, node.id)
        RETURN_IF_INVALID_READ(buf, node.spec.resource_pool)
        RETURN_IF_INVALID_READ(buf, node.spec.dc)

        int32 status{0};
        RETURN_IF_INVALID_READ(buf, status)
        node.spec.status = static_cast<NodeStatus>(status);

        RETURN_IF_INVALID_STATUS(decode_endpoint(buf, node.state.endpoint))
        RETURN_IF_INVALID_READ(buf, node.state.last_heartbeat_ts)
        RETURN_IF_INVALID_READ(buf, node.derived.owned_replica_count)
        RETURN_IF_INVALID_READ(buf, node.derived.owned_leader_count)
        return Status::OK();
    }

    static void encode_replica(const Replica& replica, EncodeBuffer& buf) {
        encode_replica_id(replica.replica_id, buf);
        buf.write(replica.spec.dc);
        buf.write(replica.spec.assign_node_id);
        buf.write(static_cast<int32>(replica.spec.engine_type));
        buf.write<int32>(static_cast<int32>(replica.spec.members.size()));
        for (const PeerMember& member : replica.spec.members) {
            buf.write(member.node_id);
            encode_replica_id(member.replica_id, buf);
            encode_endpoint(member.endpoint, buf);
        }
        buf.write(static_cast<int32>(replica.state.desired));
        buf.write(static_cast<int32>(replica.state.phase));
        buf.write(static_cast<int32>(replica.state.observed_role));
        encode_endpoint(replica.state.observed_endpoint, buf);
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

        int32 member_count{0};
        RETURN_IF_INVALID_READ(buf, member_count)
        if (member_count < 0) return Status::ERROR("invalid member_count");
        replica.spec.members.clear();
        replica.spec.members.reserve(static_cast<size_t>(member_count));
        for (int32 i = 0; i < member_count; ++i) {
            PeerMember member;
            RETURN_IF_INVALID_READ(buf, member.node_id)
            RETURN_IF_INVALID_STATUS(decode_replica_id(buf, member.replica_id))
            RETURN_IF_INVALID_STATUS(decode_endpoint(buf, member.endpoint))
            replica.spec.members.push_back(std::move(member));
        }

        int32 desired{0};
        RETURN_IF_INVALID_READ(buf, desired)
        replica.state.desired = static_cast<ReplicaDesired>(desired);

        int32 phase{0};
        RETURN_IF_INVALID_READ(buf, phase)
        replica.state.phase = static_cast<ReplicaPhase>(phase);

        int32 observed_role{0};
        RETURN_IF_INVALID_READ(buf, observed_role)
        replica.state.observed_role = static_cast<ReplicaRole>(observed_role);

        RETURN_IF_INVALID_STATUS(
            decode_endpoint(buf, replica.state.observed_endpoint))
        RETURN_IF_INVALID_READ(buf, replica.state.last_error_msg)
        RETURN_IF_INVALID_READ(buf, replica.state.update_ts)
        RETURN_IF_INVALID_READ(buf, replica.state.term)

        return Status::OK();
    }

    static void encode_shard_route(const ShardRoute& route, EncodeBuffer& buf) {
        encode_shard_id(route.shard_id, buf);
        buf.write<int32>(static_cast<int32>(route.replicas.size()));
        for (const RouteEntry& entry : route.replicas) {
            encode_replica_id(entry.replica_id, buf);
            buf.write(entry.node_id);
            buf.write(entry.ip);
            buf.write(entry.port);
            buf.write(static_cast<int32>(entry.role));
        }
    }

    static Status decode_shard_route(DecodeBuffer& buf, ShardRoute& route) {
        route = {};
        RETURN_IF_INVALID_STATUS(decode_shard_id(buf, route.shard_id))

        int32 entry_count{0};
        RETURN_IF_INVALID_READ(buf, entry_count)
        if (entry_count < 0) return Status::ERROR("invalid route entry count");

        route.replicas.reserve(entry_count);
        for (int32 i = 0; i < entry_count; ++i) {
            RouteEntry entry;
            RETURN_IF_INVALID_STATUS(decode_replica_id(buf, entry.replica_id))
            RETURN_IF_INVALID_READ(buf, entry.node_id)
            RETURN_IF_INVALID_READ(buf, entry.ip)
            RETURN_IF_INVALID_READ(buf, entry.port)

            int32 role{0};
            RETURN_IF_INVALID_READ(buf, role)
            entry.role = static_cast<ReplicaRole>(role);

            route.replicas.push_back(std::move(entry));
        }
        return Status::OK();
    }
};

}  // namespace

SdmPersistEngine::SdmPersistEngine(const std::string& data_dir)
    : data_dir_(data_dir) {}

SdmPersistEngine::~SdmPersistEngine() { close(); }

Status SdmPersistEngine::init() {
    meta_path_ = data_dir_ + "/sdm_meta";
    meta_tmp_path_ = data_dir_ + "/sdm_meta.tmp";

    IGNORE_RESULT(::mkdir(data_dir_.c_str(), 0755));

    LOG_DEBUG("sdm persist engine init, data_dir_={}", data_dir_);
    init_flag_ = true;
    return Status::OK();
}

Status SdmPersistEngine::close() { return Status::OK(); }

Status SdmPersistEngine::save_sdm_meta(const SdmPersistedRecord& record) {
    if (!init_flag_) return Status::NOT_INIT("sdm persist engine is not init");

    int fd = ::open(meta_tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status::ERROR(fmt::format("failed to open sdm meta tmp file: {}",
                                         strerror(errno)));
    }
    auto fd_guard = Defer([&fd]() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    });

    RETURN_IF_INVALID_STATUS(
        FramedRecord<SdmMetaCodec>::encode_to_fd(fd, record))

    if (::fsync(fd) != 0) return Status::ERROR();

    if (::close(fd) != 0) return Status::ERROR();

    fd = -1;

    if (::rename(meta_tmp_path_.c_str(), meta_path_.c_str()) != 0) {
        return Status::ERROR(
            fmt::format("failed to rename sdm meta file: {}", strerror(errno)));
    }

    {
        int dir_fd = ::open(data_dir_.c_str(), O_RDONLY | O_DIRECTORY);
        if (dir_fd < 0) return Status::ERROR();

        if (::fsync(dir_fd) != 0) return Status::ERROR();
        if (::close(dir_fd) != 0) return Status::ERROR();
    }

    LOG_DEBUG(
        "sdm persist engine save_meta success, tables={}, nodes={}, "
        "replicas={}, pools={}, routes={}",
        record.tables.size(), record.nodes.size(), record.replicas.size(),
        record.resource_pools.size(), record.shard_routes.size());
    return Status::OK();
}

Status SdmPersistEngine::load_sdm_meta(SdmPersistedRecord& record) {
    if (!init_flag_) return Status::NOT_INIT("sdm persist engine is not init");

    record = {};

    int fd = ::open(meta_path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            LOG_DEBUG("sdm meta file not found, starting with empty state");
            return Status::OK();
        }
        return Status::ERROR(
            fmt::format("failed to open sdm meta file: {}", strerror(errno)));
    }
    auto fd_guard = Defer([fd]() { ::close(fd); });

    Status status = FramedRecord<SdmMetaCodec>::decode_from_fd(fd, record);
    if (status.code() == StatusCode::GET_EOF) {
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(status)

    LOG_DEBUG(
        "sdm persist engine load_meta success, tables={}, nodes={}, "
        "replicas={}, pools={}, routes={}",
        record.tables.size(), record.nodes.size(), record.replicas.size(),
        record.resource_pools.size(), record.shard_routes.size());
    return Status::OK();
}

}  // namespace adviskv::sdm