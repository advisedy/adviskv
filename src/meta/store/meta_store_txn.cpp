#include "meta/store/meta_store_txn.h"

#include <algorithm>

#include "common/func.h"
#include "common/status.h"

namespace adviskv::meta {

MetaStoreTxn::MetaStoreTxn(const MetaWorkingSet* ws) : ws_(ws) {}
MetaStoreTxn::MetaStoreTxn(MetaWorkingSet* ws) : ws_(ws), mutable_ws_(ws) {}

bool MetaStoreTxn::durable_dirty() const { return durable_dirty_; }

Status MetaStoreTxn::put_database(const Database& db) {
    Optional<Database> old;
    RETURN_IF_INVALID_STATUS(get_database(db.db_id, old))
    mutable_durable().databases[db.db_id] = db;
    mutable_index().on_db_upsert(old.ptr(), db);
    mark_durable_dirty();
    return Status::OK();
}

Status MetaStoreTxn::get_database(DatabaseID db_id, DatabaseOr& out) const {
    auto it = durable().databases.find(db_id);
    if (it == durable().databases.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MetaStoreTxn::get_database_by_name(const std::string& name, DatabaseOr& out) const {
    DatabaseID db_id;
    Status status = index().find_db_by_name(name, db_id);
    if (status.fail()) {
        if (status.code() == StatusCode::DB_NOT_FOUND) {
            out.reset();
            return Status::OK();
        }
        return status;
    }
    return get_database(db_id, out);
}

Status MetaStoreTxn::list_databases(std::vector<Database>& out) const {
    out.clear();
    out.reserve(durable().databases.size());
    for (const auto& kv : durable().databases) {
        out.push_back(kv.second);
    }
    std::sort(out.begin(), out.end(), [](const Database& lhs, const Database& rhs) { return lhs.db_id < rhs.db_id; });
    return Status::OK();
}

Status MetaStoreTxn::delete_database(DatabaseID db_id) {
    Optional<Database> old;
    RETURN_IF_INVALID_STATUS(get_database(db_id, old))
    if (old.is_empty()) {
        return Status::OK();
    }
    mutable_durable().databases.erase(db_id);
    mutable_index().on_db_delete(*old);
    mark_durable_dirty();
    return Status::OK();
}

DatabaseID MetaStoreTxn::allocate_db_id() {
    mark_durable_dirty();
    return mutable_durable().db_id_allocator.get_next_id();
}

TableID MetaStoreTxn::allocate_table_id() {
    mark_durable_dirty();
    return mutable_durable().table_id_allocator.get_next_id();
}

//////////////////////////////////////////////////////////////////////

Status MetaStoreTxn::put_table(const Table& table) {
    Optional<Table> old;
    RETURN_IF_INVALID_STATUS(get_table(table.table_id, old))
    mutable_durable().tables[table.table_id] = table;
    mutable_index().on_table_upsert(old.ptr(), table);
    mark_durable_dirty();
    return Status::OK();
}

Status MetaStoreTxn::get_table(TableID table_id, TableOr& out) const {
    auto it = durable().tables.find(table_id);
    if (it == durable().tables.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MetaStoreTxn::get_table_by_name(const std::string& db_name, const std::string& table_name, TableOr& out) const {
    TableID table_id;
    Status status = index().find_table_by_name(db_name, table_name, table_id);
    if (status.code() == StatusCode::TABLE_NOT_FOUND) {
        out.reset();
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(status)
    Optional<Table> table;
    RETURN_IF_INVALID_STATUS(get_table(table_id, table))
    out = table.has_value() ? TableOr{*table} : TableOr{};
    return Status::OK();
}

Status MetaStoreTxn::list_tables(std::vector<Table>& out) const {
    out.clear();
    out.reserve(durable().tables.size());
    for (const auto& kv : durable().tables) {
        out.push_back(kv.second);
    }
    std::sort(out.begin(), out.end(), [](const Table& lhs, const Table& rhs) { return lhs.table_id < rhs.table_id; });
    return Status::OK();
}

Status MetaStoreTxn::delete_table(TableID table_id) {
    Optional<Table> old;
    RETURN_IF_INVALID_STATUS(get_table(table_id, old))
    if (old.is_empty()) {
        return Status::OK();
    }
    mutable_durable().tables.erase(table_id);
    mutable_index().on_table_delete(*old);
    mark_durable_dirty();
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status MetaStoreTxn::put_node(const Node& node) {
    Optional<Node> old;
    RETURN_IF_INVALID_STATUS(get_node(node.id, old))
    mutable_runtime().nodes[node.id] = node;
    mutable_index().on_node_upsert(old.ptr(), node);
    return Status::OK();
}

Status MetaStoreTxn::get_node(const NodeID& node_id, NodeOr& out) const {
    auto it = runtime().nodes.find(node_id);
    if (it == runtime().nodes.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MetaStoreTxn::list_nodes(std::vector<Node>& out) const {
    out.clear();
    out.reserve(runtime().nodes.size());
    for (const auto& kv : runtime().nodes) {
        out.push_back(kv.second);
    }
    std::sort(out.begin(), out.end(), [](const Node& lhs, const Node& rhs) { return lhs.id < rhs.id; });
    return Status::OK();
}

Status MetaStoreTxn::list_nodes_by_resource_pool(const std::string& pool_name, std::vector<Node>& out) const {
    std::vector<NodeID> node_ids;
    RETURN_IF_INVALID_STATUS(index().list_nodes_by_resource_pool(pool_name, node_ids))
    std::sort(node_ids.begin(), node_ids.end());

    out.clear();
    out.reserve(node_ids.size());
    for (const NodeID& node_id : node_ids) {
        Optional<Node> node;
        RETURN_IF_INVALID_STATUS(get_node(node_id, node))
        if (node.has_value()) {
            out.push_back(*node);
        }
    }
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status MetaStoreTxn::get_resource_pool(const std::string& name, ResourcePoolOr& out) const {
    auto it = durable().resource_pools.find(name);
    if (it == durable().resource_pools.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MetaStoreTxn::list_resource_pools(std::vector<ResourcePool>& pools) const {
    pools.clear();
    pools.reserve(durable().resource_pools.size());
    for (const auto& kv : durable().resource_pools) {
        pools.push_back(kv.second);
    }
    std::sort(pools.begin(), pools.end(),
              [](const ResourcePool& lhs, const ResourcePool& rhs) { return lhs.name < rhs.name; });
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status MetaStoreTxn::put_replica(const Replica& replica) {
    Optional<Replica> old;
    RETURN_IF_INVALID_STATUS(get_replica(replica.replica_id, old))
    mutable_durable().replicas[replica.replica_id] = replica;
    mutable_index().on_replica_upsert(old.ptr(), replica);
    mark_durable_dirty();
    return Status::OK();
}

Status MetaStoreTxn::put_replicas(const std::vector<Replica>& replicas) {
    std::vector<Optional<Replica>> olds;
    olds.reserve(replicas.size());
    for (const Replica& replica : replicas) {
        Optional<Replica> old;
        RETURN_IF_INVALID_STATUS(get_replica(replica.replica_id, old))
        olds.emplace_back(std::move(old));
    }

    for (const Replica& replica : replicas) {
        mutable_durable().replicas[replica.replica_id] = replica;
    }
    for (size_t i = 0; i < replicas.size(); i++) {
        mutable_index().on_replica_upsert(olds[i].ptr(), replicas[i]);
    }
    if (!replicas.empty()) {
        mark_durable_dirty();
    }
    return Status::OK();
}

Status MetaStoreTxn::get_replica(const ReplicaID& replica_key, ReplicaOr& out) const {
    auto it = durable().replicas.find(replica_key);
    if (it == durable().replicas.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MetaStoreTxn::delete_replica(const ReplicaID& replica_key) {
    Optional<Replica> old;
    RETURN_IF_INVALID_STATUS(get_replica(replica_key, old))
    if (old.is_empty()) {
        return Status::OK();
    }
    mutable_durable().replicas.erase(replica_key);
    mutable_index().on_replica_delete(*old);
    mark_durable_dirty();
    return Status::OK();
}

Status MetaStoreTxn::list_replicas(std::vector<Replica>& out) const {
    out.clear();
    out.reserve(durable().replicas.size());
    for (const auto& kv : durable().replicas) {
        out.push_back(kv.second);
    }
    return Status::OK();
}

namespace {

bool less_replica_id(const ReplicaID& lhs, const ReplicaID& rhs) {
    if (lhs.table_id != rhs.table_id) {
        return lhs.table_id < rhs.table_id;
    }
    if (lhs.shard_index != rhs.shard_index) {
        return lhs.shard_index < rhs.shard_index;
    }
    return lhs.replica_seq < rhs.replica_seq;
}

bool less_shard_id(const ShardID& lhs, const ShardID& rhs) {
    if (lhs.table_id != rhs.table_id) {
        return lhs.table_id < rhs.table_id;
    }
    return lhs.shard_index < rhs.shard_index;
}

}  // namespace

// 如果shard上没有replicas，返回一个空集合是被允许的
Status MetaStoreTxn::list_replicas_by_shard(const ShardID& shard_id, std::vector<Replica>& out) const {
    std::vector<ReplicaID> replica_ids;
    RETURN_IF_INVALID_STATUS(index().list_replicas_by_shard(shard_id, replica_ids));
    std::sort(replica_ids.begin(), replica_ids.end(), less_replica_id);

    out.clear();
    out.reserve(replica_ids.size());
    for (const ReplicaID& replica_id : replica_ids) {
        Optional<Replica> replica;
        RETURN_IF_INVALID_STATUS(get_replica(replica_id, replica))
        if (replica.has_value()) {
            out.push_back(*replica);
        }
    }
    return Status::OK();
}

Status MetaStoreTxn::list_replicas_by_node(NodeID node_id, std::vector<Replica>& out) const {
    std::vector<ReplicaID> replica_ids;
    RETURN_IF_INVALID_STATUS(index().list_replicas_by_node(node_id, replica_ids));
    std::sort(replica_ids.begin(), replica_ids.end(), less_replica_id);

    out.clear();
    out.reserve(replica_ids.size());
    for (const ReplicaID& replica_id : replica_ids) {
        Optional<Replica> replica;
        RETURN_IF_INVALID_STATUS(get_replica(replica_id, replica))
        if (replica.has_value()) {
            out.push_back(*replica);
        }
    }
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status MetaStoreTxn::put_shard_route(const ShardRoute& route) {
    mutable_runtime().shard_routes[route.shard_id] = route;
    return Status::OK();
}

Status MetaStoreTxn::get_shard_route(const ShardID& shard_id, ShardRouteOr& out) const {
    auto it = runtime().shard_routes.find(shard_id);
    if (it == runtime().shard_routes.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MetaStoreTxn::delete_shard_route(const ShardID& shard_id) {
    Optional<ShardRoute> route;
    RETURN_IF_INVALID_STATUS(get_shard_route(shard_id, route))
    if (route.is_empty()) {
        return Status::OK();
    }
    mutable_runtime().shard_routes.erase(shard_id);
    return Status::OK();
}

Status MetaStoreTxn::del_shard_route_entry(const ShardID& shard_id, const ReplicaID& replica_id) {
    auto it = mutable_runtime().shard_routes.find(shard_id);
    if (it == mutable_runtime().shard_routes.end()) {
        return Status::OK();
    }
    func::ad_erase_if(it->second.replicas,
                      [&replica_id](const RouteEntry& entry) { return entry.replica_id == replica_id; });
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status MetaStoreTxn::put_replica_group(const ReplicaGroup& group) {
    mutable_durable().replica_groups[group.shard_id] = group;
    mark_durable_dirty();
    return Status::OK();
}

Status MetaStoreTxn::get_replica_group(const ShardID& shard_id, ReplicaGroupOr& out) const {
    auto it = durable().replica_groups.find(shard_id);
    if (it == durable().replica_groups.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MetaStoreTxn::delete_replica_group(const ShardID& shard_id) {
    Optional<ReplicaGroup> old;
    RETURN_IF_INVALID_STATUS(get_replica_group(shard_id, old))
    if (old.is_empty()) {
        return Status::OK();
    }
    mutable_durable().replica_groups.erase(shard_id);
    mark_durable_dirty();
    return Status::OK();
}

Status MetaStoreTxn::list_replica_groups(std::vector<ReplicaGroup>& out) const {
    out.clear();
    out.reserve(durable().replica_groups.size());
    for (const auto& kv : durable().replica_groups) {
        out.push_back(kv.second);
    }
    std::sort(out.begin(), out.end(), [](const ReplicaGroup& lhs, const ReplicaGroup& rhs) {
        return less_shard_id(lhs.shard_id, rhs.shard_id);
    });
    return Status::OK();
}

}  // namespace adviskv::meta
