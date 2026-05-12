#include "sdm/model/sdm_store.h"

#include <filesystem>
#include <mutex>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/status.h"
#include "sdm/model/i_sdm_metastore.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

SdmStore::SdmStore(SdmMetaStoreType type,
                   const std::string& persistent_data_dir) {
    if (type == SdmMetaStoreType::PERSISTENT and persistent_data_dir.empty()) {
        LOG_ERROR("persistent_data_dir is empty");
        return;
    }

    switch (type) {
        case SdmMetaStoreType::PERSISTENT:
            meta_store_ = std::make_unique<PersistentMetaStore>(
                std::filesystem::path(persistent_data_dir));
            IGNORE_RESULT(rebuild_runtime_index());
            break;
        case SdmMetaStoreType::MEMORY:
        default:
            meta_store_ = std::make_unique<MemoryMetaStore>();
            break;
    }
}

Status SdmStore::put_table(const Table& table) {
    std::unique_lock locker{mutex_};

    TablePtr old_ptr;
    Status status = meta_store_->get_table(table.table_id, old_ptr);
    RETURN_IF_INVALID_STATUS(status)

    status = meta_store_->upsert_table(table);
    RETURN_IF_INVALID_STATUS(status)

    return runtime_index_.on_table_upsert(old_ptr.get(), table);
}

Status SdmStore::get_table(TableID table_id,
                           std::shared_ptr<Table>& out) const {
    std::shared_lock locker{mutex_};
    return meta_store_->get_table(table_id, out);
}

Status SdmStore::get_table_by_name(const std::string& db_name,
                                   const std::string& table_name,
                                   std::shared_ptr<Table>& out) const {
    std::shared_lock locker{mutex_};

    TableID table_id;
    Status status =
        runtime_index_.find_table_by_name(db_name, table_name, table_id);
    RETURN_IF_INVALID_STATUS(status)
    return meta_store_->get_table(table_id, out);
}

Status SdmStore::list_tables(std::vector<std::shared_ptr<Table>>& out) const {
    std::shared_lock locker{mutex_};
    return meta_store_->list_tables(out);
}

Status SdmStore::list_tables_by_lifecycle(
    TableLifecycle lifecycle, std::vector<std::shared_ptr<Table>>& out) const {
    std::shared_lock locker{mutex_};

    std::vector<TableID> table_ids;
    Status status =
        runtime_index_.list_tables_by_lifecycle(lifecycle, table_ids);
    RETURN_IF_INVALID_STATUS(status)

    out.clear();
    out.reserve(table_ids.size());
    for (const TableID& table_id : table_ids) {
        TablePtr table;
        status = meta_store_->get_table(table_id, table);
        RETURN_IF_INVALID_STATUS(status)
        if (table != nullptr) {
            out.push_back(table);
        }
    }
    return Status::OK();
}

Status SdmStore::list_nodes_by_resource_pool(const std::string& pool_name,
                                             std::vector<NodePtr>& out) const {
    std::shared_lock locker{mutex_};

    std::vector<NodeID> node_ids;
    Status status =
        runtime_index_.list_nodes_by_resource_pool(pool_name, node_ids);
    RETURN_IF_INVALID_STATUS(status)

    out.clear();
    out.reserve(node_ids.size());
    for (const NodeID& node_id : node_ids) {
        NodePtr node;
        status = meta_store_->get_node(node_id, node);
        RETURN_IF_INVALID_STATUS(status)
        if (node != nullptr) {
            out.push_back(node);
        }
    }
    return Status::OK();
}

Status SdmStore::list_nodes(std::vector<NodePtr>& out) const {
    std::shared_lock locker{mutex_};
    return meta_store_->list_nodes(out);
}

Status SdmStore::get_shard_route(const ShardID& shard_id,
                                 std::shared_ptr<ShardRoute>& out) const {
    std::shared_lock locker{mutex_};
    return meta_store_->get_shard_route(shard_id, out);
}

Status SdmStore::put_shard_route(const ShardRoute& route) {
    std::unique_lock locker{mutex_};
    Status status = meta_store_->upsert_shard_route(route);
    RETURN_IF_INVALID_STATUS(status)
    return runtime_index_.put_shard_route(route);
}

Status SdmStore::delete_shard_route(const ShardID& shard_id) {
    std::unique_lock locker{mutex_};
    Status status = meta_store_->delete_shard_route(shard_id);
    RETURN_IF_INVALID_STATUS(status)
    return runtime_index_.delete_shard_route(shard_id);
}

Status SdmStore::del_shard_route_entry(const ShardID& shard_id,
                                       const ReplicaKey& replica_key) {
    std::unique_lock locker{mutex_};
    ShardRoutePtr route;
    Status status = meta_store_->get_shard_route(shard_id, route);
    RETURN_IF_INVALID_STATUS(status)
    if (route == nullptr) {
        return Status::OK();
    }

    func::ad_erase_if(route->replicas, [&replica_key](const RouteEntry& entry) {
        return entry.replica_id.table_id == replica_key.table_id &&
               entry.replica_id.shard_index == replica_key.shard_index &&
               entry.replica_id.replica_index == replica_key.replica_index;
    });
    status = meta_store_->upsert_shard_route(*route);
    RETURN_IF_INVALID_STATUS(status)
    return runtime_index_.put_shard_route(*route);
}

Status SdmStore::put_node(const Node& node) {
    std::unique_lock locker{mutex_};

    NodePtr old_ptr;
    Status status = meta_store_->get_node(node.id, old_ptr);
    RETURN_IF_INVALID_STATUS(status)

    status = meta_store_->upsert_node(node);
    RETURN_IF_INVALID_STATUS(status)

    return runtime_index_.on_node_upsert(old_ptr.get(), node);
}

Status SdmStore::get_node(const NodeID& node_id, NodePtr& out) const {
    std::shared_lock locker{mutex_};
    return meta_store_->get_node(node_id, out);
}

Status SdmStore::get_resource_pool(const std::string& name,
                                   std::shared_ptr<ResourcePool>& out) const {
    std::shared_lock locker{mutex_};
    return meta_store_->get_resource_pool(name, out);
}

Status SdmStore::list_resource_pools(
    std::vector<std::shared_ptr<ResourcePool>>& pools) const {
    std::shared_lock locker{mutex_};
    return meta_store_->list_resource_pools(pools);
}

Status SdmStore::get_replica(const ReplicaID& replica_key,
                             ReplicaPtr& out) const {
    std::shared_lock locker{mutex_};
    return meta_store_->get_replica(replica_key, out);
}

Status SdmStore::put_replica(const Replica& replica) {
    std::unique_lock locker{mutex_};

    ReplicaPtr old_ptr;
    Status status = meta_store_->get_replica(replica.replica_id, old_ptr);
    RETURN_IF_INVALID_STATUS(status)

    status = meta_store_->upsert_replica(replica);
    RETURN_IF_INVALID_STATUS(status)

    return runtime_index_.on_replica_upsert(old_ptr.get(), replica);
}

Status SdmStore::del_replica(const ReplicaID& replica_key) {
    std::unique_lock locker{mutex_};

    ReplicaPtr old_ptr;
    Status status = meta_store_->get_replica(replica_key, old_ptr);
    RETURN_IF_INVALID_STATUS(status)
    if (old_ptr == nullptr) {
        return Status::OK();
    }

    status = meta_store_->delete_replica(replica_key);
    RETURN_IF_INVALID_STATUS(status)

    return runtime_index_.on_replica_delete(*old_ptr);
}

Status SdmStore::list_replicas_by_shard(const ShardID& shard_id,
                                        std::vector<ReplicaPtr>& out) const {
    std::shared_lock locker{mutex_};

    std::vector<ReplicaID> replica_ids;
    Status status =
        runtime_index_.list_replicas_by_shard(shard_id, replica_ids);
    RETURN_IF_INVALID_STATUS(status)

    out.clear();
    out.reserve(replica_ids.size());
    for (const ReplicaID& replica_id : replica_ids) {
        ReplicaPtr replica;
        status = meta_store_->get_replica(replica_id, replica);
        RETURN_IF_INVALID_STATUS(status)
        if (replica != nullptr) {
            out.push_back(replica);
        }
    }
    return Status::OK();
}

Status SdmStore::list_replicas_by_node(NodeID node_id,
                                       std::vector<ReplicaPtr>& out) const {
    std::shared_lock locker{mutex_};

    std::vector<ReplicaID> replica_ids;
    Status status = runtime_index_.list_replicas_by_node(node_id, replica_ids);
    RETURN_IF_INVALID_STATUS(status)

    out.clear();
    out.reserve(replica_ids.size());
    for (const ReplicaID& replica_id : replica_ids) {
        ReplicaPtr replica;
        status = meta_store_->get_replica(replica_id, replica);
        RETURN_IF_INVALID_STATUS(status)
        if (replica != nullptr) {
            out.push_back(replica);
        }
    }
    return Status::OK();
}

Status SdmStore::delete_table(TableID table_id) {
    std::unique_lock locker{mutex_};

    TablePtr old_ptr;
    Status status = meta_store_->get_table(table_id, old_ptr);
    RETURN_IF_INVALID_STATUS(status)
    if (old_ptr == nullptr) {
        return Status::OK();
    }

    status = meta_store_->delete_table(table_id);
    RETURN_IF_INVALID_STATUS(status)

    return runtime_index_.on_table_delete(*old_ptr);
}

Status SdmStore::rebuild_runtime_index() {
    runtime_index_ = SdmRuntimeIndex();

    std::vector<TablePtr> tables;
    Status status = meta_store_->list_tables(tables);
    RETURN_IF_INVALID_STATUS(status)
    for (const TablePtr& table : tables) {
        if (!table) continue;
        status = runtime_index_.on_table_upsert(nullptr, *table);
        RETURN_IF_INVALID_STATUS(status)
    }

    std::vector<NodePtr> nodes;
    status = meta_store_->list_nodes(nodes);
    RETURN_IF_INVALID_STATUS(status)
    for (const NodePtr& node : nodes) {
        if (!node) continue;
        status = runtime_index_.on_node_upsert(nullptr, *node);
        RETURN_IF_INVALID_STATUS(status)
    }

    std::vector<ReplicaPtr> replicas;
    status = meta_store_->list_replicas(replicas);
    RETURN_IF_INVALID_STATUS(status)
    for (const ReplicaPtr& replica : replicas) {
        if (!replica) continue;
        status = runtime_index_.on_replica_upsert(nullptr, *replica);
        RETURN_IF_INVALID_STATUS(status)
    }

    std::vector<ShardRoutePtr> routes;
    status = meta_store_->list_shard_routes(routes);
    RETURN_IF_INVALID_STATUS(status)
    for (const ShardRoutePtr& route : routes) {
        if (route != nullptr) {
            status = runtime_index_.put_shard_route(*route);
            RETURN_IF_INVALID_STATUS(status)
        }
    }
    return Status::OK();
}

}  // namespace adviskv::sdm
