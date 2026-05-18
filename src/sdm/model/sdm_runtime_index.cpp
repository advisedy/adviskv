#include "sdm/model/sdm_runtime_index.h"

#include <algorithm>

#include "common/define.h"
#include "common/func.h"
#include "common/status.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

namespace {

TableNameKey make_table_name_key(const Table& table) {
    return TableNameKey{
        .db_name = table.spec.db_name,
        .table_name = table.spec.table_name,
    };
}

ShardKey make_shard_key(const ReplicaKey& key) {
    return ShardKey{
        .table_id = key.table_id,
        .shard_index = key.shard_index,
    };
}

void cleanup_empty_node_set(
    std::unordered_map<NodeID, std::unordered_set<ReplicaKey, ReplicaKeyHash>>&
        index,
    const NodeID& node_id) {
    auto it = index.find(node_id);
    if (it != index.end() && it->second.empty()) {
        index.erase(it);
    }
}

void cleanup_empty_shard_set(
    std::unordered_map<ShardKey, std::unordered_set<ReplicaKey, ReplicaKeyHash>,
                       ShardKeyHash>& index,
    const ShardKey& shard_key) {
    auto it = index.find(shard_key);
    if (it != index.end() && it->second.empty()) {
        index.erase(it);
    }
}

void cleanup_empty_pool_set(
    std::unordered_map<std::string, std::unordered_set<NodeID>>& index,
    const std::string& pool_name) {
    auto it = index.find(pool_name);
    if (it != index.end() && it->second.empty()) {
        index.erase(it);
    }
}

}  // namespace

Status SdmRuntimeIndex::on_table_upsert(const Table* old_table,
                                        const Table& new_table) {
    if (old_table != nullptr) {
        table_name_index_.erase(make_table_name_key(*old_table));
    }
    if (new_table.state.desired == TableDesired::PRESENT) return Status::OK();
    table_name_index_[make_table_name_key(new_table)] = new_table.table_id;
    return Status::OK();
}

Status SdmRuntimeIndex::on_node_upsert(const Node* old_node,
                                       const Node& new_node) {
    if (old_node != nullptr) {
        const std::string& old_pool = old_node->spec.resource_pool;
        auto pool_it = pool_nodes_index_.find(old_pool);
        if (pool_it != pool_nodes_index_.end()) {
            pool_it->second.erase(old_node->id);
            cleanup_empty_pool_set(pool_nodes_index_, old_pool);
        }
        node_pool_index_.erase(old_node->id);
    }

    pool_nodes_index_[new_node.spec.resource_pool].insert(new_node.id);
    node_pool_index_[new_node.id] = new_node.spec.resource_pool;
    return Status::OK();
}

Status SdmRuntimeIndex::on_replica_upsert(const Replica* old_replica,
                                          const Replica& new_replica) {
    if (old_replica != nullptr) {
        const ShardKey old_shard_key = make_shard_key(old_replica->replica_id);
        auto shard_it = shard_replicas_index_.find(old_shard_key);
        if (shard_it != shard_replicas_index_.end()) {
            shard_it->second.erase(old_replica->replica_id);
            cleanup_empty_shard_set(shard_replicas_index_, old_shard_key);
        }

        if (!old_replica->spec.assign_node_id.empty()) {
            auto node_it =
                node_replicas_index_.find(old_replica->spec.assign_node_id);
            if (node_it != node_replicas_index_.end()) {
                node_it->second.erase(old_replica->replica_id);
                cleanup_empty_node_set(node_replicas_index_,
                                       old_replica->spec.assign_node_id);
            }
        }
    }

    const ShardKey new_shard_key = make_shard_key(new_replica.replica_id);
    shard_replicas_index_[new_shard_key].insert(new_replica.replica_id);

    if (!new_replica.spec.assign_node_id.empty()) {
        node_replicas_index_[new_replica.spec.assign_node_id].insert(
            new_replica.replica_id);
    }
    return Status::OK();
}

Status SdmRuntimeIndex::on_table_delete(const Table& table) {
    table_name_index_.erase(make_table_name_key(table));
    return Status::OK();
}

Status SdmRuntimeIndex::on_node_delete(const Node& node) {
    auto pool_it = pool_nodes_index_.find(node.spec.resource_pool);
    if (pool_it != pool_nodes_index_.end()) {
        pool_it->second.erase(node.id);
        cleanup_empty_pool_set(pool_nodes_index_, node.spec.resource_pool);
    }
    node_pool_index_.erase(node.id);
    return Status::OK();
}

Status SdmRuntimeIndex::on_replica_delete(const Replica& replica) {
    const ShardKey shard_key = make_shard_key(replica.replica_id);
    auto shard_it = shard_replicas_index_.find(shard_key);
    if (shard_it != shard_replicas_index_.end()) {
        shard_it->second.erase(replica.replica_id);
        cleanup_empty_shard_set(shard_replicas_index_, shard_key);
    }

    if (!replica.spec.assign_node_id.empty()) {
        auto node_it = node_replicas_index_.find(replica.spec.assign_node_id);
        if (node_it != node_replicas_index_.end()) {
            node_it->second.erase(replica.replica_id);
            cleanup_empty_node_set(node_replicas_index_,
                                   replica.spec.assign_node_id);
        }
    }

    return Status::OK();
}

Status SdmRuntimeIndex::find_table_by_name(const std::string& db_name,
                                           const std::string& table_name,
                                           TableID& table_id) const {
    TableNameKey key{
        .db_name = db_name,
        .table_name = table_name,
    };
    auto it = table_name_index_.find(key);
    if (it == table_name_index_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND, "table name not found"};
    }
    table_id = it->second;
    return Status::OK();
}

Status SdmRuntimeIndex::list_nodes_by_resource_pool(
    const std::string& pool_name, std::vector<NodeID>& out) const {
    out.clear();
    auto it = pool_nodes_index_.find(pool_name);
    if (it == pool_nodes_index_.end()) {
        return Status::OK();
    }
    out.insert(out.end(), it->second.begin(), it->second.end());
    return Status::OK();
}

Status SdmRuntimeIndex::list_replicas_by_shard(
    const ShardID& shard_id, std::vector<ReplicaKey>& out) const {
    out.clear();
    auto it = shard_replicas_index_.find(shard_id);
    if (it == shard_replicas_index_.end()) {
        return Status::OK();
    }
    out.insert(out.end(), it->second.begin(), it->second.end());
    return Status::OK();
}

Status SdmRuntimeIndex::list_replicas_by_node(
    const NodeID& node_id, std::vector<ReplicaKey>& out) const {
    out.clear();
    auto it = node_replicas_index_.find(node_id);
    if (it == node_replicas_index_.end()) {
        return Status::OK();
    }
    out.insert(out.end(), it->second.begin(), it->second.end());
    return Status::OK();
}

Status SdmRuntimeIndex::get_shard_route(const ShardID& shard_id,
                                        ShardRoutePtr& out) const {
    auto it = shard_route_cache_.find(shard_id);
    if (it == shard_route_cache_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status SdmRuntimeIndex::put_shard_route(const ShardRoute& route) {
    shard_route_cache_[route.shard_id] = std::make_shared<ShardRoute>(route);
    return Status::OK();
}

Status SdmRuntimeIndex::delete_shard_route(const ShardID& shard_id) {
    shard_route_cache_.erase(shard_id);
    return Status::OK();
}

Status SdmRuntimeIndex::del_shard_route_entry(const ShardID& shard_id,
                                              const ReplicaKey& replica_id) {
    auto it = shard_route_cache_.find(shard_id);
    if (it == shard_route_cache_.end() || !(it->second)) {
        return Status::OK();
    }

    auto& replicas = it->second->replicas;
    func::ad_erase_if(replicas, [&replica_id](const RouteEntry& entry) {
        return entry.replica_id.table_id == replica_id.table_id &&
               entry.replica_id.shard_index == replica_id.shard_index &&
               entry.replica_id.replica_index == replica_id.replica_index;
    });
    return Status::OK();
}

}  // namespace adviskv::sdm
