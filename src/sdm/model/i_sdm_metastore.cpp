#include "sdm/model/i_sdm_metastore.h"

namespace adviskv::sdm {

Status MemoryMetaStore::upsert_table(const Table& table) {
    tables_[table.table_id] = std::make_shared<Table>(table);
    return Status::OK();
}

Status MemoryMetaStore::get_table(TableID table_id, TablePtr& out) const {
    auto it = tables_.find(table_id);
    if (it == tables_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MemoryMetaStore::list_tables(std::vector<TablePtr>& out) const {
    out.clear();
    out.reserve(tables_.size());
    for (const auto& [_, table] : tables_) {
        out.push_back(table);
    }
    return Status::OK();
}

Status MemoryMetaStore::upsert_node(const Node& node) {
    nodes_[node.id] = std::make_shared<Node>(node);
    return Status::OK();
}

Status MemoryMetaStore::get_node(const NodeID& node_id, NodePtr& out) const {
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MemoryMetaStore::list_nodes(std::vector<NodePtr>& out) const {
    out.clear();
    out.reserve(nodes_.size());
    for (const auto& [_, node] : nodes_) {
        out.push_back(node);
    }
    return Status::OK();
}

Status MemoryMetaStore::upsert_replica(const Replica& replica) {
    replicas_[replica.replica_id] = std::make_shared<Replica>(replica);
    return Status::OK();
}

Status MemoryMetaStore::get_replica(const ReplicaID& key, ReplicaPtr& out) const {
    auto it = replicas_.find(key);
    if (it == replicas_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MemoryMetaStore::delete_replica(const ReplicaID& key) {
    replicas_.erase(key);
    return Status::OK();
}

Status MemoryMetaStore::list_replicas(std::vector<ReplicaPtr>& out) const {
    out.clear();
    out.reserve(replicas_.size());
    for (const auto& [_, replica] : replicas_) {
        out.push_back(replica);
    }
    return Status::OK();
}

Status MemoryMetaStore::upsert_resource_pool(const ResourcePool& pool) {
    resource_pools_[pool.name] = std::make_shared<ResourcePool>(pool);
    return Status::OK();
}

Status MemoryMetaStore::get_resource_pool(const std::string& name,
                                          ResourcePoolPtr& out) const {
    auto it = resource_pools_.find(name);
    if (it == resource_pools_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MemoryMetaStore::list_resource_pools(
    std::vector<ResourcePoolPtr>& out) const {
    out.clear();
    out.reserve(resource_pools_.size());
    for (const auto& [_, pool] : resource_pools_) {
        out.push_back(pool);
    }
    return Status::OK();
}

}  // namespace adviskv::sdm
