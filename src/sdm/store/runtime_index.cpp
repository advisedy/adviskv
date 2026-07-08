#include "sdm/store/runtime_index.h"

#include <algorithm>
#include <memory>

#include "common/define.h"
#include "common/status.h"
#include "sdm/model/model.h"
namespace adviskv::sdm {

namespace {

TableNameKey make_table_name_key(const Table& table) {
    TableNameKey key;
    key.db_name = table.spec.db_name;
    key.table_name = table.spec.table_name;
    return key;
}

ShardKey make_shard_key(const ReplicaKey& key) { return ShardKey{key.table_id, key.shard_index}; }

void cleanup_empty_node_set(std::unordered_map<NodeID, std::unordered_set<ReplicaKey, ReplicaKeyHash>>& index,
                            const NodeID& node_id) {
    auto it = index.find(node_id);
    if (it != index.end() && it->second.empty()) {
        index.erase(it);
    }
}

void cleanup_empty_shard_set(
        std::unordered_map<ShardKey, std::unordered_set<ReplicaKey, ReplicaKeyHash>, ShardKeyHash>& index,
        const ShardKey& shard_key) {
    auto it = index.find(shard_key);
    if (it != index.end() && it->second.empty()) {
        index.erase(it);
    }
}

void cleanup_empty_pool_set(std::unordered_map<std::string, std::unordered_set<NodeID>>& index,
                            const std::string& pool_name) {
    auto it = index.find(pool_name);
    if (it != index.end() && it->second.empty()) {
        index.erase(it);
    }
}

}  // namespace

SdmRuntimeIndex::SdmRuntimeIndex(const SdmRuntimeIndex& other) { *this = other; }

SdmRuntimeIndex& SdmRuntimeIndex::operator=(const SdmRuntimeIndex& other) {
    if (this == &other) {
        return *this;
    }

    table_name_index_ = other.table_name_index_;
    pool_nodes_index_ = other.pool_nodes_index_;
    shard_replicas_index_ = other.shard_replicas_index_;
    node_replicas_index_ = other.node_replicas_index_;
    node_pool_index_ = other.node_pool_index_;

    return *this;
}

void SdmRuntimeIndex::clear() {
    table_name_index_.clear();
    pool_nodes_index_.clear();
    shard_replicas_index_.clear();
    node_replicas_index_.clear();
    node_pool_index_.clear();
}

Status SdmRuntimeIndex::on_table_upsert(const Table* old_table, const Table& new_table) {
    if (old_table != nullptr) {
        table_name_index_.erase(make_table_name_key(*old_table));
    }
    if (new_table.state.desired == TableDesired::ABSENT) return Status::OK();
    table_name_index_[make_table_name_key(new_table)] = new_table.table_id;
    return Status::OK();
}

Status SdmRuntimeIndex::on_node_upsert(const Node* old_node, const Node& new_node) {
    if (old_node != nullptr) {
        const std::string& old_pool = old_node->meta.resource_pool;
        auto pool_it = pool_nodes_index_.find(old_pool);
        if (pool_it != pool_nodes_index_.end()) {
            pool_it->second.erase(old_node->id);
            cleanup_empty_pool_set(pool_nodes_index_, old_pool);
        }
        node_pool_index_.erase(old_node->id);
    }

    pool_nodes_index_[new_node.meta.resource_pool].insert(new_node.id);
    node_pool_index_[new_node.id] = new_node.meta.resource_pool;
    return Status::OK();
}

Status SdmRuntimeIndex::on_replica_upsert(const Replica* old_replica, const Replica& new_replica) {
    if (old_replica != nullptr) {
        const ShardKey old_shard_key = make_shard_key(old_replica->replica_id);
        auto shard_it = shard_replicas_index_.find(old_shard_key);
        if (shard_it != shard_replicas_index_.end()) {
            shard_it->second.erase(old_replica->replica_id);
            cleanup_empty_shard_set(shard_replicas_index_, old_shard_key);
        }

        if (!old_replica->spec.assign_node_id.empty()) {
            auto node_it = node_replicas_index_.find(old_replica->spec.assign_node_id);
            if (node_it != node_replicas_index_.end()) {
                node_it->second.erase(old_replica->replica_id);
                cleanup_empty_node_set(node_replicas_index_, old_replica->spec.assign_node_id);
            }
        }
    }

    const ShardKey new_shard_key = make_shard_key(new_replica.replica_id);
    shard_replicas_index_[new_shard_key].insert(new_replica.replica_id);

    if (!new_replica.spec.assign_node_id.empty()) {
        node_replicas_index_[new_replica.spec.assign_node_id].insert(new_replica.replica_id);
    }
    return Status::OK();
}

Status SdmRuntimeIndex::on_table_delete(const Table& table) {
    table_name_index_.erase(make_table_name_key(table));
    return Status::OK();
}

Status SdmRuntimeIndex::on_node_delete(const Node& node) {
    auto pool_it = pool_nodes_index_.find(node.meta.resource_pool);
    if (pool_it != pool_nodes_index_.end()) {
        pool_it->second.erase(node.id);
        cleanup_empty_pool_set(pool_nodes_index_, node.meta.resource_pool);
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
            cleanup_empty_node_set(node_replicas_index_, replica.spec.assign_node_id);
        }
    }

    return Status::OK();
}

Status SdmRuntimeIndex::find_table_by_name(const std::string& db_name, const std::string& table_name,
                                           TableID& table_id) const {
    TableNameKey key;
    key.db_name = db_name;
    key.table_name = table_name;
    auto it = table_name_index_.find(key);
    if (it == table_name_index_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND, "table name not found"};
    }
    table_id = it->second;
    return Status::OK();
}

Status SdmRuntimeIndex::list_nodes_by_resource_pool(const std::string& pool_name, std::vector<NodeID>& out) const {
    out.clear();
    auto it = pool_nodes_index_.find(pool_name);
    if (it == pool_nodes_index_.end()) {
        return Status::OK();
    }
    out.insert(out.end(), it->second.begin(), it->second.end());
    return Status::OK();
}

Status SdmRuntimeIndex::list_replicas_by_shard(const ShardID& shard_id, std::vector<ReplicaKey>& out) const {
    out.clear();
    auto it = shard_replicas_index_.find(shard_id);
    if (it == shard_replicas_index_.end()) {
        return Status::OK();
    }
    out.insert(out.end(), it->second.begin(), it->second.end());
    return Status::OK();
}

Status SdmRuntimeIndex::list_replicas_by_node(const NodeID& node_id, std::vector<ReplicaKey>& out) const {
    out.clear();
    auto it = node_replicas_index_.find(node_id);
    if (it == node_replicas_index_.end()) {
        return Status::OK();
    }
    out.insert(out.end(), it->second.begin(), it->second.end());
    return Status::OK();
}

std::unique_ptr<SdmRuntimeIndex> SdmRuntimeIndex::clone() const { return std::make_unique<SdmRuntimeIndex>(*this); }

}  // namespace adviskv::sdm