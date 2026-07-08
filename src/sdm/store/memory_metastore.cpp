#include "sdm/store/memory_metastore.h"

#include <memory>
#include <utility>

#include "common/define.h"

namespace adviskv::sdm {

std::unique_ptr<ISdmMetaStore> MemoryMetaStore::clone_memory_snapshot() const {
    auto copied = std::make_unique<MemoryMetaStore>();
    for (const auto& [id, table] : tables_) {
        copied->tables_[id] = std::make_shared<Table>(*table);
    }
    for (const auto& [id, replica] : replicas_) {
        copied->replicas_[id] = std::make_shared<Replica>(*replica);
    }
    for (const auto& [name, pool] : resource_pools_) {
        copied->resource_pools_[name] = std::make_shared<ResourcePool>(*pool);
    }
    for (const auto& [id, group] : replica_groups_) {
        copied->replica_groups_[id] = std::make_shared<ReplicaGroup>(*group);
    }
    return copied;
}

Status MemoryMetaStore::commit_memory_snapshot(std::unique_ptr<ISdmMetaStore> next_memory_store) {
    RETURN_IF_NULLPTR(next_memory_store, "next_memory_store is nullptr");

    auto* next = dynamic_cast<MemoryMetaStore*>(next_memory_store.get());
    RETURN_IF_NULLPTR(next, "next_memory_store should be MemoryMetaStore");

    tables_ = std::move(next->tables_);
    replicas_ = std::move(next->replicas_);
    resource_pools_ = std::move(next->resource_pools_);
    replica_groups_ = std::move(next->replica_groups_);
    return Status::OK();
}

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

Status MemoryMetaStore::delete_table(TableID table_id) {
    tables_.erase(table_id);
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

Status MemoryMetaStore::upsert_replicas(const std::vector<Replica>& replicas) {
    for (const Replica& replica : replicas) {
        replicas_[replica.replica_id] = std::make_shared<Replica>(replica);
    }

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

Status MemoryMetaStore::get_resource_pool(const std::string& name, ResourcePoolPtr& out) const {
    auto it = resource_pools_.find(name);
    if (it == resource_pools_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MemoryMetaStore::list_resource_pools(std::vector<ResourcePoolPtr>& out) const {
    out.clear();
    out.reserve(resource_pools_.size());
    for (const auto& [_, pool] : resource_pools_) {
        out.push_back(pool);
    }
    return Status::OK();
}

Status MemoryMetaStore::delete_resource_pool(const std::string& name) {
    resource_pools_.erase(name);
    return Status::OK();
}

Status MemoryMetaStore::upsert_replica_group(const ReplicaGroup& group) {
    replica_groups_[group.shard_id] = std::make_shared<ReplicaGroup>(group);
    return Status::OK();
}

Status MemoryMetaStore::delete_replica_group(const ShardID& shard_id) {
    replica_groups_.erase(shard_id);
    return Status::OK();
}

Status MemoryMetaStore::get_replica_group(const ShardID& shard_id, ReplicaGroupPtr& out) const {
    auto it = replica_groups_.find(shard_id);
    if (it == replica_groups_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MemoryMetaStore::list_replica_groups(std::vector<ReplicaGroupPtr>& out) const {
    out.clear();
    out.reserve(replica_groups_.size());
    for (const auto& [_, group] : replica_groups_) {
        UNUSED(_);
        out.push_back(group);
    }
    return Status::OK();
}

}  // namespace adviskv::sdm