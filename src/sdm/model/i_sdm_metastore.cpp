#include "sdm/model/i_sdm_metastore.h"

#include <memory>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "sdm/model/store.h"
#include "sdm/persist/sdm_persist_engine.h"

namespace adviskv::sdm {

std::unique_ptr<ISdmMetaStore> MemoryMetaStore::clone() const {
    auto copied = std::make_unique<MemoryMetaStore>();
    for (const auto& [id, table] : tables_) {
        copied->tables_[id] = std::make_shared<Table>(*table);
    }
    for (const auto& [id, node] : nodes_) {
        copied->nodes_[id] = std::make_shared<Node>(*node);
    }
    for (const auto& [id, replica] : replicas_) {
        copied->replicas_[id] = std::make_shared<Replica>(*replica);
    }
    for (const auto& [name, pool] : resource_pools_) {
        copied->resource_pools_[name] = std::make_shared<ResourcePool>(*pool);
    }
    for (const auto& [id, route] : shard_routes_) {
        copied->shard_routes_[id] = std::make_shared<ShardRoute>(*route);
    }
    return copied;
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

Status MemoryMetaStore::get_replica(const ReplicaID& key,
                                    ReplicaPtr& out) const {
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

Status MemoryMetaStore::delete_resource_pool(const std::string& name) {
    resource_pools_.erase(name);
    return Status::OK();
}

Status MemoryMetaStore::upsert_shard_route(const ShardRoute& route) {
    shard_routes_[route.shard_id] = std::make_shared<ShardRoute>(route);
    return Status::OK();
}

Status MemoryMetaStore::get_shard_route(const ShardID& shard_id,
                                        ShardRoutePtr& out) const {
    auto it = shard_routes_.find(shard_id);
    if (it == shard_routes_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status MemoryMetaStore::delete_shard_route(const ShardID& shard_id) {
    shard_routes_.erase(shard_id);
    return Status::OK();
}

Status MemoryMetaStore::list_shard_routes(
    std::vector<ShardRoutePtr>& out) const {
    out.clear();
    out.reserve(shard_routes_.size());
    for (const auto& [_, route] : shard_routes_) {
        out.push_back(route);
    }
    return Status::OK();
}

PersistentMetaStore::PersistentMetaStore(std::filesystem::path data_dir)
    : inner_(std::make_unique<MemoryMetaStore>()),
      persist_engine_(data_dir.string()) {
    persist_engine_.init();
    IGNORE_RESULT(load());
}

PersistentMetaStore::PersistentMetaStore(std::unique_ptr<ISdmMetaStore> inner,
                                         std::filesystem::path data_dir)
    : inner_(std::move(inner)), persist_engine_(data_dir.string()) {
    persist_engine_.init();
    IGNORE_RESULT(load());
}

std::unique_ptr<ISdmMetaStore> PersistentMetaStore::clone() const {
    // SdmPersistEngine persist_engine{persist_engine_};
    // return PersistentMetaStore{std::move(inner_->clone()), persist_engine};
    //TODO 
    return inner_->clone();
}

Status PersistentMetaStore::load() {
    SdmPersistedRecord record;
    Status status = persist_engine_.load_sdm_meta(record);
    RETURN_IF_INVALID_STATUS(status)

    for (const auto& [_, table] : record.tables) {
        UNUSED(_);
        inner_->upsert_table(table);
    }
    for (const auto& [_, node] : record.nodes) {
        UNUSED(_);
        inner_->upsert_node(node);
    }
    for (const auto& [_, replica] : record.replicas) {
        UNUSED(_);
        inner_->upsert_replica(replica);
    }
    for (const auto& [_, pool] : record.resource_pools) {
        UNUSED(_);
        inner_->upsert_resource_pool(pool);
    }
    for (const auto& [_, route] : record.shard_routes) {
        UNUSED(_);
        inner_->upsert_shard_route(route);
    }

    return Status::OK();
}

Status PersistentMetaStore::build_record_from_store(
    const ISdmMetaStore& store, SdmPersistedRecord& record) const {
    record = {};

    std::vector<TablePtr> tables;
    RETURN_IF_INVALID_STATUS(store.list_tables(tables))
    for (const auto& t : tables) {
        record.tables[t->table_id] = *t;
    }

    std::vector<NodePtr> nodes;
    RETURN_IF_INVALID_STATUS(store.list_nodes(nodes))
    for (const auto& n : nodes) {
        record.nodes[n->id] = *n;
    }

    std::vector<ReplicaPtr> replicas;
    RETURN_IF_INVALID_STATUS(store.list_replicas(replicas))
    for (const auto& r : replicas) {
        record.replicas[r->replica_id] = *r;
    }

    std::vector<ResourcePoolPtr> pools;
    RETURN_IF_INVALID_STATUS(store.list_resource_pools(pools))
    for (const auto& p : pools) {
        record.resource_pools[p->name] = *p;
    }

    std::vector<ShardRoutePtr> routes;
    RETURN_IF_INVALID_STATUS(store.list_shard_routes(routes))
    for (const auto& r : routes) {
        record.shard_routes[r->shard_id] = *r;
    }

    return Status::OK();
}

Status PersistentMetaStore::build_record(SdmPersistedRecord& record) const {
    return build_record_from_store(*inner_, record);
}

Status PersistentMetaStore::persist_record(const SdmPersistedRecord& record) {
    return persist_engine_.save_sdm_meta(record);
}

Status PersistentMetaStore::persist() {
    SdmPersistedRecord record;
    RETURN_IF_INVALID_STATUS(build_record(record))
    return persist_record(record);
}

Status PersistentMetaStore::commit_with(
    const std::function<Status(ISdmMetaStore&)>& mutate) {
    std::unique_ptr<ISdmMetaStore> next_inner = inner_->clone();
    RETURN_IF_INVALID_STATUS(mutate(*next_inner))

    SdmPersistedRecord next;
    RETURN_IF_INVALID_STATUS(build_record_from_store(*next_inner, next))
    RETURN_IF_INVALID_STATUS(persist_record(next))

    inner_ = std::move(next_inner);
    return Status::OK();
}

Status PersistentMetaStore::upsert_table(const Table& table) {
    return commit_with(
        [&table](ISdmMetaStore& store) { return store.upsert_table(table); });
}

Status PersistentMetaStore::get_table(TableID table_id, TablePtr& out) const {
    return inner_->get_table(table_id, out);
}

Status PersistentMetaStore::delete_table(TableID table_id) {
    return commit_with([table_id](ISdmMetaStore& store) {
        return store.delete_table(table_id);
    });
}

Status PersistentMetaStore::list_tables(std::vector<TablePtr>& out) const {
    return inner_->list_tables(out);
}

Status PersistentMetaStore::upsert_node(const Node& node) {
    return commit_with(
        [&node](ISdmMetaStore& store) { return store.upsert_node(node); });
}

Status PersistentMetaStore::get_node(const NodeID& node_id,
                                     NodePtr& out) const {
    return inner_->get_node(node_id, out);
}

Status PersistentMetaStore::list_nodes(std::vector<NodePtr>& out) const {
    return inner_->list_nodes(out);
}

Status PersistentMetaStore::upsert_replica(const Replica& replica) {
    return commit_with([&replica](ISdmMetaStore& store) {
        return store.upsert_replica(replica);
    });
}

Status PersistentMetaStore::get_replica(const ReplicaID& key,
                                        ReplicaPtr& out) const {
    return inner_->get_replica(key, out);
}

Status PersistentMetaStore::delete_replica(const ReplicaID& key) {
    return commit_with(
        [&key](ISdmMetaStore& store) { return store.delete_replica(key); });
}

Status PersistentMetaStore::upsert_replicas(
    const std::vector<Replica>& replicas) {
    return commit_with([&replicas](ISdmMetaStore& store) {
        return store.upsert_replicas(replicas);
    });
}

Status PersistentMetaStore::list_replicas(std::vector<ReplicaPtr>& out) const {
    return inner_->list_replicas(out);
}

Status PersistentMetaStore::upsert_resource_pool(const ResourcePool& pool) {
    return commit_with([&pool](ISdmMetaStore& store) {
        return store.upsert_resource_pool(pool);
    });
}

Status PersistentMetaStore::get_resource_pool(const std::string& name,
                                              ResourcePoolPtr& out) const {
    return inner_->get_resource_pool(name, out);
}

Status PersistentMetaStore::list_resource_pools(
    std::vector<ResourcePoolPtr>& out) const {
    return inner_->list_resource_pools(out);
}

Status PersistentMetaStore::delete_resource_pool(const std::string& name) {
    return commit_with([&name](ISdmMetaStore& store) {
        return store.delete_resource_pool(name);
    });
}

Status PersistentMetaStore::upsert_shard_route(const ShardRoute& route) {
    return commit_with([&route](ISdmMetaStore& store) {
        return store.upsert_shard_route(route);
    });
}

Status PersistentMetaStore::get_shard_route(const ShardID& shard_id,
                                            ShardRoutePtr& out) const {
    return inner_->get_shard_route(shard_id, out);
}

Status PersistentMetaStore::delete_shard_route(const ShardID& shard_id) {
    return commit_with([&shard_id](ISdmMetaStore& store) {
        return store.delete_shard_route(shard_id);
    });
}

Status PersistentMetaStore::list_shard_routes(
    std::vector<ShardRoutePtr>& out) const {
    return inner_->list_shard_routes(out);
}

}  // namespace adviskv::sdm