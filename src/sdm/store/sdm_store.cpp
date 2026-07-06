#include "sdm/store/sdm_store.h"

#include <algorithm>
#include <filesystem>
#include <mutex>

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "sdm/model/model.h"
#include "sdm/store/memory_metastore.h"
#include "sdm/store/persistent_metastore.h"
#include "sdm/store/sdm_store_txn.h"

namespace adviskv::sdm {

SdmStore::SdmStore(SdmMetaStoreType type,
                   const std::string& persistent_data_dir)
    : SdmStore(type, std::make_unique<SdmRuntimeIndex>(), persistent_data_dir) {
}

SdmStore::SdmStore(SdmMetaStoreType type,
                   std::unique_ptr<SdmRuntimeIndex> index,
                   const std::string& persistent_data_dir)
    : runtime_index_(std::move(index)) {
    switch (type) {
        case SdmMetaStoreType::PERSISTENT:
            meta_store_ = std::make_unique<PersistentMetaStore>(
                std::filesystem::path(persistent_data_dir));
            break;
        case SdmMetaStoreType::MEMORY:
        default:
            meta_store_ = std::make_unique<MemoryMetaStore>();
            break;
    }
}

Status SdmStore::init() {
    std::unique_lock locker{mutex_};

    RETURN_IF_NULLPTR(meta_store_, "sdm meta store is nullptr")
    RETURN_IF_NULLPTR(runtime_index_, "sdm runtime index is nullptr")

    Status status = meta_store_->init();
    if (status.fail()) {
        LOG_ERROR("[SdmStore] metastore init failed: {}",
                  status.to_string());
        return status;
    }
    status = rebuild_runtime_index();
    if (status.fail()) {
        LOG_ERROR("[SdmStore] rebuild runtime index failed: {}",
                  status.to_string());
        return status;
    }
    return Status::OK();
}

SdmStore::SdmStore(std::unique_ptr<ISdmMetaStore> meta_store,
                   std::unique_ptr<SdmRuntimeIndex> runtime_index)
    : meta_store_(std::move(meta_store)),
      runtime_index_(std::move(runtime_index)) {}

Status SdmStore::read_with(
    const std::function<Status(const SdmStoreTxn&)>& func) const {
    if (func == nullptr) {
        return Status::INVALID_ARGUMENT("read_with fn is nullptr");
    }
    std::shared_lock locker{mutex_};
    RETURN_IF_NULLPTR(meta_store_, "sdm meta store is nullptr")
    RETURN_IF_NULLPTR(runtime_index_, "sdm runtime index is nullptr")
    // 这里通过设置func的参数是const SdmStoreTxn&，
    // 所以能保证txn在外部调用的时候用的也是const
    // 函数，不然这里this是const T*，没法构造出来txn
    SdmStoreTxn txn(const_cast<SdmStore*>(this));
    return func(txn);
}

Status SdmStore::write_with(const std::function<Status(SdmStoreTxn&)>& func) {
    if (func == nullptr) {
        return Status::INVALID_ARGUMENT("write_with fn is nullptr");
    }

    ADVISKV_METRICS_TIMER("sdm_store_write_with");
    ADVISKV_METRICS_COUNTER("sdm_store_write_with_request");

    std::unique_lock locker{mutex_};
    RETURN_IF_NULLPTR(meta_store_, "sdm meta store is nullptr")
    RETURN_IF_NULLPTR(runtime_index_, "sdm runtime index is nullptr")

    std::unique_ptr<ISdmMetaStore> clone_meta =
        meta_store_->clone_memory_snapshot();
    if (clone_meta == nullptr) {
        return Status::ERROR("write_with clone meta failed");
    }
    std::unique_ptr<SdmRuntimeIndex> clone_index = runtime_index_->clone();
    if (clone_index == nullptr) {
        return Status::ERROR("write_with clone runtime_index failed");
    }

    SdmStore clone_store(std::move(clone_meta), std::move(clone_index));
    SdmStoreTxn txn(&clone_store);
    Status status = func(txn);
    if (status.fail()) {
        return status;
    }

    if (clone_store.meta_dirty_) {
        status = meta_store_->commit_memory_snapshot(
            std::move(clone_store.meta_store_));
        RETURN_IF_INVALID_STATUS(status)
    }
    if (clone_store.runtime_dirty_) {
        runtime_index_ = std::move(clone_store.runtime_index_);
    }
    return Status::OK();
}

void SdmStore::mark_meta_dirty() {
    meta_dirty_ = true;
}

void SdmStore::mark_runtime_dirty() {
    runtime_dirty_ = true;
}

//////////////////////////////////////////////////////////////////////

Status SdmStore::put_table(const Table& table) {
    TablePtr old_ptr;
    RETURN_IF_INVALID_STATUS(meta_store_->get_table(table.table_id, old_ptr))
    RETURN_IF_INVALID_STATUS(meta_store_->upsert_table(table))
    RETURN_IF_INVALID_STATUS(maybe_repair_runtime_index(
        runtime_index_->on_table_upsert(old_ptr.get(), table)))
    mark_meta_dirty();
    mark_runtime_dirty();
    return Status::OK();
}

Status SdmStore::get_table(TableID table_id, TableOr& out) const {
    TablePtr table;
    RETURN_IF_INVALID_STATUS(meta_store_->get_table(table_id, table))
    out = table ? TableOr{*table} : TableOr{};
    return Status::OK();
}

Status SdmStore::get_table_by_name(const std::string& db_name,
                                   const std::string& table_name,
                                   TableOr& out) const {
    TableID table_id;
    RETURN_IF_INVALID_STATUS(
        runtime_index_->find_table_by_name(db_name, table_name, table_id))
    TablePtr table;
    RETURN_IF_INVALID_STATUS(meta_store_->get_table(table_id, table))
    out = table ? TableOr{*table} : TableOr{};
    return Status::OK();
}

Status SdmStore::list_tables(std::vector<Table>& out) const {
    std::vector<TablePtr> tables;
    RETURN_IF_INVALID_STATUS(meta_store_->list_tables(tables))
    out.clear();
    out.reserve(tables.size());
    for (const TablePtr& table : tables) {
        if (table) {
            out.push_back(*table);
        }
    }
    std::sort(out.begin(), out.end(), [](const Table& lhs, const Table& rhs) {
        return lhs.table_id < rhs.table_id;
    });
    return Status::OK();
}

Status SdmStore::delete_table(TableID table_id) {
    TablePtr old_ptr;
    RETURN_IF_INVALID_STATUS(meta_store_->get_table(table_id, old_ptr))
    if (old_ptr == nullptr) {
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(meta_store_->delete_table(table_id))
    RETURN_IF_INVALID_STATUS(
        maybe_repair_runtime_index(runtime_index_->on_table_delete(*old_ptr)))
    mark_meta_dirty();
    mark_runtime_dirty();
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status SdmStore::put_node(const Node& node) {
    NodePtr old_ptr;
    RETURN_IF_INVALID_STATUS(meta_store_->get_node(node.id, old_ptr))
    RETURN_IF_INVALID_STATUS(meta_store_->upsert_node(node))
    RETURN_IF_INVALID_STATUS(maybe_repair_runtime_index(
        runtime_index_->on_node_upsert(old_ptr.get(), node)))
    mark_meta_dirty();
    mark_runtime_dirty();
    return Status::OK();
}

Status SdmStore::get_node(const NodeID& node_id, NodeOr& out) const {
    NodePtr node;
    RETURN_IF_INVALID_STATUS(meta_store_->get_node(node_id, node))
    out = node ? NodeOr{*node} : NodeOr{};
    return Status::OK();
}

Status SdmStore::list_nodes(std::vector<Node>& out) const {
    std::vector<NodePtr> nodes;
    RETURN_IF_INVALID_STATUS(meta_store_->list_nodes(nodes))
    out.clear();
    out.reserve(nodes.size());
    for (const NodePtr& node : nodes) {
        if (node) {
            out.push_back(*node);
        }
    }
    std::sort(out.begin(), out.end(),
              [](const Node& lhs, const Node& rhs) { return lhs.id < rhs.id; });
    return Status::OK();
}

Status SdmStore::list_nodes_by_resource_pool(const std::string& pool_name,
                                             std::vector<Node>& out) const {
    std::vector<NodeID> node_ids;
    RETURN_IF_INVALID_STATUS(
        runtime_index_->list_nodes_by_resource_pool(pool_name, node_ids))
    std::sort(node_ids.begin(), node_ids.end());

    out.clear();
    out.reserve(node_ids.size());
    for (const NodeID& node_id : node_ids) {
        NodePtr node;
        RETURN_IF_INVALID_STATUS(meta_store_->get_node(node_id, node))
        if (node != nullptr) {
            out.push_back(*node);
        }
    }
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status SdmStore::get_resource_pool(const std::string& name,
                                   ResourcePoolOr& out) const {
    ResourcePoolPtr pool;
    RETURN_IF_INVALID_STATUS(meta_store_->get_resource_pool(name, pool))
    out = pool ? ResourcePoolOr{*pool} : ResourcePoolOr{};
    return Status::OK();
}

Status SdmStore::list_resource_pools(std::vector<ResourcePool>& pools) const {
    std::vector<ResourcePoolPtr> pool_ptrs;
    RETURN_IF_INVALID_STATUS(meta_store_->list_resource_pools(pool_ptrs))
    pools.clear();
    pools.reserve(pool_ptrs.size());
    for (const ResourcePoolPtr& pool : pool_ptrs) {
        if (pool) {
            pools.push_back(*pool);
        }
    }
    std::sort(pools.begin(), pools.end(),
              [](const ResourcePool& lhs, const ResourcePool& rhs) {
                  return lhs.name < rhs.name;
              });
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status SdmStore::put_replica(const Replica& replica) {
    ReplicaPtr old_ptr;
    RETURN_IF_INVALID_STATUS(
        meta_store_->get_replica(replica.replica_id, old_ptr))
    RETURN_IF_INVALID_STATUS(meta_store_->upsert_replica(replica))
    RETURN_IF_INVALID_STATUS(maybe_repair_runtime_index(
        runtime_index_->on_replica_upsert(old_ptr.get(), replica)))
    mark_meta_dirty();
    mark_runtime_dirty();
    return Status::OK();
}

Status SdmStore::put_replicas(const std::vector<Replica>& replicas) {
    std::vector<ReplicaPtr> old_ptrs;
    old_ptrs.reserve(replicas.size());
    for (const Replica& replica : replicas) {
        ReplicaPtr old_ptr;
        RETURN_IF_INVALID_STATUS(
            meta_store_->get_replica(replica.replica_id, old_ptr))
        old_ptrs.emplace_back(std::move(old_ptr));
    }
    RETURN_IF_INVALID_CONDITION(old_ptrs.size() == replicas.size(),
                                "old_ptr size should == replicas size")

    RETURN_IF_INVALID_STATUS(meta_store_->upsert_replicas(replicas))
    for (int i = 0, siz = replicas.size(); i < siz; i++) {
        Status status =
            runtime_index_->on_replica_upsert(old_ptrs[i].get(), replicas[i]);
        if (status.fail()) {
            RETURN_IF_INVALID_STATUS(maybe_repair_runtime_index(status))
            mark_meta_dirty();
            mark_runtime_dirty();
            return Status::OK();
        }
    }
    if (!replicas.empty()) {
        mark_meta_dirty();
        mark_runtime_dirty();
    }
    return Status::OK();
}

Status SdmStore::get_replica(const ReplicaID& replica_key,
                             ReplicaOr& out) const {
    ReplicaPtr replica;
    RETURN_IF_INVALID_STATUS(meta_store_->get_replica(replica_key, replica))
    out = replica ? ReplicaOr{*replica} : ReplicaOr{};
    return Status::OK();
}

Status SdmStore::del_replica(const ReplicaID& replica_key) {
    ReplicaPtr old_ptr;
    RETURN_IF_INVALID_STATUS(meta_store_->get_replica(replica_key, old_ptr))
    if (old_ptr == nullptr) {
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(meta_store_->delete_replica(replica_key))
    RETURN_IF_INVALID_STATUS(
        maybe_repair_runtime_index(runtime_index_->on_replica_delete(*old_ptr)))
    mark_meta_dirty();
    mark_runtime_dirty();
    return Status::OK();
}

Status SdmStore::list_replicas(std::vector<Replica>& out) const {
    std::vector<ReplicaPtr> replicas;
    RETURN_IF_INVALID_STATUS(meta_store_->list_replicas(replicas))

    out.clear();
    out.reserve(replicas.size());
    for (const ReplicaPtr& replica : replicas) {
        if (replica != nullptr) {
            out.push_back(*replica);
        }
    }
    return Status::OK();
}

// 如果shard上没有replicas，返回一个空集合是被允许的
// 被删除的Replica是不会被展list的。
Status SdmStore::list_replicas_by_shard(const ShardID& shard_id,
                                        std::vector<Replica>& out) const {
    std::vector<ReplicaID> replica_ids;
    RETURN_IF_INVALID_STATUS(
        runtime_index_->list_replicas_by_shard(shard_id, replica_ids));

    auto less_replica_id = [](const ReplicaID& lhs, const ReplicaID& rhs) {
        if (lhs.table_id != rhs.table_id) {
            return lhs.table_id < rhs.table_id;
        }
        if (lhs.shard_index != rhs.shard_index) {
            return lhs.shard_index < rhs.shard_index;
        }
        return lhs.replica_seq < rhs.replica_seq;
    };

    std::sort(replica_ids.begin(), replica_ids.end(), less_replica_id);

    out.clear();
    out.reserve(replica_ids.size());
    for (const ReplicaID& replica_id : replica_ids) {
        ReplicaPtr replica;
        RETURN_IF_INVALID_STATUS(meta_store_->get_replica(replica_id, replica))
        if (replica != nullptr) {
            out.push_back(*replica);
        }
    }
    return Status::OK();
}

Status SdmStore::list_replicas_by_node(NodeID node_id,
                                       std::vector<Replica>& out) const {
    std::vector<ReplicaID> replica_ids;
    RETURN_IF_INVALID_STATUS(
        runtime_index_->list_replicas_by_node(node_id, replica_ids));

    auto less_replica_id = [](const ReplicaID& lhs, const ReplicaID& rhs) {
        if (lhs.table_id != rhs.table_id) {
            return lhs.table_id < rhs.table_id;
        }
        if (lhs.shard_index != rhs.shard_index) {
            return lhs.shard_index < rhs.shard_index;
        }
        return lhs.replica_seq < rhs.replica_seq;
    };
    std::sort(replica_ids.begin(), replica_ids.end(), less_replica_id);

    out.clear();
    out.reserve(replica_ids.size());
    for (const ReplicaID& replica_id : replica_ids) {
        ReplicaPtr replica;
        RETURN_IF_INVALID_STATUS(meta_store_->get_replica(replica_id, replica))
        if (replica != nullptr) {
            out.push_back(*replica);
        }
    }
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status SdmStore::put_shard_route(const ShardRoute& route) {
    RETURN_IF_INVALID_STATUS(runtime_index_->put_shard_route(route))
    mark_runtime_dirty();
    return Status::OK();
}

Status SdmStore::get_shard_route(const ShardID& shard_id,
                                 ShardRouteOr& out) const {
    ShardRoutePtr route;
    RETURN_IF_INVALID_STATUS(runtime_index_->get_shard_route(shard_id, route))
    out = route ? ShardRouteOr{*route} : ShardRouteOr{};
    return Status::OK();
}

Status SdmStore::delete_shard_route(const ShardID& shard_id) {
    ShardRoutePtr route;
    RETURN_IF_INVALID_STATUS(runtime_index_->get_shard_route(shard_id, route))
    if (route == nullptr) {
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(runtime_index_->delete_shard_route(shard_id))
    mark_runtime_dirty();
    return Status::OK();
}

Status SdmStore::del_shard_route_entry(const ShardID& shard_id,
                                       const ReplicaKey& replica_key) {
    ShardRoutePtr route;
    RETURN_IF_INVALID_STATUS(runtime_index_->get_shard_route(shard_id, route))
    if (route == nullptr) {
        return Status::OK();
    }
    RETURN_IF_INVALID_STATUS(
        runtime_index_->del_shard_route_entry(shard_id, replica_key))
    mark_runtime_dirty();
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status SdmStore::put_replica_group(const ReplicaGroup& group) {
    RETURN_IF_INVALID_STATUS(meta_store_->upsert_replica_group(group))
    mark_meta_dirty();
    return Status::OK();
}

Status SdmStore::get_replica_group(const ShardID& shard_id,
                                   ReplicaGroupOr& out) const {
    ReplicaGroupPtr group;
    RETURN_IF_INVALID_STATUS(meta_store_->get_replica_group(shard_id, group))
    out = group ? ReplicaGroupOr{*group} : ReplicaGroupOr{};
    return Status::OK();
}

Status SdmStore::delete_replica_group(const ShardID& shard_id) {
    RETURN_IF_INVALID_STATUS(meta_store_->delete_replica_group(shard_id))
    mark_meta_dirty();
    return Status::OK();
}

Status SdmStore::list_replica_groups(std::vector<ReplicaGroup>& out) const {
    std::vector<ReplicaGroupPtr> groups;
    RETURN_IF_INVALID_STATUS(meta_store_->list_replica_groups(groups))
    out.clear();
    out.reserve(groups.size());
    for (const ReplicaGroupPtr& group : groups) {
        if (group) {
            out.push_back(*group);
        }
    }

    auto less_shard_id = [](const ShardID& lhs, const ShardID& rhs) {
        if (lhs.table_id != rhs.table_id) {
            return lhs.table_id < rhs.table_id;
        }
        return lhs.shard_index < rhs.shard_index;
    };

    std::sort(
        out.begin(), out.end(),
        [&less_shard_id](const ReplicaGroup& lhs, const ReplicaGroup& rhs) {
            return less_shard_id(lhs.shard_id, rhs.shard_id);
        });
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////

Status SdmStore::maybe_repair_runtime_index(Status index_status) {
    if (index_status.ok()) {
        return Status::OK();
    }
    LOG_ERROR("runtime index update failed, rebuild index, msg={}",
              index_status.msg());
    return rebuild_runtime_index();
}

Status SdmStore::rebuild_runtime_index() {
    RETURN_IF_NULLPTR(runtime_index_, "sdm runtime index is nullptr")
    runtime_index_->clear();

    std::vector<TablePtr> tables;
    Status status = meta_store_->list_tables(tables);
    RETURN_IF_INVALID_STATUS(status)
    for (const TablePtr& table : tables) {
        if (!table) continue;
        status = runtime_index_->on_table_upsert(nullptr, *table);
        RETURN_IF_INVALID_STATUS(status)
    }

    std::vector<NodePtr> nodes;
    status = meta_store_->list_nodes(nodes);
    RETURN_IF_INVALID_STATUS(status)
    for (const NodePtr& node : nodes) {
        if (!node) continue;
        status = runtime_index_->on_node_upsert(nullptr, *node);
        RETURN_IF_INVALID_STATUS(status)
    }

    std::vector<ReplicaPtr> replicas;
    status = meta_store_->list_replicas(replicas);
    RETURN_IF_INVALID_STATUS(status)
    for (const ReplicaPtr& replica : replicas) {
        if (!replica) continue;
        status = runtime_index_->on_replica_upsert(nullptr, *replica);
        RETURN_IF_INVALID_STATUS(status)
    }

    return Status::OK();
}

}  // namespace adviskv::sdm
