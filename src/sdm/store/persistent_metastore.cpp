#include "sdm/store/persistent_metastore.h"

#include <memory>
#include <utility>

#include "common/metrics/metrics.h"
#include "common/status.h"
#include "sdm/store/memory_metastore.h"

namespace adviskv::sdm {

PersistentMetaStore::PersistentMetaStore(std::filesystem::path data_dir)
        : memory_store_(std::make_unique<MemoryMetaStore>()),
          persist_engine_(std::make_unique<SdmPersistEngine>(data_dir.string())) {}

PersistentMetaStore::PersistentMetaStore(std::unique_ptr<ISdmMetaStore> memory_store, std::filesystem::path data_dir)
        : memory_store_(std::move(memory_store)),
          persist_engine_(std::make_unique<SdmPersistEngine>(data_dir.string())) {}

PersistentMetaStore::PersistentMetaStore(std::unique_ptr<ISdmMetaStore> memory_store,
                                         std::unique_ptr<ISdmPersistEngine> persist_engine)
        : memory_store_(std::move(memory_store)), persist_engine_(std::move(persist_engine)) {}

Status PersistentMetaStore::init() {
    RETURN_IF_NULLPTR(memory_store_, "sdm memory store is nullptr")
    RETURN_IF_NULLPTR(persist_engine_, "sdm persist engine is nullptr")
    RETURN_IF_INVALID_STATUS(memory_store_->init())
    RETURN_IF_INVALID_STATUS(persist_engine_->init())
    RETURN_IF_INVALID_STATUS(load())
    return Status::OK();
}

std::unique_ptr<ISdmMetaStore> PersistentMetaStore::clone_memory_snapshot() const {
    return memory_store_->clone_memory_snapshot();
}

Status PersistentMetaStore::commit_memory_snapshot(std::unique_ptr<ISdmMetaStore> next_memory_store) {
    ADVISKV_METRICS_TIMER("sdm_metastore_commit_snapshot");
    ADVISKV_METRICS_COUNTER("sdm_metastore_commit_snapshot_request");

    RETURN_IF_NULLPTR(next_memory_store, "next_memory_store is nullptr");

    SdmPersistedRecord record;
    RETURN_IF_INVALID_STATUS(build_record_from_store(*next_memory_store, record))
    RETURN_IF_INVALID_STATUS(persist_record(record))

    memory_store_ = std::move(next_memory_store);
    return Status::OK();
}

Status PersistentMetaStore::load() {
    SdmPersistedRecord record;
    Status status = persist_engine_->load_sdm_meta(record);
    RETURN_IF_INVALID_STATUS(status)

    for (const auto& [_, table] : record.tables) {
        UNUSED(_);
        memory_store_->upsert_table(table);
    }
    for (const auto& [_, replica] : record.replicas) {
        UNUSED(_);
        memory_store_->upsert_replica(replica);
    }
    for (const auto& [_, pool] : record.resource_pools) {
        UNUSED(_);
        memory_store_->upsert_resource_pool(pool);
    }
    for (const auto& [_, group] : record.replica_groups) {
        UNUSED(_);
        memory_store_->upsert_replica_group(group);
    }

    return Status::OK();
}

Status PersistentMetaStore::build_record_from_store(const ISdmMetaStore& store, SdmPersistedRecord& record) const {
    record = {};

    std::vector<TablePtr> tables;
    RETURN_IF_INVALID_STATUS(store.list_tables(tables))
    for (const auto& t : tables) {
        record.tables[t->table_id] = *t;
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

    std::vector<ReplicaGroupPtr> groups;
    RETURN_IF_INVALID_STATUS(store.list_replica_groups(groups))
    for (const auto& group : groups) {
        record.replica_groups[group->shard_id] = *group;
    }

    return Status::OK();
}

Status PersistentMetaStore::persist_record(const SdmPersistedRecord& record) {
    return persist_engine_->save_sdm_meta(record);
}

Status PersistentMetaStore::commit_with(const std::function<Status(ISdmMetaStore&)>& mutate) {
    std::unique_ptr<ISdmMetaStore> next_memory_store = memory_store_->clone_memory_snapshot();
    RETURN_IF_INVALID_STATUS(mutate(*next_memory_store))

    SdmPersistedRecord next;
    RETURN_IF_INVALID_STATUS(build_record_from_store(*next_memory_store, next))
    RETURN_IF_INVALID_STATUS(persist_record(next))

    memory_store_ = std::move(next_memory_store);
    return Status::OK();
}

Status PersistentMetaStore::upsert_table(const Table& table) {
    return commit_with([&table](ISdmMetaStore& store) { return store.upsert_table(table); });
}

Status PersistentMetaStore::get_table(TableID table_id, TablePtr& out) const {
    return memory_store_->get_table(table_id, out);
}

Status PersistentMetaStore::delete_table(TableID table_id) {
    return commit_with([table_id](ISdmMetaStore& store) { return store.delete_table(table_id); });
}

Status PersistentMetaStore::list_tables(std::vector<TablePtr>& out) const { return memory_store_->list_tables(out); }

Status PersistentMetaStore::upsert_replica(const Replica& replica) {
    return commit_with([&replica](ISdmMetaStore& store) { return store.upsert_replica(replica); });
}

Status PersistentMetaStore::get_replica(const ReplicaID& key, ReplicaPtr& out) const {
    return memory_store_->get_replica(key, out);
}

Status PersistentMetaStore::delete_replica(const ReplicaID& key) {
    return commit_with([&key](ISdmMetaStore& store) { return store.delete_replica(key); });
}

Status PersistentMetaStore::upsert_replicas(const std::vector<Replica>& replicas) {
    return commit_with([&replicas](ISdmMetaStore& store) { return store.upsert_replicas(replicas); });
}

Status PersistentMetaStore::list_replicas(std::vector<ReplicaPtr>& out) const {
    return memory_store_->list_replicas(out);
}

Status PersistentMetaStore::upsert_resource_pool(const ResourcePool& pool) {
    return commit_with([&pool](ISdmMetaStore& store) { return store.upsert_resource_pool(pool); });
}

Status PersistentMetaStore::get_resource_pool(const std::string& name, ResourcePoolPtr& out) const {
    return memory_store_->get_resource_pool(name, out);
}

Status PersistentMetaStore::list_resource_pools(std::vector<ResourcePoolPtr>& out) const {
    return memory_store_->list_resource_pools(out);
}

Status PersistentMetaStore::delete_resource_pool(const std::string& name) {
    return commit_with([&name](ISdmMetaStore& store) { return store.delete_resource_pool(name); });
}

Status PersistentMetaStore::upsert_replica_group(const ReplicaGroup& group) {
    return commit_with([&group](ISdmMetaStore& store) { return store.upsert_replica_group(group); });
}

Status PersistentMetaStore::delete_replica_group(const ShardID& shard_id) {
    return commit_with([&shard_id](ISdmMetaStore& store) { return store.delete_replica_group(shard_id); });
}

Status PersistentMetaStore::get_replica_group(const ShardID& shard_id, ReplicaGroupPtr& out) const {
    return memory_store_->get_replica_group(shard_id, out);
}

Status PersistentMetaStore::list_replica_groups(std::vector<ReplicaGroupPtr>& out) const {
    return memory_store_->list_replica_groups(out);
}

}  // namespace adviskv::sdm