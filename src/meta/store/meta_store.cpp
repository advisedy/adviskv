#include "meta/store/meta_store.h"

#include <atomic>
#include <utility>
#include <variant>

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/persist/meta_persist_engine.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

MetaStore::MetaStore(const MetaStoreParam& param) {
    if (auto* persist_param = std::get_if<PersistentMetaStoreParam>(&param)) {
        persist_engine_ = std::make_unique<MetaPersistEngine>(persist_param->data_dir);
    }
    publish(std::make_shared<const MetaSnapshot>(
            MetaSnapshot{std::make_shared<const DurableData>(), std::make_shared<const RuntimeData>(),
                         std::make_shared<const MetaIndex>()}));
}

Status MetaStore::init() {
    std::lock_guard<std::mutex> locker{writer_mutex_};

    auto durable = std::make_shared<DurableData>();
    auto runtime = std::make_shared<RuntimeData>();
    auto index = std::make_shared<MetaIndex>();

    if (persist_engine_) {
        Status status = persist_engine_->init();
        if (status.fail()) {
            LOG_ERROR("[MetaStore] persist engine init failed: {}", status.to_string());
            return status;
        }
        status = load_from_persist(*durable);
        if (status.fail()) {
            LOG_ERROR("[MetaStore] load meta.json failed: {}", status.to_string());
            return status;
        }
    }
    rebuild_index(*durable, *runtime, *index);
    publish(std::make_shared<const MetaSnapshot>(MetaSnapshot{durable, runtime, index}));
    return Status::OK();
}

// 读就是拿一次快照然后跑，全程无锁；写串行。
Status MetaStore::read_with(const std::function<Status(const MetaStoreTxn&)>& func) const {
    if (func == nullptr) {
        return Status::INVALID_ARGUMENT("read_with fn is nullptr");
    }
    MetaStoreTxn txn(load_snapshot());
    return func(txn);
}

Status MetaStore::write_with(const std::function<Status(MetaStoreTxn&)>& func) {
    if (func == nullptr) {
        return Status::INVALID_ARGUMENT("write_with fn is nullptr");
    }

    ADVISKV_METRICS_TIMER("meta_store_write_with");
    ADVISKV_METRICS_COUNTER("meta_store_write_with_request");

    std::lock_guard<std::mutex> locker{writer_mutex_};

    MetaStoreTxn txn(load_snapshot());
    Status status = func(txn);
    if (status.fail()) {
        return status;
    }

    std::shared_ptr<const MetaSnapshot> next = txn.finish();

    // durable 有脏数据就先落盘，落盘成功才发布；失败不发布，等于回滚。
    if (txn.durable_dirty()) {
        RETURN_IF_INVALID_STATUS(persist_if_needed(*next->durable))
    }
    publish(std::move(next));
    return Status::OK();
}

void MetaStore::rebuild_index(const DurableData& durable, const RuntimeData& runtime, MetaIndex& index) {
    index.clear();

    for (const auto& [_, db] : durable.databases) {
        UNUSED(_);
        index.on_db_upsert(nullptr, db);
    }
    for (const auto& [_, table] : durable.tables) {
        UNUSED(_);
        index.on_table_upsert(nullptr, table);
    }
    for (const auto& [_, node] : runtime.nodes) {
        UNUSED(_);
        index.on_node_upsert(nullptr, node);
    }
    for (const auto& [_, replica] : durable.replicas) {
        UNUSED(_);
        index.on_replica_upsert(nullptr, replica);
    }
}

std::shared_ptr<const MetaSnapshot> MetaStore::load_snapshot() const {
    return std::atomic_load(&published_);
}

void MetaStore::publish(std::shared_ptr<const MetaSnapshot> next) {
    std::atomic_store(&published_, std::move(next));
}

Status MetaStore::persist_if_needed(const DurableData& durable) {
    if (!persist_engine_) return Status::OK();
    MetaPersistedRecord record;
    record.next_db_id = durable.db_id_allocator.current_id();
    for (const auto& [_, db] : durable.databases) {
        UNUSED(_);
        record.next_db_id = std::max(record.next_db_id, db.db_id + 1);
    }
    record.next_table_id = durable.table_id_allocator.current_id();
    for (const auto& [_, table] : durable.tables) {
        UNUSED(_);
        record.next_table_id = std::max(record.next_table_id, table.table_id + 1);
    }
    record.databases.reserve(durable.databases.size());
    for (const auto& [_, db] : durable.databases) record.databases.push_back(db);
    record.tables.reserve(durable.tables.size());
    for (const auto& [_, table] : durable.tables) record.tables.push_back(table);
    record.replicas.reserve(durable.replicas.size());
    for (const auto& [_, replica] : durable.replicas) record.replicas.push_back(replica);
    record.replica_groups.reserve(durable.replica_groups.size());
    for (const auto& [_, group] : durable.replica_groups) record.replica_groups.push_back(group);
    return persist_engine_->save_meta(record);
}

Status MetaStore::load_from_persist(DurableData& durable) {
    if (!persist_engine_) return Status::OK();
    MetaPersistedRecord record;
    RETURN_IF_INVALID_STATUS(persist_engine_->load_meta(record))
    durable.db_id_allocator = IDAllocator<DatabaseID>(record.next_db_id);
    durable.table_id_allocator = IDAllocator<TableID>(record.next_table_id);
    for (auto& db : record.databases) durable.databases[db.db_id] = std::move(db);
    for (auto& table : record.tables) durable.tables[table.table_id] = std::move(table);
    for (auto& replica : record.replicas) {
        durable.replicas[replica.replica_id] = std::move(replica);
    }
    for (auto& group : record.replica_groups) durable.replica_groups[group.shard_id] = std::move(group);
    return Status::OK();
}

}  // namespace adviskv::meta
