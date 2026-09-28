#include "meta/store/meta_store.h"

#include <mutex>
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
}

Status MetaStore::init() {
    std::unique_lock locker{mutex_};

    if (persist_engine_) {
        Status status = persist_engine_->init();
        if (status.fail()) {
            LOG_ERROR("[MetaStore] persist engine init failed: {}", status.to_string());
            return status;
        }
        status = load_from_persist(committed_);
        if (status.fail()) {
            LOG_ERROR("[MetaStore] load meta.json failed: {}", status.to_string());
            return status;
        }
    }
    rebuild_index(committed_);
    return Status::OK();
}

Status MetaStore::read_with(const std::function<Status(const MetaStoreTxn&)>& func) const {
    if (func == nullptr) {
        return Status::INVALID_ARGUMENT("read_with fn is nullptr");
    }
    std::shared_lock locker{mutex_};
    MetaStoreTxn txn(&committed_);
    return func(txn);
}

Status MetaStore::write_with(const std::function<Status(MetaStoreTxn&)>& func) {
    if (func == nullptr) {
        return Status::INVALID_ARGUMENT("write_with fn is nullptr");
    }

    ADVISKV_METRICS_TIMER("meta_store_write_with");
    ADVISKV_METRICS_COUNTER("meta_store_write_with_request");

    std::unique_lock locker{mutex_};

    // 先拷贝工作集，在副本上执行事务，失败就丢弃副本。
    MetaWorkingSet working = committed_;
    MetaStoreTxn txn(&working);
    Status status = func(txn);
    if (status.fail()) {
        return status;
    }

    // 落盘成功才发布内存状态；落盘失败不发布。
    if (txn.durable_dirty()) {
        RETURN_IF_INVALID_STATUS(persist_if_needed(working.durable))
        committed_.durable = std::move(working.durable);
    }
    committed_.runtime = std::move(working.runtime);
    committed_.index = std::move(working.index);
    return Status::OK();
}

void MetaStore::rebuild_index(MetaWorkingSet& ws) const {
    ws.index.clear();

    for (const auto& [_, db] : ws.durable.databases) {
        UNUSED(_);
        ws.index.on_db_upsert(nullptr, db);
    }
    for (const auto& [_, table] : ws.durable.tables) {
        UNUSED(_);
        ws.index.on_table_upsert(nullptr, table);
    }
    for (const auto& [_, node] : ws.runtime.nodes) {
        UNUSED(_);
        ws.index.on_node_upsert(nullptr, node);
    }
    for (const auto& [_, replica] : ws.durable.replicas) {
        UNUSED(_);
        ws.index.on_replica_upsert(nullptr, replica);
    }
}

Status MetaStore::persist_if_needed(const DurableData& durable) {
    if (!persist_engine_) return Status::OK();
    MetaPersistedRecord record;
    record.next_db_id = durable.db_id_allocator.current_id();
    record.next_table_id = durable.table_id_allocator.current_id();
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

Status MetaStore::load_from_persist(MetaWorkingSet& ws) {
    if (!persist_engine_) return Status::OK();
    MetaPersistedRecord record;
    RETURN_IF_INVALID_STATUS(persist_engine_->load_meta(record))
    ws.durable.db_id_allocator = IDAllocator<DatabaseID>(record.next_db_id);
    ws.durable.table_id_allocator = IDAllocator<TableID>(record.next_table_id);
    for (auto& db : record.databases) ws.durable.databases[db.db_id] = std::move(db);
    for (auto& table : record.tables) ws.durable.tables[table.table_id] = std::move(table);
    for (auto& replica : record.replicas) {
        ws.durable.replicas[replica.replica_id] = std::move(replica);
    }
    for (auto& group : record.replica_groups) ws.durable.replica_groups[group.shard_id] = std::move(group);
    return Status::OK();
}

}  // namespace adviskv::meta
