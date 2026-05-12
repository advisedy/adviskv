#include "storage/replica/replica_manager.h"

#include <fmt/format.h>

#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

Replica* ReplicaManager::get_replica_by_id(const ReplicaID& replica_id) const {
    std::shared_lock locker(mutex_);
    auto it = replica_map_.find(replica_id);
    if (it == replica_map_.end()) {
        return nullptr;
    }
    return it->second.get();
}

Replica* ReplicaManager::get_replica_by_shard(const ShardID& shard_id) const {
    std::shared_lock locker(mutex_);

    auto shard_it = shard_primary_index_.find(shard_id);
    if (shard_it == shard_primary_index_.end()) {
        return nullptr;
    }

    auto replica_it = replica_map_.find(shard_it->second);
    if (replica_it == replica_map_.end()) {
        return nullptr;
    }
    return replica_it->second.get();
}

Status ReplicaManager::add_replica(const ReplicaInitParam& param) {
    const ReplicaID replica_id = param.replica_id;
    const ShardID shard_id{.table_id = replica_id.table_id,
                           .shard_index = replica_id.shard_index};
    std::unique_lock locker(mutex_);

    if (replica_map_.count(replica_id)) {
        return Status{StatusCode::INVALID_ARGUMENT,
                      fmt::format("replica already exists, table_id: {}, "
                                  "shard_index: {}, replica_index: {}",
                                  replica_id.table_id, replica_id.shard_index,
                                  replica_id.replica_index)};
    }
    if (shard_primary_index_.count(shard_id)) {
        return Status{StatusCode::INVALID_ARGUMENT,
                      fmt::format("shard already exists on current node, "
                                  "table_id: {}, shard_index: {}",
                                  shard_id.table_id, shard_id.shard_index)};
    }

    auto replica = std::make_unique<Replica>();
    RETURN_IF_INVALID_STATUS(replica->init(param))

    ReplicaMetaPayload payload{.init_param = param};
    RETURN_IF_INVALID_STATUS(meta_persist_.save_replica_meta(payload))

    shard_primary_index_.insert({shard_id, replica_id});
    replica_map_.insert({replica_id, std::move(replica)});
    return Status::OK();
}

Status ReplicaManager::delete_replica(const ReplicaID& replica_id) {
    std::unique_lock locker(mutex_);
    auto it = replica_map_.find(replica_id);
    if (it == replica_map_.end()) {
        return Status::OK();
    }
    ShardID shard_id = it->second->get_shard_id();
    shard_primary_index_.erase(shard_id);
    replica_map_.erase(it);
    return Status::OK();
}

std::vector<Replica*> ReplicaManager::get_replicas() const {
    std::shared_lock locker(mutex_);
    std::vector<Replica*> replicas;
    replicas.reserve(replica_map_.size());
    for (const auto& [_, replica] : replica_map_) {
        replicas.push_back(replica.get());
    }
    return replicas;
}

void ReplicaManager::start_tick() {
    raft_tick_task_ = std::make_unique<RaftTickTask>(this);
    raft_tick_task_->start(Milliseconds(20));
}

void ReplicaManager::recover() {
    std::unique_lock locker(mutex_);
    {
        try {
            std::filesystem::create_directories(data_dir_);
        } catch (const std::filesystem::filesystem_error& e) {
            LOG_WARN("create replica manager data dir failed: {}", e.what());
        }

        const std::vector<std::filesystem::path> meta_paths =
            meta_persist_.scan_replica_meta_files();
        for (const auto& meta_path : meta_paths) {
            ReplicaMetaPayload payload;
            Status status = meta_persist_.load_replica_meta(meta_path, payload);
            if (status.fail()) {
                LOG_WARN("load replica meta failed, path={}, msg={}",
                         meta_path.string(), status.msg());
                continue;
            }
            ReplicaInitParam& param = payload.init_param;
            ShardID shard_id{.table_id = param.replica_id.table_id,
                             .shard_index = param.replica_id.shard_index};
            if (replica_map_.count(param.replica_id) ||
                shard_primary_index_.count(shard_id)) {
                continue;
            }

            auto replica = std::make_unique<Replica>();
            status = replica->init(param);
            if (status.fail()) {
                LOG_WARN("init recovered replica failed, path={}, msg={}",
                         meta_path.string(), status.msg());
                continue;
            }
            replica_map_[param.replica_id] = std::move(replica);
            shard_primary_index_[shard_id] = param.replica_id;
        }
    }

    for (const auto& [_, replica] : replica_map_) {
        Status status = replica->recover();
        if (status.fail()) {
            LOG_WARN("replica recover failed, table_id={}, shard_index={}",
                     replica->shard_id_.table_id,
                     replica->shard_id_.shard_index);
        }
    }
    LOG_INFO("all replicas recovered");
}

const std::string& ReplicaManager::get_data_dir() const { return data_dir_; }

}  // namespace adviskv::storage
