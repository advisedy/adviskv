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

ReplicaPtr ReplicaManager::get_replica_by_id(
    const ReplicaID& replica_id) const {
    std::shared_lock locker(mutex_);
    auto it = replica_map_.find(replica_id);
    if (it == replica_map_.end()) {
        return nullptr;
    }
    return it->second;
}

ReplicaPtr ReplicaManager::get_replica_by_shard(const ShardID& shard_id) const {
    std::shared_lock locker(mutex_);

    auto shard_it = shard_primary_index_.find(shard_id);
    if (shard_it == shard_primary_index_.end()) {
        return nullptr;
    }

    auto replica_it = replica_map_.find(shard_it->second);
    if (replica_it == replica_map_.end()) {
        return nullptr;
    }
    return replica_it->second;
}

Status ReplicaManager::add_replica(const ReplicaInitParam& param) {
    const ReplicaID replica_id = param.replica_id;
    ShardID shard_id{replica_id.table_id, replica_id.shard_index};
    std::unique_lock locker(mutex_);

    if (replica_map_.count(replica_id)) {
        ReplicaMetaPayload old_payload;
        RETURN_IF_INVALID_STATUS(
            meta_persist_.load_replica_meta(replica_id, old_payload))
        if (old_payload.init_param == param) {
            return Status::OK();
        }
        return Status{
            StatusCode::ALREADY_EXIST,
            fmt::format("replica already exists, "
                        "table_id: {}, shard_index: {}, replica_index: {}",
                        replica_id.table_id, replica_id.shard_index,
                        replica_id.replica_index)};
    }

    if (shard_primary_index_.count(shard_id)) {
        return Status{StatusCode::ALREADY_EXIST,
                      fmt::format("shard already exists on current node, "
                                  "table_id: {}, shard_index: {}",
                                  shard_id.table_id, shard_id.shard_index)};
    }

    auto replica = std::make_shared<Replica>();
    RETURN_IF_INVALID_STATUS(replica->init(param))

    ReplicaMetaPayload payload;
    payload.init_param = param;
    RETURN_IF_INVALID_STATUS(meta_persist_.save_replica_meta(payload))

    shard_primary_index_.insert({shard_id, replica_id});
    replica_map_.insert({replica_id, std::move(replica)});
    return Status::OK();
}

Status ReplicaManager::delete_replica(const ReplicaID& replica_id) {
    std::unique_lock locker(mutex_);

    if (auto it = replica_map_.find(replica_id); it != replica_map_.end()) {
        it->second->shutdown();
        ShardID shard_id = it->second->get_shard_id();
        shard_primary_index_.erase(shard_id);
        replica_map_.erase(it);
    }

    // 这里是因为，如果我们之前replica_map上删除了，但是持久化没有删除成功，我们也是不会在replica_map上添加回来的、
    // 毕竟replica删除是因为table被删除了，我们总不能继续保留这个replica_map的索引。
    // 所以就可能出现，replica_map上没有，但是持久化数据上还存在相关残留数据的情况，那当我们再次调用的时候，
    // 就再调用一次persist上的delete。
    // 不然就再也没有可能去清掉残留数据的机会了。
    return meta_persist_.delete_replica_data(replica_id);
}

std::vector<ReplicaPtr> ReplicaManager::get_replicas() const {
    std::shared_lock locker(mutex_);
    std::vector<ReplicaPtr> replicas;
    replicas.reserve(replica_map_.size());
    for (const auto& [_, replica] : replica_map_) {
        replicas.emplace_back(replica);
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
            ShardID shard_id{param.replica_id.table_id,
                             param.replica_id.shard_index};
            if (replica_map_.count(param.replica_id) ||
                shard_primary_index_.count(shard_id)) {
                continue;
            }

            auto replica = std::make_shared<Replica>();
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