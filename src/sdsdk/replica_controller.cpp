#include "sdsdk/replica_controller.h"

#include <unordered_set>

#include "common/define.h"

namespace adviskv::sdsdk {

Status ReplicaController::init(StorageCallbackPtr callback, int32_t worker_count) {
    RETURN_IF_INVALID_CONDITION(callback != nullptr, "callback is nullptr")
    RETURN_IF_INVALID_CONDITION(worker_count > 0, "worker_count should > 0")
    RETURN_IF_INVALID_CONDITION(!initialized_, "replica controller already initialized")

    callback_ = std::move(callback);
    thread_pool_.start(static_cast<size_t>(worker_count));
    initialized_ = true;
    return Status::OK();
}

Status ReplicaController::apply_desired_set(
    const std::vector<DesiredReplicaSpec>& desired_set) {
    RETURN_IF_INVALID_CONDITION(initialized_, "replica controller not initialized")

    std::unordered_set<ReplicaKey, ReplicaKeyHash> desired_keys;
    desired_keys.reserve(desired_set.size());

    std::vector<DesiredReplicaSpec> create_specs;
    std::vector<std::pair<ReplicaKey, std::pair<pb::ReplicaRole, pb::ReplicaRole>>>
        role_changes;
    std::vector<ReplicaKey> delete_keys;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const DesiredReplicaSpec& spec : desired_set) {
            desired_keys.insert(spec.key);

            auto [it, inserted] = replicas_.emplace(spec.key, LocalReplica{});
            LocalReplica& local = it->second;
            if (inserted) {
                local.key = spec.key;
                local.role = spec.role;
                local.status = pb::ReplicaStatus::ADDING;
            }

            if (local.is_updating) {
                continue;
            }

            if (spec.is_dropped) {
                local.is_updating = true;
                local.status = pb::ReplicaStatus::LOST;
                delete_keys.push_back(spec.key);
                continue;
            }

            if (!local.exists_locally) {
                local.is_updating = true;
                local.status = pb::ReplicaStatus::ADDING;
                create_specs.push_back(spec);
                continue;
            }

            if (local.role != spec.role) {
                local.is_updating = true;
                role_changes.push_back(
                    {spec.key, {local.role, spec.role}});
            }
        }

        for (auto& [key, local] : replicas_) {
            if (local.is_updating) {
                continue;
            }
            if (!desired_keys.count(key)) {
                local.is_updating = true;
                local.status = pb::ReplicaStatus::LOST;
                delete_keys.push_back(key);
            }
        }
    }

    for (const DesiredReplicaSpec& spec : create_specs) {
        schedule_create(spec);
    }
    for (const auto& [key, roles] : role_changes) {
        schedule_change_role(key, roles.first, roles.second);
    }
    for (const ReplicaKey& key : delete_keys) {
        schedule_delete(key);
    }

    return Status::OK();
}

Status ReplicaController::collect_cached_replica_reports(
    std::vector<ReplicaReport>& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    out.clear();
    out.reserve(replicas_.size());
    for (const auto& [_, local] : replicas_) {
        ReplicaReport report;
        report.key = local.key;
        report.role = local.role;
        report.status = local.status;
        report.endpoint = local.endpoint;
        out.push_back(std::move(report));
    }
    return Status::OK();
}

bool ReplicaController::all_ready() const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [_, local] : replicas_) {
        if (!local.exists_locally || local.is_updating ||
            local.status != pb::ReplicaStatus::READY) {
            return false;
        }
    }
    return true;
}

bool ReplicaController::has_non_follower_replica() const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [_, local] : replicas_) {
        if (local.exists_locally &&
            local.role != pb::ReplicaRole::FOLLOWER) {
            return true;
        }
    }
    return false;
}

void ReplicaController::stop() {
    if (!initialized_) {
        return;
    }
    thread_pool_.stop();
}

void ReplicaController::schedule_create(const DesiredReplicaSpec& spec) {
    thread_pool_.submit([this, spec]() {
        CreateReplicaArgs args;
        args.key = spec.key;
        args.role = spec.role;
        args.engine_type = spec.engine_type;

        CreateReplicaResult result;
        Status status = callback_->create_replica(args, result);

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = replicas_.find(spec.key);
        if (it == replicas_.end()) {
            return;
        }
        LocalReplica& local = it->second;
        local.is_updating = false;
        if (status.ok()) {
            local.exists_locally = true;
            local.role = spec.role;
            local.status = pb::ReplicaStatus::READY;
            local.endpoint = result.endpoint;
        } else {
            local.exists_locally = false;
            local.status = pb::ReplicaStatus::ERROR;
        }
    });
}

void ReplicaController::schedule_delete(const ReplicaKey& key) {
    thread_pool_.submit([this, key]() {
        DeleteReplicaArgs args;
        args.key = key;
        Status status = callback_->delete_replica(args);

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = replicas_.find(key);
        if (it == replicas_.end()) {
            return;
        }
        if (status.ok()) {
            replicas_.erase(it);
        } else {
            it->second.is_updating = false;
            it->second.status = pb::ReplicaStatus::ERROR;
        }
    });
}

void ReplicaController::schedule_change_role(const ReplicaKey& key,
                                             pb::ReplicaRole old_role,
                                             pb::ReplicaRole new_role) {
    thread_pool_.submit([this, key, old_role, new_role]() {
        ChangeReplicaRoleArgs args;
        args.key = key;
        args.old_role = old_role;
        args.new_role = new_role;

        ChangeReplicaRoleResult result;
        Status status = callback_->change_replica_role(args, result);

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = replicas_.find(key);
        if (it == replicas_.end()) {
            return;
        }
        LocalReplica& local = it->second;
        local.is_updating = false;
        if (status.ok()) {
            local.role = new_role;
            if (!result.endpoint.ip.empty()) {
                local.endpoint = result.endpoint;
            }
        } else {
            local.status = pb::ReplicaStatus::ERROR;
        }
    });
}

}  // namespace adviskv::sdsdk
