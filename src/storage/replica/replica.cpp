#include "storage/replica/replica.h"

#include <algorithm>
#include <filesystem>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <utility>

#include "common/constants.h"
#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/model/model.h"
#include "common/oper_gate.h"
#include "common/status.h"
#include "common/types.h"
#include "storage/model/param.h"
#include "common/path.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/replica_loop.h"
#include "storage/replica/replica_message_dispatcher.h"

namespace adviskv::storage {

Replica::Replica() = default;

Replica::~Replica() { shutdown(); }

namespace {

bool disk_has_replica_state(const ReplicaInitParam& param) {
    const std::filesystem::path dir = adviskv::replica_dir(param.runtime.data_dir, param.replica_id);
    std::error_code ec;
    return std::filesystem::exists(adviskv::wal_file(dir), ec) ||
           std::filesystem::exists(adviskv::raft_meta_file(dir), ec) ||
           std::filesystem::exists(adviskv::snapshot_file(dir), ec);
}

}

Status Replica::init(const ReplicaInitParam& param) {
    shard_id_ = ShardID{param.replica_id.table_id, param.replica_id.shard_index};
    replica_id_ = param.replica_id;

    local_state_.store(disk_has_replica_state(param) ? LocalState::AWAITING_RECOVER : LocalState::CREATED);

    persist_ = std::make_unique<PersistEngine>(param.runtime.data_dir, param.replica_id);
    RETURN_IF_INVALID_STATUS(persist_->init())

    state_machine_ = std::make_unique<KvStateMachine>(param.engine_type);

    raft_core_ = std::make_unique<RaftCore>(param.replica_id, param.members);
    message_dispatcher_ = std::make_unique<ReplicaMessageDispatcher>(param.runtime.raft_rpc_timeout_ms, *persist_,
                                                                     [this](Event event) {
                                                                         if (loop_) {
                                                                             loop_->submit_async(std::move(event));
                                                                         }
                                                                     });
    runtime_info_.set_builder([this]() {
        auto info = std::make_shared<ReplicaRuntimeInfo>();
        info->role = raft_core_->role();
        info->current_term = raft_core_->current_term();
        info->members = raft_core_->raft_members();
        info->snapshot_index = raft_core_->snapshot_index();
        info->snapshot_term = raft_core_->snapshot_term();
        info->commit_index = raft_core_->commit_index();
        info->last_applied = raft_core_->last_applied();
        info->is_recovering = raft_core_->is_recovering();
        return info;
    });
    runtime_info_.refresh();
    loop_ = std::make_unique<ReplicaLoop>(
            replica_id_, *raft_core_, *persist_, *state_machine_, *message_dispatcher_,
            FaultReporter([this](Status status) { return fault_if_fail(std::move(status)); },
                          [this](Status status) { report_fault(std::move(status)); }),
            param.runtime.raft_do_snapshot_limit, [this]() { runtime_info_.refresh(); });
    loop_->start();

    return Status::OK();
}

Status Replica::start() {
    if (local_state_.load() == LocalState::FAULTED) {
        return Status::IS_FAULTED("faulted or shut down replica requires destroy and rebuild");
    }
    if (local_state_.load() == LocalState::RUNNING) {
        return Status::OK();
    }
    if (local_state_.load() == LocalState::AWAITING_RECOVER) {
        LOG_WARN(
                "[Replica] start rejected: persisted state exists, recover is required first. replica_id:{}",
                replica_id_.to_string());
        return Status::NOT_INIT("replica has persisted state, recover must run before start");
    }
    message_dispatcher_->start();
    local_state_.store(LocalState::RUNNING);
    return Status::OK();
}

Status Replica::put(const PutParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_serving());

    ProposeCall call{ProposeParam::write(WriteOpType::PUT, param.key, param.value, param.request_id),
                     Milliseconds{K_REPLICA_PROPOSAL_COMMIT_TIMEOUT_MS}};
    loop_->submit_and_wait(&call);
    RETURN_IF_INVALID_STATUS(call.status)

    return Status::OK();
}

Status Replica::handle_install_snapshot(const InstallSnapshotParam& param) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_running())

    InstallSnapshotCall call{param};
    loop_->submit_and_wait(&call);
    return call.status;
}

Status Replica::get(const GetParam& param, Value& value) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_serving())

    SubmitReadCall read_call;
    read_call.key = param.key;
    loop_->submit_and_wait(&read_call);
    if (read_call.status.fail()) {
        return read_call.status;
    }
    if (read_call.state->status.ok()) {
        value = std::move(read_call.state->value);
    }
    return read_call.state->status;
}

Status Replica::del(const DelParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_serving());

    ProposeCall call{ProposeParam::write(WriteOpType::DEL, param.key, "", param.request_id),
                     Milliseconds{K_REPLICA_PROPOSAL_COMMIT_TIMEOUT_MS}};
    loop_->submit_and_wait(&call);
    RETURN_IF_INVALID_STATUS(call.status)

    return Status::OK();
}

Status Replica::cas(const CasParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_serving());

    ProposeCall call{ProposeParam::write(WriteOpType::CAS, param.key, param.value, param.request_id, param.expected),
                     Milliseconds{K_REPLICA_PROPOSAL_COMMIT_TIMEOUT_MS}};
    loop_->submit_and_wait(&call);
    RETURN_IF_INVALID_STATUS(call.status)
    return Status::OK();
}

Status Replica::handle_request_vote(const RequestVoteParam& param, RequestVoteResult& result) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_running())

    RequestVoteCall call{param};
    loop_->submit_and_wait(&call);
    RETURN_IF_INVALID_STATUS(call.status)
    result = call.result;
    return Status::OK();
}

Status Replica::handle_pre_vote(const PreVoteParam& param, PreVoteResult& result) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_running())

    PreVoteCall call{param};
    loop_->submit_and_wait(&call);
    RETURN_IF_INVALID_STATUS(call.status)
    result = call.result;
    return Status::OK();
}

Status Replica::handle_append_entries(const AppendEntriesParam& param, AppendEntriesResult& result) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_running())

    ADVISKV_METRICS_TIMER("storage_replica_handle_append_entries_raft_step");
    AppendEntriesCall call{param};
    loop_->submit_and_wait(&call);
    RETURN_IF_INVALID_STATUS(call.status)
    result = call.result;

    return Status::OK();
}

Status Replica::add_member(const PeerMember& member) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_serving());
    LOG_INFO("[Replica] add_member, replica_id:{}, add target:{}", replica_id_.to_string(), member.to_string());
    AddMemberCall call{member};
    loop_->submit_and_wait(&call);
    RETURN_IF_INVALID_STATUS(call.status)
    return Status::OK();
}

Status Replica::remove_member(const ReplicaID& replica_id) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_serving());
    LOG_INFO("[Replica] remove_member, replica_id:{}, remove target:{}", replica_id_.to_string(),
             replica_id.to_string());
    RemoveMemberCall call{replica_id};
    loop_->submit_and_wait(&call);
    RETURN_IF_INVALID_STATUS(call.status)
    return Status::OK();
}

void Replica::on_tick() {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED_VOID(oper_gate_)
    if (ensure_running().fail()) return;

    loop_->submit_async(TickEvent{});
}

Status Replica::recover() {
    const LocalState state = local_state_.load();
    if (state == LocalState::FAULTED) {
        return Status::IS_FAULTED("faulted or shut down replica requires destroy and rebuild");
    }
    if (state == LocalState::RUNNING) {
        LOG_WARN(
                "[Replica] recover on started replica is forbidden, "
                "replica_id:{} (reload = destroy and rebuild)",
                replica_id_.to_string());
        return Status::INVALID_ARGUMENT("replica is already started; reload requires destroy and rebuild");
    }

    RecoverCall call;
    loop_->submit_and_wait(&call);
    if (call.status.fail()) {
        report_fault(call.status);
        return call.status;
    }

    local_state_.store(LocalState::RECOVERING);
    return Status::OK();
}

Status Replica::get_replica_state_for_test(ReplicaStateForTest& result) const {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_running())

    const auto info = runtime_info_.load();
    result.current_term = info->current_term;
    result.commit_index = info->commit_index;
    result.last_applied = info->last_applied;
    result.snapshot_index = info->snapshot_index;
    result.snapshot_term = info->snapshot_term;
    return Status::OK();
}

void Replica::report_fault(Status status) {
    if (local_state_.exchange(LocalState::FAULTED) == LocalState::FAULTED) {
        return;
    }
    LOG_WARN("[Replica] enter faulted, replica_id:{}, status:{}", replica_id_.to_string(), status.to_string());
    if (loop_ == nullptr) {
        return;
    }
    if (loop_->on_loop_thread()) {
        loop_->fault_stop();
        return;
    }
    loop_->submit_async(FaultEvent{std::move(status)});
}

void Replica::shutdown() {
    local_state_.store(LocalState::FAULTED);
    oper_gate_.close_and_wait();
    if (message_dispatcher_) {
        message_dispatcher_->stop();
    }
    if (loop_) {
        loop_->stop_snapshot_writer();
        loop_->stop();
    }
}

ReplicaStatus Replica::get_status() const {
    switch (local_state_.load()) {
        case LocalState::CREATED:
        case LocalState::AWAITING_RECOVER:
        case LocalState::RECOVERING:
            return ReplicaStatus::INITIALIZING;
        case LocalState::FAULTED:
            return ReplicaStatus::FAULTED;
        case LocalState::RUNNING:
            return is_recovering() ? ReplicaStatus::RECOVERING : ReplicaStatus::READY;
    }
    return ReplicaStatus::INITIALIZING;
}

}  // namespace adviskv::storage
