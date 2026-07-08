#include "storage/replica/replica.h"

#include <memory>
#include <mutex>
#include <utility>

#include "common/background_task.h"
#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/model/type.h"
#include "common/oper_gate.h"
#include "common/status.h"
#include "common/types.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/replica_applier.h"
#include "storage/replica/replica_loop.h"
#include "storage/replica/replica_message_dispatcher.h"
#include "storage/replica/replica_read_index_checker.h"
#include "storage/replica/replica_snapshot_coordinator.h"

namespace adviskv::storage {

namespace {

constexpr Milliseconds K_REPLICA_APPLY_TASK_INTERVAL = Milliseconds{5};
constexpr Milliseconds K_REPLICA_PROPOSAL_COMMIT_TIMEOUT = Milliseconds{1000};

}  // namespace

class ReplicaApplyTask : public BackgroundTask {
public:
    explicit ReplicaApplyTask(Replica* replica) : replica_(replica) {}

    void run() override {
        if (!replica_) return;
        replica_->apply_committed_entries_from_task();
    }

private:
    Replica* replica_{nullptr};
};

Replica::Replica() = default;

Replica::~Replica() { shutdown(); }

Status Replica::init(const ReplicaInitParam& param) {
    shard_id_ = ShardID{param.replica_id.table_id, param.replica_id.shard_index};
    replica_id_ = param.replica_id;

    persist_ = std::make_unique<PersistEngine>(param.runtime.data_dir, param.replica_id);
    RETURN_IF_INVALID_STATUS(persist_->init())

    state_machine_ = std::make_unique<KvStateMachine>(param.engine_type);

    raft_core_ = std::make_unique<RaftCore>(param.replica_id, param.members);
    message_dispatcher_ = std::make_unique<ReplicaMessageDispatcher>(param.runtime.raft_rpc_timeout_ms, *persist_,
                                                                     persist_snapshot_mutex_, [this](Event event) {
                                                                         if (loop_) {
                                                                             loop_->async_submit(std::move(event));
                                                                         }
                                                                     });
    context_ = std::make_unique<ReplicaContext>(ReplicaContext{
            replica_id_,
            *raft_core_,
            *persist_,
            *state_machine_,
            *message_dispatcher_,
            state_machine_mutex_,
            persist_snapshot_mutex_,
            raft_core_mutex_,
            [this](Status status) { return fault_if_fail(status); },
            [this]() { enter_local_state_faulted(); },
    });
    loop_ = std::make_unique<ReplicaLoop>(*context_);
    applier_ = std::make_unique<ReplicaApplier>(*context_);
    apply_task_ = std::make_unique<ReplicaApplyTask>(this);
    read_index_checker_ = std::make_unique<ReplicaReadIndexChecker>(*context_, *loop_);
    snapshot_coordinator_ = std::make_unique<ReplicaSnapshotCoordinator>(*context_, *loop_);

    loop_->start();
    message_dispatcher_->start();
    enter_local_state_running();
    apply_task_->start(K_REPLICA_APPLY_TASK_INTERVAL);
    return Status::OK();
}

Status Replica::put(const PutParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_local_state_running());
    {
        std::lock_guard lock(raft_core_mutex_);
        if (raft_core_->is_recovering()) {
            return Status::IS_RECOVERING("replica is recovering");
        }
    }

    ProposeCall call{ProposeParam::write(WriteOpType::PUT, param.key, param.value), K_REPLICA_PROPOSAL_COMMIT_TIMEOUT};
    loop_->sync_submit(&call);
    RETURN_IF_INVALID_STATUS(call.status)

    // // 这里有一个容易误解的点：flush_messages()
    // //  虽然是同步发送 RPC， apply_committed_entries() 也会立刻把已经
    // committed
    // //  的日志应用到状态机， 但这并不意味着本次 propose 出来的 target_index
    // //  一定已经 committed。
    // //
    // //  原因是 flush_messages() 只发送当前这一轮 pending messages。
    // //  如果 follower 日志落后，第一次 AppendEntries 可能会因为
    // //  prev_log_index / prev_log_term 不匹配而失败；leader 这时通常只是回退
    // //  该 follower 的 next_index，需要后续多轮 AppendEntries 才能把 follower
    // //  补齐并拿到多数派确认。
    // //
    // //  因此，一轮 flush 后 commit_index 可能仍然小于 target_index。
    // //  apply_committed_entries() 只能 apply 到当前 commit_index，不能把尚未
    // //  committed 的 target_index 应用到状态机。 apply_committed_entries();

    // // apply 已提交的日志到 engine
    // // 这里推动raftnode里的apply_index是交给了replica外层去控制。
    // {
    //     ADVISKV_METRICS_TIMER("storage_replica_put_apply_committed");
    //     std::lock_guard lock(state_machine_mutex_);
    //     RETURN_IF_INVALID_STATUS(
    //         fault_if_fail(applier_->apply_committed_entries()))
    // }

    // if (!raft_node_->is_leader()) {
    //     return Status{StatusCode::NOT_LEADER, "leader changed during
    //     propose"};
    // }

    // if (raft_node_->commit_index() < new_commit_idx) {
    //     // 日志已经进入当前 leader 本地
    //     // log，但这次请求返回前还没确认被多数派提交。
    //     //
    //     所以是有多种情况，有可能会在之后的这个操作里面，然后把当前的这个没有提交
    //     //
    //     的操作给提交掉。但是也有可能会被新的leader然后到时候给覆盖掉。所以这里其
    //     //
    //     实真正的语义是希望让客户那边去重试，或者说是用Get自己去判断一下这个结果，
    //     // 我们这边的结果其实返回的应该是未知。
    //     //
    //     //
    //     至于发生的原因的话，有可能是follower那边的prev_log_index没有对齐，导致一次发送append_entries不够。
    //     //
    //     但是或者也有可能是因为自己的message被别的线程的PUT操作给吃掉，然后一起发送了，
    //     //
    //     那自己这边的flush_message就是空的，就会很快结束进入下一阶段，可能这个
    //     //
    //     时候commit_Idx还没有被别的线程给推进，所以就会此时这边的判断就还会小于
    //     // commit_idx
    //     // TODO 那这边以后想办法看要不要搞搞优化啥的
    //     return Status{StatusCode::NOT_YET_COMMIT, "this put is not yet
    //     commit"};
    // }

    notify_apply_task();
    return Status::OK();
}

// 收到了来自leader的快照下载要求
Status Replica::handle_install_snapshot(const InstallSnapshotParam& param) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_local_state_running())

    return snapshot_coordinator_->handle_install_snapshot(param);
}

Status Replica::get(const GetParam& param, Value& value) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_local_state_running())

    if (!state_machine_) {
        LOG_WARN(
                "state_machine is nullptr, replica: table_id = {}, shard_index = "
                "{}",
                shard_id_.table_id, shard_id_.shard_index);
        enter_local_state_faulted();
        return Status{StatusCode::ERROR, "engine is nullptr"};
    }

    {
        std::lock_guard lock(raft_core_mutex_);
        if (raft_core_->is_recovering()) {
            return Status::IS_RECOVERING("replica is recovering");
        }
    }

    LogIndex read_index;
    RETURN_IF_INVALID_STATUS(read_index_checker_->check_self_leader_and_get_read_index(read_index))

    std::lock_guard lock(state_machine_mutex_);
    RETURN_IF_INVALID_STATUS(fault_if_fail(applier_->apply_committed_entries()))
    if (state_machine_->apply_index() < read_index) {
        return Status::NOT_YET_COMMIT("state machine not yet apply");
    }

    Status status = state_machine_->get(param.key, value);
    if (status.fail()) {
        LOG_WARN("engine get is not ok, key = {}, msg = {}", param.key, status.msg());
        if (status.code() != StatusCode::KEY_NOT_FOUND) {
            enter_local_state_faulted();
        }
    }

    return status;
}

Status Replica::del(const DelParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_local_state_running());
    {
        std::lock_guard lock(raft_core_mutex_);
        if (raft_core_->is_recovering()) {
            return Status::IS_RECOVERING("replica is recovering");
        }
    }

    ProposeCall call{ProposeParam::write(WriteOpType::DEL, param.key, ""), K_REPLICA_PROPOSAL_COMMIT_TIMEOUT};
    loop_->sync_submit(&call);
    RETURN_IF_INVALID_STATUS(call.status)

    notify_apply_task();
    // // delete 与 put 的完成语义保持一致：只有当当前请求对应的日志在本次返回前
    // // 已经达到 committed 条件时，才返回 OK。
    // //
    // // 如果这轮 flush 之后 commit_index 仍然小于本次 propose 的目标 index，
    // // 说明当前 leader 已经接受了这条删除日志，但还没有办法在这个返回点确认它
    // // 已经被多数派提交。此时它后续可能提交成功，也可能在 leader 变化后失效，
    // // 因此对客户端而言属于“结果未决”，而不是普通失败。
    // {
    //     ADVISKV_METRICS_TIMER("storage_replica_delete_apply_committed");
    //     std::lock_guard lock(state_machine_mutex_);
    //     RETURN_IF_INVALID_STATUS(
    //         fault_if_fail(applier_->apply_committed_entries()))
    // }

    // if (!raft_node_->is_leader()) {
    //     return Status{StatusCode::NOT_LEADER,
    //                   "leader changed during delete propose"};
    // }

    // if (raft_node_->commit_index() < new_commit_idx) {
    //     return Status{
    //         StatusCode::NOT_YET_COMMIT,
    //         "delete accepted by leader but commit is not yet confirmed"};
    // }

    return Status::OK();
}

Status Replica::handle_request_vote(const RequestVoteParam& param, RequestVoteResult& result) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_local_state_running())

    RequestVoteCall call{param};
    loop_->sync_submit(&call);
    RETURN_IF_INVALID_STATUS(call.status)
    result = call.result;
    return Status::OK();
}

Status Replica::handle_append_entries(const AppendEntriesParam& param, AppendEntriesResult& result) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_local_state_running())

    // 这里是作为follower那边的handle，会更新commit_idx
    {
        ADVISKV_METRICS_TIMER("storage_replica_handle_append_entries_raft_step");
        AppendEntriesCall call{param};
        loop_->sync_submit(&call);
        RETURN_IF_INVALID_STATUS(call.status)
        result = call.result;
    }

    notify_apply_task();

    return Status::OK();
}

Status Replica::add_member(const PeerMember& member) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_local_state_running());
    {
        std::lock_guard lock(raft_core_mutex_);
        if (raft_core_->is_recovering()) {
            return Status::IS_RECOVERING("replica is recovering");
        }
    }
    LOG_INFO("[Replica] add_member, replica_id:{}, add target:{}", replica_id_.to_string(), member.to_string());
    AddMemberCall call{member};
    loop_->sync_submit(&call);
    RETURN_IF_INVALID_STATUS(call.status)
    notify_apply_task();
    return Status::OK();
}

Status Replica::remove_member(const ReplicaID& replica_id) {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_local_state_running());
    {
        std::lock_guard lock(raft_core_mutex_);
        if (raft_core_->is_recovering()) {
            return Status::IS_RECOVERING("replica is recovering");
        }
    }
    LOG_INFO("[Replica] remove_member, replica_id:{}, remove target:{}", replica_id_.to_string(),
             replica_id.to_string());
    RemoveMemberCall call{replica_id};
    loop_->sync_submit(&call);
    RETURN_IF_INVALID_STATUS(call.status)
    notify_apply_task();
    return Status::OK();
}

void Replica::notify_apply_task() {
    if (apply_task_) {
        apply_task_->notify();
    }
}

void Replica::apply_committed_entries_from_task() {
    ADVISKV_METRICS_TIMER("storage_replica_async_apply_committed");
    ADVISKV_METRICS_COUNTER("storage_replica_async_apply_committed_request");

    OperGate::Guard guard;
    if (oper_gate_.acquire(guard).fail()) {
        ADVISKV_METRICS_COUNTER("storage_replica_async_apply_committed_skipped");
        return;
    }
    if (ensure_local_state_running().fail() || !applier_) {
        ADVISKV_METRICS_COUNTER("storage_replica_async_apply_committed_skipped");
        return;
    }

    {
        std::lock_guard lock(raft_core_mutex_);
        if (raft_core_->commit_index() <= raft_core_->last_applied()) {
            ADVISKV_METRICS_COUNTER("storage_replica_async_apply_committed_none");
            return;
        }
    }

    std::lock_guard lock(state_machine_mutex_);
    Status status = fault_if_fail(applier_->apply_committed_entries());
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_replica_async_apply_committed_failure");
        LOG_WARN("async apply committed entries failed, replica_id={}, status={}", replica_id_.to_string(),
                 status.to_string());
        return;
    }
    ADVISKV_METRICS_COUNTER("storage_replica_async_apply_committed_success");
}

void Replica::on_tick() {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED_VOID(oper_gate_)
    if (ensure_local_state_running().fail()) return;

    loop_->async_submit(TickEvent{});
    notify_apply_task();

    // IGNORE_RESULT(raft_effect_runner_->run_raft_step([&](RaftEffects&
    // effects) {
    //     raft_node_->tick(effects);
    //     return Status::OK();
    // }));

    // //
    // 如果put最开始的时候没有推进commit_idx（例如其他的followers都太慢了或者有问题了），
    // // 后来这边跑心跳的时候才有回应，这个时候就应该apply一下commmit_entry
    // // 所以这个是专门为了raft_node是leader去发送心跳的时候准备的。

    // {
    //     std::lock_guard lock(state_machine_mutex_);
    //     if (fault_if_fail(applier_->apply_committed_entries()).fail())
    //     return;
    // }

    snapshot_coordinator_->try_take_snapshot();
}

Status Replica::recover() {
    enter_local_state_starting();
    PersistEngine::RecoverResult result;
    if (Status status = persist_->recover(result); status.fail()) {
        LOG_WARN("replica's persist recover failed. msg:{}", status.msg());
        enter_local_state_faulted();
        return status;
    }

    if (result.snapshot) {
        std::lock_guard lock(state_machine_mutex_);
        RETURN_IF_INVALID_STATUS(
                fault_if_fail(state_machine_->restore(result.snapshot, [this](const KvVisitor& visitor) -> Status {
                    return persist_->for_each_snapshot_kv(visitor);
                })))
    }

    {
        std::lock_guard lock(raft_core_mutex_);
        if (result.snapshot) {
            raft_core_->install_local_snapshot(InstallSnapshotContext{
                    result.snapshot->apply_index,
                    result.snapshot->apply_term,
                    result.snapshot->members,
            });
        }
        raft_core_->update_raft_meta(result.raft_meta);
        raft_core_->update_log_entries(result.wal_entries);

        if (result.need_recover) {
            raft_core_->enter_recovering();
        }
    }

    enter_local_state_running();
    return Status::OK();
}

Status Replica::get_replica_state_for_test(ReplicaStateForTest& result) const {
    RETURN_IF_OPER_GUARD_ACQUIRE_FAILED(oper_gate_)
    RETURN_IF_INVALID_STATUS(ensure_local_state_running())

    std::lock_guard lock(raft_core_mutex_);
    result.current_term = raft_core_->current_term();
    result.commit_index = raft_core_->commit_index();
    result.last_applied = raft_core_->last_applied();
    result.snapshot_index = raft_core_->snapshot_index();
    result.snapshot_term = raft_core_->snapshot_term();
    return Status::OK();
}

void Replica::shutdown() {
    enter_local_state_faulted();
    if (apply_task_) {
        apply_task_->stop();
    }
    oper_gate_.close_and_wait();
    if (message_dispatcher_) {
        message_dispatcher_->stop();
    }
    if (loop_) {
        loop_->stop();
    }
}

ReplicaStatus Replica::get_status() const {
    switch (local_state_) {
        case LocalState::STARTING:
            return ReplicaStatus::INITIALIZING;
        case LocalState::FAULTED:
            return ReplicaStatus::FAULTED;

        case LocalState::RUNNING:
        default:
            break;
    }
    std::lock_guard lock(raft_core_mutex_);
    if (!raft_core_) return ReplicaStatus::INITIALIZING;
    if (raft_core_->is_recovering()) return ReplicaStatus::RECOVERING;
    if (raft_core_->is_ready()) return ReplicaStatus::READY;

    return ReplicaStatus::INITIALIZING;
}

}  // namespace adviskv::storage