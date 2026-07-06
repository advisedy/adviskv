#pragma once

#include <fmt/format.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/model/type.h"
#include "common/oper_gate.h"
#include "common/status.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::storage {

class ReplicaManager;
class ReplicaApplier;
class ReplicaApplyTask;
class ReplicaLoop;
class ReplicaMessageDispatcher;
class ReplicaReadIndexChecker;
class ReplicaSnapshotCoordinator;

struct ReplicaContext {
    ReplicaID replica_id;

    RaftCore& raft_core;
    PersistEngine& persist;
    StateMachine& state_machine;
    ReplicaMessageDispatcher& message_dispatcher;

    std::mutex& state_machine_mutex;
    std::mutex& persist_snapshot_mutex;
    std::mutex& raft_core_mutex;

    std::function<Status(Status)> fault_if_fail;
    std::function<void()> enter_faulted;
};

class Replica {
   public:
    Replica();
    ~Replica();

    TableID get_table_id() const { return shard_id_.table_id; }
    ShardID get_shard_id() const { return shard_id_; }
    ReplicaID get_replica_id() const { return replica_id_; }
    ReplicaRole get_role() const {
        std::lock_guard lock(raft_core_mutex_);
        return raft_core_ ? raft_core_->role() : ReplicaRole::FOLLOWER;
    }
    RaftMemberType get_member_type() const {
        std::lock_guard lock(raft_core_mutex_);
        return raft_core_ ? raft_core_->member_type(replica_id_)
                          : RaftMemberType::NON_MEMBER;
    }
    std::vector<RaftMember> get_raft_members() const {
        std::lock_guard lock(raft_core_mutex_);
        return raft_core_ ? raft_core_->raft_members() : std::vector<RaftMember>{};
    }
    Term current_term() const {
        std::lock_guard lock(raft_core_mutex_);
        return raft_core_ ? raft_core_->current_term() : 0;
    }
    LogIndex snapshot_index() const {
        std::lock_guard lock(raft_core_mutex_);
        return raft_core_ ? raft_core_->snapshot_index() : 0;
    }

    // 这个是返回给外部的ReplicaStatus的， 内部使用LocalState
    ReplicaStatus get_status() const;

    bool is_recovering() const {
        std::lock_guard lock(raft_core_mutex_);
        return raft_core_ && raft_core_->is_recovering();
    }

    Status put(const PutParam& param);
    Status get(const GetParam& param, Value& value);
    Status del(const DelParam& param);
    Status handle_request_vote(const RequestVoteParam& param,
                               RequestVoteResult& result);
    Status handle_append_entries(const AppendEntriesParam& param,
                                 AppendEntriesResult& result);
    // 收到了来自leader的快照下载要求
    Status handle_install_snapshot(const InstallSnapshotParam& param);
    Status add_member(const PeerMember& member);
    Status remove_member(const ReplicaID& replica_id);

   private:
    friend class ReplicaManager;
    friend class ReplicaApplyTask;

    Status init(const ReplicaInitParam& param);
    Status recover();
    void shutdown();
    void notify_apply_task();
    void apply_committed_entries_from_task();

    friend class RaftTickTask;
    void on_tick();

    void enter_local_state_faulted() {
        local_state_.store(LocalState::FAULTED);
    }
    void enter_local_state_starting() {
        local_state_.store(LocalState::STARTING);
    }
    void enter_local_state_running() {
        local_state_.store(LocalState::RUNNING);
    }

    Status ensure_local_state_running() const {
        if (auto state = local_state_.load(); state != LocalState::RUNNING) {
            return Status::ERROR(fmt::format(
                "replica is not running, local_state:{}",
                (state == LocalState::FAULTED ? "faulted" : "starting")));
        }
        return Status::OK();
    }

    Status fault_if_fail(Status status) {
        if (status.fail()) {
            LOG_WARN("[Replica] fault_if_fail, replica enter faulted: Status:{}", status.to_string());
            enter_local_state_faulted();
        }
        return status;
    }

    ShardID shard_id_;
    ReplicaID replica_id_;

    std::unique_ptr<StateMachine> state_machine_;

    std::unique_ptr<RaftCore> raft_core_;

    std::unique_ptr<PersistEngine> persist_;

    std::unique_ptr<ReplicaMessageDispatcher> message_dispatcher_;

    enum class ReplicaLocalState {
        STARTING = 0,  // 代表目前还无法和外界正常服务
        RUNNING = 1,   // 可以和外界存在正常服务，不保证所有正常服务都OK
        FAULTED = 2,   // 错误，得需要重启才可以好。
    };
    using LocalState = ReplicaLocalState;
    std::atomic<LocalState> local_state_{LocalState::STARTING};

    OperGate oper_gate_;
    mutable std::mutex state_machine_mutex_;
    mutable std::mutex persist_snapshot_mutex_;
    mutable std::mutex raft_core_mutex_;

    std::unique_ptr<ReplicaContext> context_;
    std::unique_ptr<ReplicaLoop> loop_;
    std::unique_ptr<ReplicaApplier> applier_;
    std::unique_ptr<ReplicaApplyTask> apply_task_;
    std::unique_ptr<ReplicaSnapshotCoordinator> snapshot_coordinator_;
    std::unique_ptr<ReplicaReadIndexChecker> read_index_checker_;

   public:
    ///////////
    // 专门给测试开了个接口
    struct ReplicaStateForTest {
        Term current_term;
        LogIndex commit_index;
        LogIndex last_applied;
        LogIndex snapshot_index;
        Term snapshot_term;
    };
    Status get_replica_state_for_test(ReplicaStateForTest& result) const;
    ///////////
};

using ReplicaPtr = std::shared_ptr<Replica>;

}  // namespace adviskv::storage