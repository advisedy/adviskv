#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "common/define.h"
#include "common/log.h"
#include "common/model/model.h"
#include "common/oper_gate.h"
#include "common/published_view.h"
#include "common/status.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/fault_reporter.h"

namespace adviskv::storage {

class ReplicaManager;
class ReplicaLoop;
class ReplicaMessageDispatcher;

struct ReplicaRuntimeInfo {
    ReplicaRole role{ReplicaRole::FOLLOWER};
    Term current_term{0};
    std::vector<RaftMember> members;
    LogIndex snapshot_index{0};
    Term snapshot_term{0};
    LogIndex commit_index{0};
    LogIndex last_applied{0};
    bool is_recovering{false};
};

class Replica {
public:
    Replica();
    ~Replica();

    ShardID get_shard_id() const { return shard_id_; }
    ReplicaID get_replica_id() const { return replica_id_; }

    ReplicaRole get_role() const { return runtime_info_.load()->role; }
    std::vector<RaftMember> get_raft_members() const { return runtime_info_.load()->members; }
    Term current_term() const { return runtime_info_.load()->current_term; }
    LogIndex snapshot_index() const { return runtime_info_.load()->snapshot_index; }
    bool is_recovering() const { return runtime_info_.load()->is_recovering; }

    ReplicaStatus get_status() const;

    Status put(const PutParam& param);
    Status get(const GetParam& param, Value& value);
    Status del(const DelParam& param);
    Status cas(const CasParam& param);
    Status handle_request_vote(const RequestVoteParam& param, RequestVoteResult& result);
    Status handle_pre_vote(const PreVoteParam& param, PreVoteResult& result);
    Status handle_append_entries(const AppendEntriesParam& param, AppendEntriesResult& result);
    Status handle_install_snapshot(const InstallSnapshotParam& param);
    Status add_member(const PeerMember& member);
    Status remove_member(const ReplicaID& replica_id);

private:
    friend class ReplicaManager;

    Status init(const ReplicaInitParam& param);
    Status recover();
    Status start();
    void shutdown();

    bool needs_recover() const { return local_state_.load() == LocalState::AWAITING_RECOVER; }

    friend class RaftTickTask;
    void on_tick();

    enum class LocalState {
        CREATED = 0,
        AWAITING_RECOVER = 1,
        RECOVERING = 2,
        RUNNING = 3,
        FAULTED = 4,
    };

    Status ensure_running() const {
        switch (local_state_.load()) {
            case LocalState::RUNNING:
                return Status::OK();
            case LocalState::FAULTED:
                return Status::IS_FAULTED("replica is not running, local_state:faulted");
            case LocalState::AWAITING_RECOVER:
                return Status::NOT_INIT("replica awaits recover, start is not allowed yet");
            case LocalState::RECOVERING:
            case LocalState::CREATED:
            default:
                return Status::NOT_INIT("replica is not running, local_state:not started");
        }
    }

    Status ensure_serving() const {
        RETURN_IF_INVALID_STATUS(ensure_running())
        if (is_recovering()) {
            return Status::IS_RECOVERING("replica is recovering");
        }
        return Status::OK();
    }

    Status fault_if_fail(Status status) {
        if (status.fail()) {
            report_fault(status);
        }
        return status;
    }

    void report_fault(Status status);

    ShardID shard_id_;
    ReplicaID replica_id_;

    std::unique_ptr<StateMachine> state_machine_;

    std::unique_ptr<RaftCore> raft_core_;

    std::unique_ptr<PersistEngine> persist_;

    std::unique_ptr<ReplicaMessageDispatcher> message_dispatcher_;

    std::atomic<LocalState> local_state_{LocalState::CREATED};

    OperGate oper_gate_;

    std::unique_ptr<ReplicaLoop> loop_;

    PublishedView<ReplicaRuntimeInfo> runtime_info_;

public:
    struct ReplicaStateForTest {
        Term current_term;
        LogIndex commit_index;
        LogIndex last_applied;
        LogIndex snapshot_index;
        Term snapshot_term;
    };
    Status get_replica_state_for_test(ReplicaStateForTest& result) const;

    LogIndex snapshot_inflight_index_for_test(const ReplicaID& target) const {
        return raft_core_->inflight_snapshot_index_for_test(target);
    }

    LogIndex next_index_for_test(const ReplicaID& target) const {
        return raft_core_->next_index_for_test(target);
    }
    ///////////
};

using ReplicaPtr = std::shared_ptr<Replica>;

}
