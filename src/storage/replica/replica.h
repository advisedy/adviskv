#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/model/replica_status.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/raft_node.h"
#include "storage/raft/raft_sender.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::storage {

class ReplicaManager;

class Replica {
   public:
    TableID get_table_id() const { return shard_id_.table_id; }
    ShardID get_shard_id() const { return shard_id_; }
    ReplicaID get_replica_id() const { return replica_id_; }
    ReplicaRole get_role() const {
        return raft_node_ ? raft_node_->role() : ReplicaRole::FOLLOWER;
    }
    Term current_term() const {
        return raft_node_ ? raft_node_->current_term() : 0;
    }
    LogIndex snapshot_index() const {
        return raft_node_ ? raft_node_->snapshot_index() : 0;
    }

    // 这个是返回给外部的ReplicaStatus的， 内部使用LocalState
    ReplicaStatus get_status() const;

    bool is_recovering() const {
        return raft_node_ && raft_node_->is_recovering();
    }

    Status put(const PutParam& param);
    Status get(const GetParam& param, Value& value);
    Status del(const DelParam& param);
    Status handle_request_vote(const RequestVoteParam& param,
                               RequestVoteResult& result);
    Status handle_append_entries(const AppendEntriesParam& param,
                                 AppendEntriesResult& result);
    Status handle_install_snapshot(const InstallSnapshotParam& param);

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

   private:
    friend class ReplicaManager;

    Status init(const ReplicaInitParam& param);
    Status recover();
    void shutdown();

    friend class RaftTickTask;
    // tick 回调（Timer 定时调用）
    void on_tick();

    Status persist_raft_effects(const RaftEffects& effects);
    Status send_raft_messages(std::vector<RaftMessage> messages);
    Status send_raft_message(const RaftMessage& msg);

    using RaftStepFunc = std::function<Status(RaftEffects&)>;
    Status run_raft_step(RaftStepFunc&& step);

    // 把已经提交但是还没有apply的entry给apply到我们的engine
    // 调用方必须已经持有state_machine_mutex_，内部没有吃锁，放到外部了。
    Status apply_committed_entries();
    Status apply_log_entry(const LogEntry& entry);
    Status apply_kv_log_entry(const LogEntry& entry);

    void try_take_snapshot();

    // 给readIndex准备的
    Status check_self_leader_and_get_read_index(LogIndex& read_index);

    Status ensure_running() const;

    void enter_local_state_faulted() {
        local_state_.store(LocalState::FAULTED);
    }
    void enter_local_state_starting() {
        local_state_.store(LocalState::STARTING);
    }
    void enter_local_state_running() {
        local_state_.store(LocalState::RUNNING);
    }

    Status fault_if_fail(Status status) {
        if (status.fail()) {
            LOG_WARN("replica enter fualted: Status:{}", status.to_string());
            enter_local_state_faulted();
        }
        return status;
    }

    class OperGuard {
       public:
        OperGuard() = default;
        DISALLOW_COPY_AND_ASSIGN(OperGuard)
        ALLOW_MOVE_AND_ASSIGN(OperGuard)

       private:
        friend class Replica;
        explicit OperGuard(std::shared_lock<std::shared_mutex>&& life_lock)
            : life_lock_(std::move(life_lock)) {}

        std::shared_lock<std::shared_mutex> life_lock_;
    };
    Status acquire_operation(OperGuard& guard) const;

    ShardID shard_id_;
    ReplicaID replica_id_;

    std::unique_ptr<StateMachine> state_machine_;

    // raft
    // replica算是给raft_node包了一层，会帮忙处理RPC的事情和状态机落实的事情
    // 让raft_node专心走协议的事情
    std::unique_ptr<RaftNode> raft_node_;

    std::unique_ptr<PersistEngine> persist_;

    // 通信
    RaftSender raft_sender_;

    enum class ReplicaLocalState {
        STARTING = 0,  // 代表目前还无法和外界正常服务
        RUNNING = 1,   // 可以和外界存在正常服务，不保证所有正常服务都OK
        FAULTED = 2,   // 错误，得需要重启才可以好。
    };
    using LocalState = ReplicaLocalState;
    std::atomic<LocalState> local_state_{LocalState::STARTING};

    // 定时器（驱动 tick）
    // TimerPtr tick_timer_;

    std::atomic<bool> stopping_{false};
    mutable std::shared_mutex life_mutex_;
    mutable std::mutex state_machine_mutex_;
    mutable std::mutex persist_snapshot_mutex_;
    mutable std::mutex raft_step_mutex_;
};

using ReplicaPtr = std::shared_ptr<Replica>;

}  // namespace adviskv::storage