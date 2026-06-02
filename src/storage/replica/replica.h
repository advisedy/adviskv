#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/raft_callback.h"
#include "storage/raft/raft_node.h"
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
    ReplicaStatus get_status() const {
        return (raft_node_ && state_machine_ && persist_ && !is_recovering())
                   ? ReplicaStatus::READY
                   : ReplicaStatus::ADDING;
    }
    bool is_recovering() const { return recovering_.load(); }

    Status put(const PutParam& param);
    Status get(const GetParam& param, Value& value);
    Status del(const DelParam& param);
    Status handle_request_vote(const RequestVoteParam& param,
                               RequestVoteResult& result);
    Status handle_append_entries(const AppendEntriesParam& param,
                                 AppendEntriesResult& result);
    Status handle_install_snapshot(const InstallSnapshotParam& param);

    struct ReplicaStateForTest {
        Term current_term;
        LogIndex commit_index;
        LogIndex last_applied;
        LogIndex snapshot_index;
        Term snapshot_term;
    };
    Status get_replica_state_for_test(ReplicaStateForTest& result) const;

   private:
    friend class ReplicaManager;

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

    Status init(const ReplicaInitParam& param);
    Status recover();
    void shutdown();

    friend class RaftTickTask;
    // tick 回调（Timer 定时调用）
    void on_tick();

    // 把raft_node发生的消息落实他，发送RPC消息
    void flush_messages();

    // 把已经提交但是还没有apply的entry给apply到我们的engine
    // 调用方必须已经持有state_machine_mutex_，内部没有吃锁，放到外部了。
    void apply_committed_entries();

    // 单条 apply
    // Status apply_log_entry(const LogEntry& entry);

    void refresh_recovering_state();

    void try_take_snapshot();

    // 给readIndex准备的
    Status check_self_leader_and_get_read_index(LogIndex& read_index);

    Status ensure_running() const;
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

    std::atomic<bool> recovering_{false};

    // 定时器（驱动 tick）
    // TimerPtr tick_timer_;

    std::atomic<bool> stopping_{false};
    mutable std::shared_mutex life_mutex_;
    mutable std::mutex state_machine_mutex_;
    mutable std::mutex persist_snapshot_mutex_;
};

using ReplicaPtr = std::shared_ptr<Replica>;

}  // namespace adviskv::storage
