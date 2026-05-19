#include "storage/replica/replica.h"

#include <memory>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/raft_callback.h"
#include "storage/raft/raft_node.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::storage {

static constexpr int SNAPSHOT_LIMIT = 1000;

Status Replica::init(const ReplicaInitParam& param) {
    shard_id_ = {.table_id = param.replica_id.table_id,
                 .shard_index = param.replica_id.shard_index};
    replica_id_ = param.replica_id;

    persist_ =
        std::make_unique<PersistEngine>(param.data_dir, param.replica_id);
    RETURN_IF_INVALID_STATUS(persist_->init())

    state_machine_ = std::make_unique<KvStateMachine>(param.engine_type);

    raft_node_ = std::make_unique<RaftNode>(param.replica_id, param.members,
                                            persist_.get());

    // 创建定时器驱动 tick（统一的 tick timer，替代原来的 election + heartbeat
    // 两个 timer）
    // if (param.scheduler) {
    //     tick_timer_ = std::make_shared<Timer>(param.scheduler,
    //                                           [this]() { this->on_tick(); });
    //     tick_timer_->reset(MILLISECONDS(20));
    // }

    return Status::OK();
}

Status Replica::put(const PutParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    if (is_recovering()) {
        return Status::ERROR("replica is recovering");
    }

    // 提交给 RaftNode
    auto [status, new_commit_idx] =
        raft_node_->propose(WriteOpType::PUT, param.key, param.value);
    RETURN_IF_INVALID_STATUS(status)

    // 发送 RaftNode 产出的消息（同步 RPC）
    flush_messages();

    // apply 已提交的日志到 engine
    // 这里推动raftnode里的apply_index是交给了replica外层去控制。
    apply_committed_entries();

    // if (!raft_node_->is_leader()) {
    //     return Status{StatusCode::NOT_LEADER, "leader changed during propose"};
    // }

    if (raft_node_->commit_index() < new_commit_idx) {
        return Status{StatusCode::NOT_YET_COMMIT, "this pyt is not yet commit"};
    }

    return Status::OK();
}

Status Replica::handle_install_snapshot(const InstallSnapshotParam& param) {
    RETURN_IF_INVALID_STATUS(
        raft_node_->prepare_install_snapshot(param.term, param.snapshot_index))
    RETURN_IF_INVALID_STATUS(persist_->append_snapshot_chunk(param))
    if (!param.done) return Status::OK();

    SnapshotPtr snap = std::make_shared<Snapshot>();
    snap->apply_index = param.snapshot_index;
    snap->apply_term = param.snapshot_term;

    RETURN_IF_INVALID_STATUS(persist_->finish_snapshot_receive(snap))
    RETURN_IF_INVALID_STATUS(state_machine_->restore(
        snap, [this](const KvVisitor& visitor) -> Status {
            return persist_->for_each_snapshot_kv(visitor);
        }))
    raft_node_->install_snapshot(param.snapshot_index, param.snapshot_term,
                                 param.term);
    refresh_recovering_state();

    return Status::OK();
}

void Replica::flush_messages() {
    auto messages = raft_node_->extract_messages();
    for (auto& msg : messages) {
        switch (msg.type) {
            case RaftMessageType::REQUEST_VOTE: {
                RequestVoteResult result;
                Status status = raft_sender_.send_request_vote(
                    msg.target, msg.vote_param, result);
                if (status.ok()) {
                    raft_node_->handle_vote_response(msg.target.replica_id,
                                                     result);
                } else {
                    LOG_WARN("[flush_messages] send_request_vote failed: {}",
                             status.msg());
                }
                break;
            }
            case RaftMessageType::APPEND_ENTRIES: {
                AppendEntriesResult result;
                Status status = raft_sender_.send_append_entries(
                    msg.target, msg.append_param, result);
                if (status.ok()) {
                    raft_node_->handle_append_response(msg.target.replica_id,
                                                       result);
                }
                break;
            }
            case RaftMessageType::INSTALL_SNAPSHOT: {
                InstallSnapshotResult result;
                Status status = raft_sender_.send_install_snapshot(
                    msg.target, msg.snapshot_param, *persist_, result);
                if (status.ok()) {
                    raft_node_->handle_install_snapshot_response(
                        msg.target.replica_id, result);
                }
                break;
            }
        }
    }
}

void Replica::apply_committed_entries() {
    auto entries = raft_node_->extract_committed_entries();
    for (const LogEntry& entry : entries) {
        // Status status = apply_log_entry(entry);
        Status status = state_machine_->apply(entry);
        if (status.fail()) {
            LOG_WARN("apply_log_entry failed, index={}, msg={}", entry.index,
                     status.msg());
            return;
        }
        raft_node_->advance_last_applied(entry.index);
    }
    refresh_recovering_state();
}

// Status Replica::apply_log_entry(const LogEntry& entry) {
// if (!engine_) {
//     return {StatusCode::ERROR, "engine is nullptr"};
// }

// switch (entry.op_type) {
//     case WriteOpType::PUT:
//         return engine_->put(entry.key, entry.value);
//     case WriteOpType::DEL:
//         return engine_->del(entry.key);
//     case WriteOpType::NONE:
//         return Status::OK();
//     default:
//         return {StatusCode::INVALID_ARGUMENT, "unknown log op type"};
// }
// }

Status Replica::get(const GetParam& param, Value& value) {
    if (!state_machine_) {
        LOG_WARN(
            "state_machine is nullptr, replica: table_id = {}, shard_index = "
            "{}",
            shard_id_.table_id, shard_id_.shard_index);
        return Status{StatusCode::ERROR, "engine is nullptr"};
    }

    RETURN_IF_INVALID_PARAM(param)
    if (is_recovering()) {
        return Status::ERROR("replica is recovering");
    }

    // TODO: ReadIndex 保证线性一致性 以后待定吧
    if (!raft_node_->is_leader()) {
        return Status{StatusCode::NOT_LEADER, "not leader"};
    }

    Status status = state_machine_->get(param.key, value);
    if (status.fail()) {
        LOG_WARN("engine get is not ok, key = {}, msg = {}", param.key,
                 status.msg());
    }
    return status;
}

Status Replica::del(const DelParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    if (is_recovering()) {
        return Status::ERROR("replica is recovering");
    }

    auto [status, new_commit_idx] =
        raft_node_->propose(WriteOpType::DEL, param.key, "");
    RETURN_IF_INVALID_STATUS(status)

    flush_messages();
    apply_committed_entries();

    if (raft_node_->commit_index() < new_commit_idx) {
        return Status{StatusCode::NOT_YET_COMMIT, "delete is not yet commit"};
    }

    return Status::OK();
}

void Replica::refresh_recovering_state() {
    if (!raft_node_) return;
    raft_node_->maybe_finish_recovering();
    recovering_.store(raft_node_->is_recovering());
}

Status Replica::handle_request_vote(const RequestVoteParam& param,
                                    RequestVoteResult& result) {
    raft_node_->handle_request_vote(param, result);
    return Status::OK();
}

Status Replica::handle_append_entries(const AppendEntriesParam& param,
                                      AppendEntriesResult& result) {
    // 这里是作为follower那边的handle，会更新commit_idx
    raft_node_->handle_append_entries(param, result);

    // 收到 AppendEntries 后，可能有新的 committed entries 需要 apply
    apply_committed_entries();

    return Status::OK();
}

void Replica::try_take_snapshot() {
    // LogIndex last_index = raft_node_->last_log_index();
    // LogIndex apply_index = state_machine_->apply_index();
    LogIndex last_apply_index = raft_node_->last_applied(),
             snapshot_index = state_machine_->apply_index();
    if (last_apply_index - snapshot_index < SNAPSHOT_LIMIT) return;
    Status status = persist_->do_snapshot(*state_machine_);
    if (status.ok()) {
        // 持久化成功了，这边得截断wal日志了。
        raft_node_->truncate_log(state_machine_->apply_index());
    } else {
        LOG_WARN("...");
    }
}

void Replica::on_tick() {
    raft_node_->tick();
    flush_messages();

    // 如果put最开始的时候没有推进commit_idx（例如其他的followers都太慢了或者有问题了），
    // 后来这边跑心跳的时候才有回应，这个时候就应该apply一下commmit_entry
    // 所以这个是专门为了raft_node是leader去发送心跳的时候准备的。

    apply_committed_entries();

    // 重新调度下一次 tick（Timer 是 one-shot 的）
    // if (tick_timer_) {
    //     tick_timer_->reset(MILLISECONDS(20));
    // }
    try_take_snapshot();
}

Status Replica::recover() {
    PersistEngine::RecoverResult result;
    RETURN_IF_INVALID_STATUS(persist_->recover(result))

    if (result.snapshot) {
        RETURN_IF_INVALID_STATUS(state_machine_->restore(
            result.snapshot, [this](const KvVisitor& visitor) -> Status {
                return persist_->for_each_snapshot_kv(visitor);
            }))
        raft_node_->install_snapshot(result.snapshot->apply_index,
                                     result.snapshot->apply_term, 0);
    }

    raft_node_->update_raft_meta(result.raft_meta);
    raft_node_->update_log_entries(result.wal_entries);
    if (result.wal_recovery.action == WalRecoveryAction::NEED_RAFT_CATCHUP) {
        recovering_.store(true);
        raft_node_->enter_recovering(
            result.wal_recovery.recovery_target_commit_index);
    }
    refresh_recovering_state();
    return Status::OK();
}

}  // namespace adviskv::storage
