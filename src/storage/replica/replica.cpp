#include "storage/replica/replica.h"

#include <memory>
#include <mutex>

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/raft_node.h"
#include "storage/raft/raft_sender.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::storage {
namespace {

static constexpr int SNAPSHOT_LIMIT = 1000;

}  // namespace

Status Replica::init(const ReplicaInitParam& param) {
    shard_id_ =
        ShardID{param.replica_id.table_id, param.replica_id.shard_index};
    replica_id_ = param.replica_id;

    persist_ = std::make_unique<PersistEngine>(param.runtime.data_dir,
                                               param.replica_id);
    RETURN_IF_INVALID_STATUS(persist_->init())

    state_machine_ = std::make_unique<KvStateMachine>(param.engine_type);

    raft_node_ = std::make_unique<RaftNode>(param.replica_id, param.members,
                                            persist_.get());
    raft_sender_.set_timeout_ms(param.runtime.raft_rpc_timeout_ms);

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

    OperGuard guard;
    RETURN_IF_INVALID_STATUS(acquire_operation(guard))

    if (is_recovering()) {
        return Status::IS_RECOVERING("replica is recovering");
    }

    // 提交给 RaftNode
    LogIndex new_commit_idx = 0;
    Status status;
    {
        ADVISKV_METRICS_TIMER("storage_replica_put_propose");
        auto propose_result =
            raft_node_->propose(WriteOpType::PUT, param.key, param.value);
        status = propose_result.first;
        new_commit_idx = propose_result.second;
    }
    RETURN_IF_INVALID_STATUS(status)

    // 发送 RaftNode 产出的消息（同步 RPC）
    {
        ADVISKV_METRICS_TIMER("storage_replica_put_flush_messages");
        flush_messages();
    }

    // (AI重新表述过的版本，自己的语言描述太烂了）：
    //
    // 这里有一个容易误解的点：flush_messages()
    // 虽然是同步发送 RPC， apply_committed_entries() 也会立刻把已经 committed
    // 的日志应用到状态机， 但这并不意味着本次 propose 出来的 target_index
    // 一定已经 committed。
    //
    // 原因是 flush_messages() 只发送当前这一轮 pending messages。
    // 如果 follower 日志落后，第一次 AppendEntries 可能会因为
    // prev_log_index / prev_log_term 不匹配而失败；leader 这时通常只是回退
    // 该 follower 的 next_index，需要后续多轮 AppendEntries 才能把 follower
    // 补齐并拿到多数派确认。
    //
    // 因此，一轮 flush 后 commit_index 可能仍然小于 target_index。
    // apply_committed_entries() 只能 apply 到当前 commit_index，不能把尚未
    // committed 的 target_index 应用到状态机。 apply_committed_entries();

    // apply 已提交的日志到 engine
    // 这里推动raftnode里的apply_index是交给了replica外层去控制。
    {
        ADVISKV_METRICS_TIMER("storage_replica_put_apply_committed");
        std::lock_guard lock(state_machine_mutex_);
        apply_committed_entries();
    }

    if (!raft_node_->is_leader()) {
        return Status{StatusCode::NOT_LEADER, "leader changed during propose"};
    }

    if (raft_node_->commit_index() < new_commit_idx) {
        // 日志已经进入当前 leader 本地
        // log，但这次请求返回前还没确认被多数派提交。
        //  所以是有多种情况，有可能会在之后的这个操作里面，然后把当前的这个没有提交
        // 的操作给提交掉。但是也有可能会被新的leader然后到时候给覆盖掉。所以这里其
        // 实真正的语义是希望让客户那边去重试，或者说是用Get自己去判断一下这个结果，
        // 我们这边的结果其实返回的应该是未知。
        //
        // 至于发生的原因的话，有可能是follower那边的prev_log_index没有对齐，导致一次发送append_entries不够。
        // 但是或者也有可能是因为自己的message被别的线程的PUT操作给吃掉，然后一起发送了，
        // 那自己这边的flush_message就是空的，就会很快结束进入下一阶段，可能这个
        // 时候commit_Idx还没有被别的线程给推进，所以就会此时这边的判断就还会小于
        // commit_idx
        // TODO 那这边以后想办法看要不要搞搞优化啥的
        return Status{StatusCode::NOT_YET_COMMIT, "this put is not yet commit"};
    }

    return Status::OK();
}

Status Replica::handle_install_snapshot(const InstallSnapshotParam& param) {
    LOG_INFO(
        "replica:{} is handling to install snapshot from replica:{}. The "
        "snapshot index:{}, snapshot term:{}",
        param.to_replica_id.to_string(), param.from_replica_id.to_string(),
        param.snapshot_index, param.snapshot_term);

    OperGuard guard;
    RETURN_IF_INVALID_STATUS(acquire_operation(guard))

    RETURN_IF_INVALID_STATUS(
        raft_node_->prepare_install_snapshot(param.term, param.snapshot_index));

    SnapshotPtr snap;
    {
        std::lock_guard locker{persist_snapshot_mutex_};
        RETURN_IF_INVALID_STATUS(persist_->append_snapshot_chunk(param))
        if (!param.done) return Status::OK();

        snap = std::make_shared<Snapshot>();
        snap->apply_index = param.snapshot_index;
        snap->apply_term = param.snapshot_term;

        RETURN_IF_INVALID_STATUS(persist_->finish_snapshot_receive(snap));
        {
            std::lock_guard lock(state_machine_mutex_);
            RETURN_IF_INVALID_STATUS(state_machine_->restore(
                snap, [this](const KvVisitor& visitor) -> Status {
                    return persist_->for_each_snapshot_kv(visitor);
                }))
            raft_node_->install_snapshot(param.snapshot_index,
                                         param.snapshot_term, param.term);
            refresh_recovering_state();
        }
    }

    return Status::OK();
}

void Replica::flush_messages() {
    ADVISKV_METRICS_TIMER("storage_raft_flush_messages");
    ADVISKV_METRICS_COUNTER("storage_raft_flush_messages_batch");

    auto messages = raft_node_->extract_messages();
    ADVISKV_METRICS_COUNTER("storage_raft_flush_messages_message",
                            static_cast<int64_t>(messages.size()));
    for (auto& msg : messages) {
        switch (msg.type) {
            case RaftMessageType::REQUEST_VOTE: {
                ADVISKV_METRICS_TIMER(
                    "storage_raft_flush_messages_request_vote");

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
                    IGNORE_RESULT(raft_node_->handle_append_response(
                        msg.target.replica_id, result));
                } else {
                    LOG_WARN("storage raft append entries failed, status:{}",
                             status.to_string());
                }
                break;
            }
            case RaftMessageType::INSTALL_SNAPSHOT: {
                ADVISKV_METRICS_TIMER(
                    "storage_raft_flush_messages_install_snapshot");

                InstallSnapshotResult result;
                Status status = raft_sender_.send_install_snapshot(
                    msg.target, msg.snapshot_param, *persist_, result);
                if (status.ok()) {
                    raft_node_->handle_install_snapshot_response(
                        msg.target.replica_id, result);
                } else {
                    LOG_WARN(
                        "storage raft send install snapshot failed, status:{}",
                        status.to_string());
                }
                break;
            }
        }
    }
}

void Replica::apply_committed_entries() {
    ADVISKV_METRICS_TIMER("storage_replica_apply_committed_entries");
    ADVISKV_METRICS_COUNTER("storage_replica_apply_committed_entries_request");

    auto entries = raft_node_->extract_committed_entries();
    ADVISKV_METRICS_COUNTER("storage_replica_apply_entry",
                            static_cast<int64_t>(entries.size()));
    for (const LogEntry& entry : entries) {
        // Status status = apply_log_entry(entry);
        Status status = state_machine_->apply(entry);
        if (status.fail()) {
            ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_failure");
            LOG_WARN("apply_log_entry failed, index={}, msg={}", entry.index,
                     status.msg());
            return;
        }
        ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_success");
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
    RETURN_IF_INVALID_PARAM(param)

    OperGuard guard;
    RETURN_IF_INVALID_STATUS(acquire_operation(guard))

    if (!state_machine_) {
        LOG_WARN(
            "state_machine is nullptr, replica: table_id = {}, shard_index = "
            "{}",
            shard_id_.table_id, shard_id_.shard_index);
        return Status{StatusCode::ERROR, "engine is nullptr"};
    }

    if (is_recovering()) {
        return Status::IS_RECOVERING("replica is recovering");
    }
    // TODO: ReadIndex 保证线性一致性 以后待定吧

    LogIndex read_index;
    RETURN_IF_INVALID_STATUS(check_self_leader_and_get_read_index(read_index))

    std::lock_guard lock(state_machine_mutex_);
    apply_committed_entries();
    if (state_machine_->apply_index() < read_index) {
        return Status::NOT_YET_COMMIT("state machine not yet apply");
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

    OperGuard guard;
    RETURN_IF_INVALID_STATUS(acquire_operation(guard))

    if (is_recovering()) {
        return Status::IS_RECOVERING("replica is recovering");
    }

    LogIndex new_commit_idx = 0;
    Status status;
    {
        ADVISKV_METRICS_TIMER("storage_replica_delete_propose");
        auto propose_result =
            raft_node_->propose(WriteOpType::DEL, param.key, "");
        status = propose_result.first;
        new_commit_idx = propose_result.second;
    }
    RETURN_IF_INVALID_STATUS(status)

    {
        ADVISKV_METRICS_TIMER("storage_replica_delete_flush_messages");
        flush_messages();
    }

    // delete 与 put 的完成语义保持一致：只有当当前请求对应的日志在本次返回前
    // 已经达到 committed 条件时，才返回 OK。
    //
    // 如果这轮 flush 之后 commit_index 仍然小于本次 propose 的目标 index，
    // 说明当前 leader 已经接受了这条删除日志，但还没有办法在这个返回点确认它
    // 已经被多数派提交。此时它后续可能提交成功，也可能在 leader 变化后失效，
    // 因此对客户端而言属于“结果未决”，而不是普通失败。
    {
        ADVISKV_METRICS_TIMER("storage_replica_delete_apply_committed");
        std::lock_guard lock(state_machine_mutex_);
        apply_committed_entries();
    }

    if (!raft_node_->is_leader()) {
        return Status{StatusCode::NOT_LEADER,
                      "leader changed during delete propose"};
    }

    if (raft_node_->commit_index() < new_commit_idx) {
        return Status{
            StatusCode::NOT_YET_COMMIT,
            "delete accepted by leader but commit is not yet confirmed"};
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
    OperGuard guard;
    RETURN_IF_INVALID_STATUS(acquire_operation(guard))

    raft_node_->handle_request_vote(param, result);
    return Status::OK();
}

Status Replica::handle_append_entries(const AppendEntriesParam& param,
                                      AppendEntriesResult& result) {
    OperGuard guard;
    RETURN_IF_INVALID_STATUS(acquire_operation(guard))

    // 这里是作为follower那边的handle，会更新commit_idx
    raft_node_->handle_append_entries(param, result);

    // 收到 AppendEntries 后，可能有新的 committed entries 需要 apply
    {
        std::lock_guard lock(state_machine_mutex_);
        apply_committed_entries();
    }

    return Status::OK();
}

void Replica::try_take_snapshot() {
    // LogIndex last_index = raft_node_->last_log_index();
    // LogIndex apply_index = state_machine_->apply_index();
    // //我勒个雷，这里写成狗屎了啊，还一直没看出来，一直到e2e测试才发现

    LogIndex last_apply_index = raft_node_->last_applied(),
             snapshot_index = raft_node_->snapshot_index();
    if (last_apply_index - snapshot_index < SNAPSHOT_LIMIT) return;

    std::lock_guard lock(state_machine_mutex_);
    Status status = persist_->do_snapshot(*state_machine_);

    // 现在这里的情况是，persist的执行快照有可能会失败，但是失败是有可能是因为truncate_wal里面的read_wal_from_disk里走到了：
    //        if (has_prev_index && entry.index != prev_index + 1) {
    // result.error = true;
    // result.error_msg =
    //     fmt::format("wal index is not continuous, prev={}, current={}",
    //                 prev_index, entry.index);
    // break;
    // }
    // 走到以上这块内容，但是我们没有办法回补，从而有bug
    //

    if (status.ok()) {
        // 持久化成功了，这边得截断wal日志了。
        raft_node_->truncate_log(state_machine_->apply_index());
    } else {
        LOG_WARN("replica:try_take_snapshot: status:{}", status.to_string());
    }
}

void Replica::on_tick() {
    OperGuard guard;
    if (acquire_operation(guard).fail()) return;

    raft_node_->tick();
    flush_messages();

    // 如果put最开始的时候没有推进commit_idx（例如其他的followers都太慢了或者有问题了），
    // 后来这边跑心跳的时候才有回应，这个时候就应该apply一下commmit_entry
    // 所以这个是专门为了raft_node是leader去发送心跳的时候准备的。

    {
        std::lock_guard lock(state_machine_mutex_);
        apply_committed_entries();
    }

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

// 新 leader 刚当选时，虽然它是合法 leader，但它的 commit_index 可能还没包含旧
// term 中已经成功提交的日志。Raft 通过让新 leader 提交一条当前 term 的 no-op
// entry，来安全推进 commit_index；一旦当前 term 的 entry 被提交，它前面的旧
// term 日志也会被一起提交。ReadIndex 读之前检查“当前 term 是否已有 committed
// entry”，就是为了确保 leader 的 commit_index
// 已经处在一个安全位置。否则即使心跳拿到了多数派确认，也可能因为状态机还没
// apply 到那些旧的已提交写，导致读到旧值。
Status Replica::check_self_leader_and_get_read_index(LogIndex& read_index) {
    if (!raft_node_->is_leader()) return Status::NOT_LEADER();

    // 我们需要检测一下自己是不是leader，并且需要发送给followers自己的心跳，主动发送一次
    std::vector<RaftMessage> messages;
    Term read_term;
    RETURN_IF_INVALID_STATUS(raft_node_->build_append_entries_for_read(
        messages, read_index, read_term))

    int success_cnt = 1;
    int limit = to<int>(messages.size() + 1) / 2 + 1;
    // 达到limit就可以了 // 这里就是要message_size
    // +1，review代码的时候差点绕进去了，这个是msg，得再加上自己

    for (const RaftMessage& msg : messages) {
        // if (msg.type != RaftMessageType::APPEND_ENTRIES) {
        //     continue;
        // }

        if (msg.type == RaftMessageType::APPEND_ENTRIES) {
            AppendEntriesResult res;
            Status status = raft_sender_.send_append_entries(
                msg.target, msg.append_param, res);
            if (status.fail()) continue;

            // 这个handle如果失败了，就说明自己不是leader
            RETURN_IF_INVALID_STATUS(
                raft_node_->handle_append_response(msg.target.replica_id, res));
            if (res.term == read_term)
                success_cnt++;  // 这里不用写关于判断res.success，
            // 因为我们只是需要确认leader这个身份，success可能是fail，因为prev没对齐
        } else if (msg.type == RaftMessageType::INSTALL_SNAPSHOT) {
            InstallSnapshotResult res;
            Status status = raft_sender_.send_install_snapshot(
                msg.target, msg.snapshot_param, *persist_, res);
            if (status.fail()) continue;
            raft_node_->handle_install_snapshot_response(msg.target.replica_id,
                                                         res);
            if (res.term == read_term) success_cnt++;
        } else {
            LOG_WARN("replica:{} check self leader, but have request vote msg!",
                     replica_id_.to_string());
        }
    }

    if (success_cnt >= limit) {
        return Status::OK();
    }
    return Status::NOT_LEADER("failed to confirm leader with quorum");
}

Status Replica::get_replica_state_for_test(ReplicaStateForTest& result) const {
    OperGuard guard;
    RETURN_IF_INVALID_STATUS(acquire_operation(guard))

    result.current_term = current_term();
    result.commit_index = raft_node_->commit_index();
    result.last_applied = raft_node_->last_applied();
    result.snapshot_index = raft_node_->snapshot_index();
    result.snapshot_term = raft_node_->snapshot_term();
    return Status::OK();
}

void Replica::shutdown() {
    stopping_.store(true);
    std::unique_lock lock(life_mutex_);
}

Status Replica::ensure_running() const {
    if (stopping_.load()) {
        return Status::ERROR("replica is not running");
    }
    return Status::OK();
}

Status Replica::acquire_operation(OperGuard& guard) const {
    RETURN_IF_INVALID_STATUS(ensure_running())

    std::shared_lock lock(life_mutex_);
    RETURN_IF_INVALID_STATUS(ensure_running())

    guard = OperGuard(std::move(lock));
    return Status::OK();
}
}  // namespace adviskv::storage