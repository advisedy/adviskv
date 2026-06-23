#include <utility>

#include "common/log.h"
#include "common/metrics/metrics.h"
#include "storage/raft/core/raft_core.h"

namespace adviskv::storage {

/*

写链路:
在执行写操作之后就加入自己的log_entries，然后广播所有follower，

（flush_message）
然后replica会执行自己产生的message，去实际的发送给别的replica
然后replica接收到了返回来的消息，并且把结果返回给raft_node。
结果返回给raft_node后，raft_node会处理对应的内容:
例如如果是日志复制的回复的话，就可以更新对应的match_idx。

然后replica侧调用apply_committed_entries，去apply到状态机上。同时也会更新raft_node的last_apply_

*/
LogIndex RaftCore::append_new_entry(const ProposeParam& param,
                                             RaftEffects& effects) {
    LogIndex new_index =
        raft_log_.append_new_entry(election_.current_term(), param);
    const LogEntry* entry = raft_log_.entry_at(new_index);

    if (entry != nullptr) {
        effects.entries_to_append.push_back(*entry);
    }
    return new_index;
}

Status RaftCore::validate_proposal(const ProposeParam& param) const {
    if (const auto* write = std::get_if<WriteProposal>(&param.payload)) {
        if (write->op != WriteOpType::PUT && write->op != WriteOpType::DEL) {
            return Status::INVALID_ARGUMENT("invalid write proposal op");
        }
        return Status::OK();
    }
    if (const auto* noop = std::get_if<NoopProposal>(&param.payload)) {
        UNUSED(noop);
        return Status::OK();
    }
    return Status::INVALID_ARGUMENT("unknown proposal type");
}

std::pair<Status, LogIndex> RaftCore::propose(const ProposeParam& param,
                                              RaftEffects& effects) {
    Status ready_status = ensure_ready();
    if (ready_status.fail()) return {ready_status, -1};

    if (!election_.is_leader()) {
        return {Status{StatusCode::NOT_LEADER, "not leader"}, -1};
    }

    Status validate_status = validate_proposal(param);
    if (validate_status.fail()) return {validate_status, -1};

    LogIndex new_commit_idx = append_new_entry(param, effects);
    broadcast_append_entries(effects);

    if (election_.is_leader() and membership_.has_quorum(1)) {
        try_update_commit_index();
    }

    return {Status::OK(), new_commit_idx};
}

/*
注意，在广播的时候，我们会把每一个follower的 从他们next_idx开始到最新的消息
都发送给他们。 具体是会放到pending_message队列里面。
所以每一次广播之后，我们的pending_message就会更新。
*/
void RaftCore::broadcast_append_entries(RaftEffects& effects) {
    if (ensure_ready_unlocked().fail()) return;
    if (!election_.is_leader()) return;

    replication_.broadcast_append_entries(election_.current_term(), effects);
}

// 这个是leadr发过来，raftnode作为follower/cacdidate做的handle
void RaftCore::handle_append_entries(const AppendEntriesParam& param,
                                     AppendEntriesResult& result,
                                     RaftEffects& effects) {
    result = AppendEntriesResult{};
    result.success = false;
    result.term = election_.current_term();
    result.last_log_index = raft_log_.last_log_index();

    if (param.term < election_.current_term()) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_handle_append_entries_stale_term");
        LOG_INFO(
            "[RaftCore Append] handle_append_entries, param.term:{} < "
            "election_.current_term:{}",
            param.term, election_.current_term());
        return;
    }

    if (param.term > election_.current_term() || !election_.is_follower()) {
        // 这里的判断条件是，只要role不是FOLLOWER，就会become，但是如果term和param的term是相等的
        // 然后prev_log_index不一样呢？ 这个时候不需要比较一下index吗？
        // 这里并不需要，毕竟append 不需要去干关于选举方面的事情，
        become_follower(param.term, effects);
    }

    result.term = election_.current_term();
    election_tick_trigger_.clear();
    LOG_DEBUG(
        "[RaftCore Append] handle_append_entries: clear election_tick_trigger, "
        "cur_cnt:{}, limit_cnt:{}",
        election_tick_trigger_.get_cur_cnt(),
        election_tick_trigger_.get_limit_cnt())

    if (param.prev_log_index > 0) {
        if (param.prev_log_index < raft_log_.snapshot_index()) {
            // 在这个情况下，目前的处理是:
            // leader那边会一直prev_log_index--，然后直到达不到leader的
            // snapshot_index，然后发送快照让follower安装leader的快照。
            LOG_INFO(
                "[RaftCore Append] handle_append_entries, replica_id:{} "
                "follower "
                "receive append entries: "
                "param.prev_log_index:{} < snapshot_index_:{}",
                self_id_.to_string(), param.prev_log_index,
                raft_log_.snapshot_index());
        } else if (raft_log_.term_at(param.prev_log_index) !=
                   param.prev_log_term) {
            ADVISKV_METRICS_COUNTER(
                "storage_raft_handle_append_entries_prev_mismatch");
            return;
        }
    }
    // if (param.prev_log_index < snapshot_index_) {
    //     //
    //     压测情况下leader那边高并发发送，follower来不及回复，prev_log_index没更新，follower这边接收到的消息过多导致自己跑了快照，就会触发这种情况
    //     ADVISKV_METRICS_COUNTER(
    //         "storage_raft_handle_append_entries_prev_behind_snapshot");
    //     LOG_WARN(
    //         "raft node: follower receive append entries: "
    //         "param.prev_log_index < snapshot_index_!!");
    //     return;
    // }

    if (param.entries.empty()) {
        // 这里是心跳
        LOG_DEBUG(
            "[RaftCore Append] handle_append_entries: this is heartbeat, from "
            "replica_id:{}, term:{}, prev_log_index:{}, prev_log_term:{}, "
            "leader_commit:{}",
            param.from_replica_id.to_string(), param.term, param.prev_log_index,
            param.prev_log_term, param.leader_commit)
    } else {
        RaftLog::AppendEntriesResult append_result;
        Status append_status =
            raft_log_.append_entries_from_leader(param.entries, append_result);
        if (append_status.fail()) {
            LOG_WARN(
                "[RaftCore Append] storage raft handle append entries failed, "
                "msg={}",
                     append_status.msg());
            return;
        }
        if (append_result.entries_to_rewrite.has_value()) {
            effects.entries_to_rewrite =
                std::move(append_result.entries_to_rewrite.value());
        } else {
            effects.entries_to_append =
                std::move(append_result.entries_to_append);
        }
        LOG_DEBUG(
            "[RaftCore Append] handle_append_entries: effects finish, "
            "effect:{}",
            effects.to_string())
    }

    // 不管是否有entry，也就是不管是日志追加还是心跳，都会需要更新commit_idx
    raft_apply_.advance_commit_index_from_leader(param.leader_commit);
    finish_recovering();

    result.success = true;
    result.term = election_.current_term();
    ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_success");
}

Status RaftCore::handle_append_response(const ReplicaID& from,
                                        const AppendEntriesParam& sent_param,
                                        const AppendEntriesResult& result,
                                        RaftEffects& effects) {
    RETURN_IF_INVALID_STATUS(ensure_ready())

    if (result.term > election_.current_term()) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_handle_append_response_higher_term");
        become_follower(result.term, effects);
        return Status::NOT_LEADER("higher term");
    }

    if (sent_param.term != election_.current_term()) {
        LOG_DEBUG(
            "[RaftCore Append] leader replica:{} handle append response: "
            "result.term:{} != "
            "current_term:{}",
            self_id_.to_string(), sent_param.term, election_.current_term());
        return Status::OK();
    }

    if (!election_.is_leader()) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_handle_append_response_not_leader");
        return Status::NOT_LEADER();
    }

    if (result.success) {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_response_success");
        LOG_DEBUG(
            "[RaftCore Append] leader replica:{} append enrties to replica:{} "
            "success.",
                  self_id_.to_string(), from.to_string());
        replication_.handle_append_ok(from, sent_param.prev_log_index,
                                      sent_param.entries.size());
        try_update_commit_index();
    } else {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_response_reject");
        // prev_log 对不上

        // 需要先确认一下关于response的时效性
        if (replication_.is_stale_append_response(from, sent_param)) {
            // 说明其实过期了，这个是旧的请求的回应，应该忽略才对
            LOG_DEBUG(
                "[RaftCore Append] leader replica:{} sent "
                "param.prev_log_index:{} + 1 != "
                "next_index_[from]:{}",
                self_id_.to_string(), sent_param.prev_log_index,
                replication_.next_index(from));
            return Status::OK();
        }

        replication_.handle_append_failed(from, result.last_log_index);

        LOG_DEBUG(
            "[RaftCore Append] leader replica:{} append enrties to replica:{} "
            "failed. set "
            "next_index:{}",
            self_id_.to_string(), from.to_string(),
            replication_.next_index(from));
    }
    return Status::OK();
}

void RaftCore::handle_append_send_failed(const ReplicaID& from,
                                         const AppendEntriesParam&,
                                         const Status& status) {
    if (ensure_ready().fail()) return;
    if (!election_.is_leader()) return;
    if (!membership_.contains(from)) return;

    ADVISKV_METRICS_COUNTER("storage_raft_append_entries_send_failed");
    LOG_WARN(
        "[RaftCore Append] leader replica:{} append entries send to "
        "replica:{} failed, status:{}",
        self_id_.to_string(), from.to_string(), status.to_string());
}

// raftnode 作为leader，需要更新自己的commit_idx
// 当收到了follower们关于日志复制的回应时，就调用一下这个函数，去更新自己的commit_idx
void RaftCore::try_update_commit_index() {
    if (ensure_ready().fail()) return;

    RaftReplication::CommitAdvanceResult result =
        replication_.try_advance_commit_index(election_.current_term());
    if (result.advanced) {
        ADVISKV_METRICS_COUNTER("storage_raft_commit_index_advance");
        ADVISKV_METRICS_COUNTER("storage_raft_committed_entry",
                                static_cast<int64_t>(result.new_commit_index -
                                                     result.old_commit_index));
    }
}

}  // namespace adviskv::storage