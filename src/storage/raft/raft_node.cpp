#include "storage/raft/raft_node.h"

#include <fmt/format.h>

#include <algorithm>
#include <cassert>
#include <functional>
#include <mutex>
#include <utility>
#include <vector>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/model/replica_role.h"
#include "common/status.h"
#include "common/tick_trigger.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/raft_apply.h"
#include "storage/raft/raft_log.h"
#include "storage/raft/raft_membership.h"
#include "storage/raft/raft_replication.h"
namespace adviskv::storage {

static constexpr int32_t HEARTBEAT_INTERVAL = 3;

static const TickTrigger::TickLimitFunc ELECTION_TIMEOUT_FUNC =
    std::bind(func::get_random_int32, 15, 30);

RaftNode::RaftNode(const ReplicaID& self_id,
                   const std::vector<PeerMember>& members)
    : self_id_(self_id),
      election_(self_id_),
      raft_log_(),
      raft_apply_(raft_log_),
      membership_(self_id_, members),
      replication_(self_id_, membership_, raft_log_, raft_apply_),
      election_tick_trigger_(ELECTION_TIMEOUT_FUNC),
      heartbeat_tick_trigger_(HEARTBEAT_INTERVAL) {}

LogIndex RaftNode::last_log_index_unlocked() const {
    return raft_log_.last_log_index();
}

Term RaftNode::last_log_term_unlocked() const {
    return raft_log_.last_log_term();
}

LogIndex RaftNode::snapshot_index_unlocked() const {
    return raft_log_.snapshot_index();
}

Term RaftNode::snapshot_term_unlocked() const {
    return raft_log_.snapshot_term();
}

LogIndex RaftNode::last_log_index() const {
    std::lock_guard lock(mutex_);
    return last_log_index_unlocked();
}

Term RaftNode::last_log_term() const {
    std::lock_guard lock(mutex_);
    return last_log_term_unlocked();
}

int RaftNode::quorum_size_unlocked() const {
    return membership_.quorum_size_unlocked();
}

int RaftNode::quorum_size() const {
    std::lock_guard lock(mutex_);
    return quorum_size_unlocked();
}

bool RaftNode::has_quorum_unlocked(int ack_count) const {
    return membership_.has_quorum_unlocked(ack_count);
}

ReplicaRole RaftNode::role() const {
    std::lock_guard lock(mutex_);
    return election_.role();
}

Term RaftNode::current_term() const {
    std::lock_guard lock(mutex_);
    return election_.current_term();
}

LogIndex RaftNode::commit_index() const {
    std::lock_guard lock(mutex_);
    return raft_apply_.commit_index();
}

LogIndex RaftNode::last_applied() const {
    std::lock_guard lock(mutex_);
    return raft_apply_.last_applied();
}

LogIndex RaftNode::snapshot_index() const {
    std::lock_guard lock(mutex_);
    return snapshot_index_unlocked();
}

Term RaftNode::snapshot_term() const {
    std::lock_guard lock(mutex_);
    return snapshot_term_unlocked();
}

bool RaftNode::is_leader() const {
    std::lock_guard lock(mutex_);
    return election_.is_leader();
}

RaftMeta RaftNode::get_rafe_meta() const { return election_.hard_state(); }

bool RaftNode::later_than_other(Term other_term, LogIndex other_index) const {
    if (last_log_term_unlocked() != other_term) {
        return last_log_term_unlocked() > other_term;
    }
    return last_log_index_unlocked() > other_index;
}

void RaftNode::tick(RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    if (ensure_ready_unlocked().fail()) return;

    if (election_.is_leader()) {
        heartbeat_tick_trigger_.tick(
            [&] { broadcast_append_entries(effects); });
    } else {
        election_tick_trigger_.tick([&] { become_candidate(effects); });
    }
}

void RaftNode::become_candidate(RaftEffects& effects) {
    if (ensure_ready_unlocked().fail()) return;
    LOG_INFO("replica:{} start become cadidate", self_id_.to_string());
    election_.become_candidate();
    record_hard_state_unlocked(effects);

    heartbeat_tick_trigger_.stop();
    election_tick_trigger_.reset();

    // 如果只有一个节点的话，就直接当选
    if (has_quorum_unlocked(election_.granted_vote_count())) {
        become_leader(effects);
        return;
    }

    // 给所有 peer 发 RequestVote
    for (const PeerMember& member : membership_.get_members()) {
        if (member.replica_id == self_id_) continue;
        send_request_vote_to(member, effects);
    }
}

void RaftNode::send_request_vote_to(const PeerMember& member,
                                    RaftEffects& effects) {
    RaftMessage msg;
    msg.type = RaftMessageType::REQUEST_VOTE;
    msg.target = member;
    msg.vote_param.from_replica_id = self_id_;
    msg.vote_param.to_replica_id = member.replica_id;
    msg.vote_param.term = election_.current_term();
    msg.vote_param.last_log_index = last_log_index_unlocked();
    msg.vote_param.last_log_term = last_log_term_unlocked();
    effects.messages.push_back(std::move(msg));
}

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
LogIndex RaftNode::append_new_entry_unlocked(WriteOpType op, const Key& key,
                                             const Value& value,
                                             RaftEffects& effects) {
    LogIndex new_index =
        raft_log_.append_new_entry(election_.current_term(), op, key, value);
    const LogEntry* entry = raft_log_.entry_at(new_index);

    if (entry != nullptr) {
        effects.entries_to_append.push_back(*entry);
    }
    return new_index;
}

std::pair<Status, LogIndex> RaftNode::propose(WriteOpType op, const Key& key,
                                              const Value& value,
                                              RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};

    Status ready_status = ensure_ready_unlocked();
    if (ready_status.fail()) return {ready_status, -1};

    if (!election_.is_leader()) {
        return {Status{StatusCode::NOT_LEADER, "not leader"}, -1};
    }

    LogIndex new_commit_idx =
        append_new_entry_unlocked(op, key, value, effects);

    broadcast_append_entries(effects);

    if (election_.is_leader() and has_quorum_unlocked(1)) {
        try_update_commit_index();
    }

    return {Status::OK(), new_commit_idx};
}

/*
注意，在广播的时候，我们会把每一个follower的 从他们next_idx开始到最新的消息
都发送给他们。 具体是会放到pending_message队列里面。
所以每一次广播之后，我们的pending_message就会更新。
*/
void RaftNode::broadcast_append_entries(RaftEffects& effects) {
    if (ensure_ready_unlocked().fail()) return;
    if (!election_.is_leader()) return;

    replication_.broadcast_append_entries(election_.current_term(), effects);
}

void RaftNode::handle_request_vote(const RequestVoteParam& param,
                                   RequestVoteResult& result,
                                   RaftEffects& effects) {
    LOG_DEBUG("replica:{} get request vote from {}", self_id_.to_string(),
              param.from_replica_id.to_string());
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    result.term = election_.current_term();
    result.vote_granted = false;

    // 这边对于recovering状态，我们是会去更新它的current
    // term的，以防止在恢复正常之后可能会接收到一些比较旧的term的节点的消息，并且做出回复
    // if (ensure_not_faulted_unlocked().fail()) {
    //     return;
    // }

    if (param.term < election_.current_term()) {
        return;
    }

    if (param.term > election_.current_term()) {
        // 这里这个函数会把自己的vote_for_给清空掉，会不会导致投了很多次票
        // 但是应该没有问题，毕竟是这种情况应该是发生在多次投票里面，他们的term不一样
        // 既然不一样的话，肯定是最终term最大的那一个去当的leader了，别的会自动变成follower
        become_follower(param.term, effects);
        result.term = election_.current_term();
    }

    if (ensure_ready_unlocked().fail()) {
        return;
    }

    // 如果比人家的新 （是一定会新，不包括相等）
    if (later_than_other(param.last_log_term, param.last_log_index)) {
        return;
    }

    // 没有投过票，或者投过了，还是这个人
    // 如果投过了这个同一个人的话，其实这里vote_granted是true还是false都无所谓吧，反正对方已经拿到票了。
    // 保障一致性，还是设置成true把

    if (election_.grant_vote_to(param.from_replica_id)) {
        LOG_DEBUG("replica:{} vote to {}, current_term:{}",
                  self_id_.to_string(), param.from_replica_id.to_string(),
                  election_.current_term());
        record_hard_state_unlocked(effects);
        result.vote_granted = true;
        election_tick_trigger_.clear();
    }
}

// 这个是leadr发过来，raftnode作为follower/cacdidate做的handle
void RaftNode::handle_append_entries(const AppendEntriesParam& param,
                                     AppendEntriesResult& result,
                                     RaftEffects& effects) {
    ADVISKV_METRICS_TIMER("storage_raft_handle_append_entries");
    ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_request");
    effects = RaftEffects{};
    if (param.entries.empty()) {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_heartbeat");
    } else {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_log");
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_entry",
                                static_cast<int64_t>(param.entries.size()));
    }

    std::lock_guard lock(mutex_);
    result = AppendEntriesResult{};
    result.success = false;
    result.term = election_.current_term();
    result.last_log_index = last_log_index_unlocked();

    if (param.term < election_.current_term()) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_handle_append_entries_stale_term");
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

    if (param.prev_log_index > 0) {
        if (param.prev_log_index < snapshot_index_unlocked()) {
            // 在这个情况下，目前的处理是:
            // leader那边会一直prev_log_index--，然后直到达不到leader的
            // snapshot_index，然后发送快照让follower安装leader的快照。
            LOG_INFO(
                "raft node: follower receive append entries: "
                "param.prev_log_index:{} < snapshot_index_:{}",
                param.prev_log_index, snapshot_index_unlocked());
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
    } else {
        RaftLog::AppendEntriesResult append_result;
        Status append_status =
            raft_log_.append_entries_from_leader(param.entries, append_result);
        if (append_status.fail()) {
            LOG_WARN("storage raft handle append entries failed, msg={}",
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
    }

    // 不管是否有entry，也就是不管是日志追加还是心跳，都会需要更新commit_idx
    raft_apply_.advance_commit_index_from_leader(param.leader_commit);
    finish_recovering_unlocked();

    result.success = true;
    result.term = election_.current_term();
    ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_success");
}

void RaftNode::handle_vote_response(const ReplicaID& from,
                                    const RequestVoteResult& result,
                                    RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    if (ensure_ready_unlocked().fail()) return;

    // 已经不是 CANDIDATE 了，就直接忽略之前的发起内容
    if (!election_.is_candidate()) return;

    LOG_DEBUG("candidate replica:{} get vote response from replica:{}",
              self_id_.to_string(), from.to_string());

    if (result.term > election_.current_term()) {
        become_follower(result.term, effects);
        return;
    }

    if (result.term < election_.current_term()) {
        return;
    }

    if (!result.vote_granted) return;

    if (!election_.record_vote_granted_from(from)) {
        return;
    }
    LOG_DEBUG(
        "candidate replica:{} get vote response from replica:{}, self vote "
        "count++ to {}",
        self_id_.to_string(), from.to_string(), election_.granted_vote_count());

    if (has_quorum_unlocked(election_.granted_vote_count())) {
        become_leader(effects);
    }
}

Status RaftNode::handle_append_response(const ReplicaID& from,
                                        const AppendEntriesParam& sent_param,
                                        const AppendEntriesResult& result,
                                        RaftEffects& effects) {
    ADVISKV_METRICS_TIMER("storage_raft_handle_append_response");
    ADVISKV_METRICS_COUNTER("storage_raft_handle_append_response_request");

    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    RETURN_IF_INVALID_STATUS(ensure_ready_unlocked())

    if (result.term > election_.current_term()) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_handle_append_response_higher_term");
        become_follower(result.term, effects);
        return Status::NOT_LEADER("higher term");
    }

    if (sent_param.term != election_.current_term()) {
        LOG_DEBUG(
            "leader replica:{} handle append response: result.term:{} != "
            "current_term:{}",
            self_id_.to_string(), result.term, election_.current_term());
        return Status::OK();
    }

    if (!election_.is_leader()) {
        ADVISKV_METRICS_COUNTER(
            "storage_raft_handle_append_response_not_leader");
        return Status::NOT_LEADER();
    }

    if (result.success) {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_response_success");
        LOG_DEBUG("leader replica:{} append enrties to replica:{} success.",
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
                "leader replica:{} sent param.prev_log_index:{} + 1 != "
                "next_index_[from]:{}",
                self_id_.to_string(), sent_param.prev_log_index,
                replication_.next_index(from));
            return Status::OK();
        }

        replication_.handle_append_failed(from, result.last_log_index);

        LOG_DEBUG(
            "leader replica:{} append enrties to replica:{} failed. set "
            "next_index:{}",
            self_id_.to_string(), from.to_string(),
            replication_.next_index(from));
    }
    return Status::OK();
}

// 提取出来已经commit，但是还没有apply的entry
std::vector<LogEntry> RaftNode::extract_committed_entries() {
    std::lock_guard lock(mutex_);
    return raft_apply_.extract_committed_entries();
}

void RaftNode::advance_last_applied(LogIndex applied) {
    std::lock_guard lock(mutex_);
    raft_apply_.advance_last_applied(applied);
}

void RaftNode::become_follower(Term later_term, RaftEffects& effects) {
    assert(later_term >= election_.current_term());

    LOG_INFO("replica:{} become follower, new term:{}", self_id_.to_string(),
             later_term);

    heartbeat_tick_trigger_.stop();
    election_tick_trigger_.reset();

    if (election_.become_follower(later_term)) {
        record_hard_state_unlocked(effects);
    }
}

void RaftNode::become_leader(RaftEffects& effects) {
    if (ensure_ready_unlocked().fail()) return;

    LOG_INFO("replica:{} become leader", self_id_.to_string());
    election_.become_leader();

    election_tick_trigger_.stop();
    heartbeat_tick_trigger_.reset();

    // TODO 为什么当上了leader之后需要把这些全都初始化呢？ 保留原来的值不行吗？
    replication_.reset_for_leader();

    append_new_entry_unlocked(
        WriteOpType::NONE, "for debug: this is a no-op entry key",
        "for debug: this is a no-op entry value", effects);
    // 立即广播（含 no-op），相当于心跳 + 日志复制合一
    broadcast_append_entries(effects);

    if (has_quorum_unlocked(1)) {
        try_update_commit_index();
    }
}

// raftnode 作为leader，需要更新自己的commit_idx
// 当收到了follower们关于日志复制的回应时，就调用一下这个函数，去更新自己的commit_idx
void RaftNode::try_update_commit_index() {
    if (ensure_ready_unlocked().fail()) return;

    RaftReplication::CommitAdvanceResult result =
        replication_.try_advance_commit_index(election_.current_term());
    if (result.advanced) {
        ADVISKV_METRICS_COUNTER("storage_raft_commit_index_advance");
        ADVISKV_METRICS_COUNTER("storage_raft_committed_entry",
                                static_cast<int64_t>(result.new_commit_index -
                                                     result.old_commit_index));
    }
}

Status RaftNode::ensure_ready_unlocked() const {
    if (state_ == RaftNodeState::RECOVERING) {
        return Status::IS_RECOVERING("raft node is recovering");
    }
    if (state_ != RaftNodeState::READY) {
        return Status::NOT_INIT("raft node is not ready");
    }
    return Status::OK();
}

void RaftNode::record_hard_state_unlocked(RaftEffects& effects) const {
    effects.hard_state = get_rafe_meta();
}

// void try_take_snapshot();
// void RaftNode::()

Status RaftNode::truncate_log(LogIndex new_snapshot_index) {
    std::lock_guard lock(mutex_);
    if (new_snapshot_index > raft_apply_.last_applied()) {
        LOG_WARN(
            "new_snap_index > last_applied, new_snapshhot_index:{}, "
            "last_applied:{}",
            new_snapshot_index, raft_apply_.last_applied());
        return Status::ERROR("new_snap_index > last_applied");
    }
    return raft_log_.truncate(new_snapshot_index);
}

RaftLog::InstallSnapshotResult RaftNode::install_snapshot_unlocked(
    LogIndex new_snapshot_index, Term new_snapshot_term) {
    RaftLog::InstallSnapshotResult result =
        raft_log_.install_snapshot(new_snapshot_index, new_snapshot_term);
    raft_apply_.install_snapshot(new_snapshot_index);
    finish_recovering_unlocked();
    return result;
}

void RaftNode::commit_install_snapshot_unlocked(const SnapshotInstallPlan& plan,
                                                RaftEffects& effects) {
    RaftLog::InstallSnapshotResult result =
        install_snapshot_unlocked(plan.snapshot_index, plan.snapshot_term);
    if (result.retained_entries != plan.retained_entries) {
        LOG_WARN(
            "snapshot install retained entries changed between plan and "
            "commit, replica:{}, snapshot_index:{}",
            self_id_.to_string(), plan.snapshot_index);
    }
    effects.entries_to_rewrite = std::move(result.retained_entries);
}

void RaftNode::install_local_snapshot(LogIndex new_snapshot_index,
                                      Term new_snapshot_term) {
    std::lock_guard lock(mutex_);
    install_snapshot_unlocked(new_snapshot_index, new_snapshot_term);
}

Status RaftNode::install_leader_snapshot(LogIndex new_snapshot_index,
                                         Term new_snapshot_term,
                                         Term leader_term,
                                         RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};

    RETURN_IF_INVALID_STATUS(prepare_install_snapshot_unlocked(
        leader_term, new_snapshot_index, new_snapshot_term, effects))

    SnapshotInstallPlan plan;
    plan.snapshot_index = new_snapshot_index;
    plan.snapshot_term = new_snapshot_term;
    plan.retained_entries = raft_log_.retained_entries_after_snapshot(
        new_snapshot_index, new_snapshot_term);
    commit_install_snapshot_unlocked(plan, effects);
    return Status::OK();
}

Status RaftNode::build_install_snapshot_plan(Term leader_term,
                                             LogIndex new_snapshot_index,
                                             Term new_snapshot_term,
                                             SnapshotInstallPlan& plan,
                                             RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    plan = SnapshotInstallPlan{};

    RETURN_IF_INVALID_STATUS(prepare_install_snapshot_unlocked(
        leader_term, new_snapshot_index, new_snapshot_term, effects))

    plan.snapshot_index = new_snapshot_index;
    plan.snapshot_term = new_snapshot_term;
    plan.retained_entries = raft_log_.retained_entries_after_snapshot(
        new_snapshot_index, new_snapshot_term);
    return Status::OK();
}

void RaftNode::commit_install_snapshot(const SnapshotInstallPlan& plan,
                                       RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    commit_install_snapshot_unlocked(plan, effects);
}

void RaftNode::handle_install_snapshot_response(
    const ReplicaID& from, const InstallSnapshotParam& sent_param,
    const InstallSnapshotResult& result, RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    if (ensure_ready_unlocked().fail()) return;

    if (result.term > election_.current_term()) {
        become_follower(result.term, effects);
        return;
    }

    if (!election_.is_leader()) return;

    if (sent_param.term != election_.current_term()) {
        LOG_WARN(
            "raft replica_id:{} handle snapshot response replica_id:{}, "
            "sent_param.term:{} != election_.current_term:{}",
            self_id_.to_string(), from.to_string(), sent_param.term,
            election_.current_term());
        return;
    }

    replication_.clear_snapshot_inflight(from, sent_param.snapshot_index);

    LogIndex snapshot_watermark = result.snapshot_watermark;
    if (result.status.ok()) {
        snapshot_watermark =
            std::max(snapshot_watermark, sent_param.snapshot_index);
    }
    if (snapshot_watermark >= sent_param.snapshot_index) {
        replication_.update_snapshot_watermark(from, snapshot_watermark);
        LOG_DEBUG(
            "raft node replica_id:{} follower replica_id:{} snapshot watermark "
            "updated, sent_snapshot_index:{}, snapshot_watermark:{}",
            self_id_.to_string(), from.to_string(), sent_param.snapshot_index,
            snapshot_watermark);
        return;
    }

    if (result.status.fail()) {
        LOG_WARN(
            "raft replica_id:{} handle snapshot response replica_id:{}, result "
            "status.fail, status:{}",
            self_id_.to_string(), from.to_string(), result.status.to_string());
        return;
    }
}

void RaftNode::handle_install_snapshot_send_failed(
    const ReplicaID& from, const InstallSnapshotParam& sent_param,
    const Status& status) {
    std::lock_guard lock(mutex_);
    if (ensure_ready_unlocked().fail()) return;

    if (!election_.is_leader()) return;

    if (sent_param.term != election_.current_term()) {
        LOG_WARN(
            "raft replica_id:{} handle snapshot send failed replica_id:{}, "
            "sent_param.term:{} != election_.current_term:{}, status:{}",
            self_id_.to_string(), from.to_string(), sent_param.term,
            election_.current_term(), status.to_string());
        return;
    }

    replication_.clear_snapshot_inflight(from, sent_param.snapshot_index);
    LOG_WARN(
        "raft replica_id:{} clear snapshot inflight for replica_id:{} after "
        "send failed, snapshot_index:{}, status:{}",
        self_id_.to_string(), from.to_string(), sent_param.snapshot_index,
        status.to_string());
}

Status RaftNode::prepare_install_snapshot_unlocked(Term leader_term,
                                                   LogIndex new_snapshot_index,
                                                   Term new_snapshot_term,
                                                   RaftEffects& effects) {
    if (leader_term < election_.current_term()) {  // 发送过来的leader的term低
        return Status::ERROR(
            fmt::format("leade term:{} is oldear than current term:{}",
                        leader_term, election_.current_term()));
    }

    if (leader_term > election_.current_term() || !election_.is_follower()) {
        become_follower(leader_term, effects);
    }
    
    election_tick_trigger_.clear();

    if (new_snapshot_index <= raft_apply_.commit_index()) {
        return Status::ALREADY_EXIST(fmt::format(
            "prepare_install_snapshot_unlocked: leader snapshot_index:{} <= "
            "commit_index:{}, snapshot_index:{}, last_applied:{}",
            new_snapshot_index, raft_apply_.commit_index(),
            snapshot_index_unlocked(), raft_apply_.last_applied()));
    }

    if (state_ == RaftNodeState::READY &&
        new_snapshot_index <= raft_log_.last_log_index() &&
        raft_log_.term_at(new_snapshot_index) == new_snapshot_term) {
        return Status::ALREADY_EXIST(
            fmt::format("follower already has the log, leader snapshot can not "
                        "bring something new, snapshot_index:{}, "
                        "snapshot_term:{}",
                        new_snapshot_index, new_snapshot_term));
    }

    return Status::OK();
}

Status RaftNode::prepare_install_snapshot(Term leader_term,
                                          LogIndex new_snapshot_index,
                                          Term new_snapshot_term,
                                          RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    return prepare_install_snapshot_unlocked(leader_term, new_snapshot_index,
                                             new_snapshot_term, effects);
}

void RaftNode::update_raft_meta(const RaftMeta& meta) {
    std::lock_guard lock(mutex_);
    election_.update_meta(meta);
}

void RaftNode::update_log_entries(const std::vector<LogEntry>& entries) {
    std::lock_guard lock(mutex_);
    raft_log_.update_entries(entries);
}

void RaftNode::enter_recovering() {
    std::lock_guard lock(mutex_);
    state_ = RaftNodeState::RECOVERING;
    election_.become_follower(election_.current_term());
    election_tick_trigger_.stop();
    heartbeat_tick_trigger_.stop();
}

void RaftNode::finish_recovering_unlocked() {
    if (state_ != RaftNodeState::RECOVERING) return;

    state_ = RaftNodeState::READY;
    election_tick_trigger_.reset();
}

// 判判断当前这个term，这个leader是否已经提交过entry了
// 原因的话是因为如果他没有提交过的话，那这个commit
// index就还没有这个及时的更新到，那么就有可能会导致客户端最终那边会读到旧的数据。
bool RaftNode::has_committed_current_term_entry_unlocked() const {
    return raft_apply_.has_committed_current_term_entry(
        election_.current_term());
}

// 其实由于我们get操作会专门发送一次，所以导致同样的append_entires
// 可能会多次发送
// ，但是无伤大雅，handle_append_entries里面会判断出来new_entries是空的
Status RaftNode::build_append_entries_for_read(RaftEffects& effects,
                                               LogIndex& read_index,
                                               Term& read_term) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    RETURN_IF_INVALID_STATUS(ensure_ready_unlocked())

    if (!election_.is_leader()) return Status::NOT_LEADER("not leader");
    if (!has_committed_current_term_entry_unlocked()) {
        return Status::NOT_YET_COMMIT("current term entry is not committed");
    }

    read_term = election_.current_term();
    read_index = raft_apply_.commit_index();

    replication_.broadcast_append_entries(election_.current_term(), effects);
    return Status::OK();
}

}  // namespace adviskv::storage