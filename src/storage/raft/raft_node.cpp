#include "storage/raft/raft_node.h"

#include <mutex>

#include "common/log.h"
#include "common/metrics/metrics.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::storage {

// ============================================================================
// 构造
// ============================================================================

RaftNode::RaftNode(const ReplicaID& self_id,
                   const std::vector<PeerMember>& members)
    : core_(self_id, members) {}

// ============================================================================
// tick 和外层写请求
// ============================================================================

void RaftNode::tick(RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    core_.tick(effects);
}

std::pair<Status, LogIndex> RaftNode::propose(const ProposeParam& param,
                                              RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    return core_.propose(param, effects);
}

Status RaftNode::propose_batch(
    const std::vector<ProposeParam>& params,
    std::vector<std::pair<Status, LogIndex>>& results, RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    results = core_.propose_batch(params, effects);
    return Status::OK();
}

// ============================================================================
// 处理别的 replica 发过来的 Raft RPC 请求
// ============================================================================

void RaftNode::handle_request_vote(const RequestVoteParam& param,
                                   RequestVoteResult& result,
                                   RaftEffects& effects) {
    LOG_DEBUG("replica:{} get request vote from {}",
              param.to_replica_id.to_string(),
              param.from_replica_id.to_string());
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    core_.handle_request_vote(param, result, effects);
}

void RaftNode::handle_append_entries(const AppendEntriesParam& param,
                                     AppendEntriesResult& result,
                                     RaftEffects& effects) {
    ADVISKV_METRICS_TIMER("storage_raft_handle_append_entries");
    ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_request");
    if (param.entries.empty()) {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_heartbeat");
    } else {
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_log");
        ADVISKV_METRICS_COUNTER("storage_raft_handle_append_entries_entry",
                                static_cast<int64_t>(param.entries.size()));
    }

    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    core_.handle_append_entries(param, result, effects);
}

// ============================================================================
// 处理自己发出去的 Raft RPC 的 response
// ============================================================================

void RaftNode::handle_vote_response(const ReplicaID& from,
                                    const RequestVoteResult& result,
                                    RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    core_.handle_vote_response(from, result, effects);
}

Status RaftNode::handle_append_response(const ReplicaID& from,
                                        const AppendEntriesParam& sent_param,
                                        const AppendEntriesResult& result,
                                        RaftEffects& effects) {
    ADVISKV_METRICS_TIMER("storage_raft_handle_append_response");
    ADVISKV_METRICS_COUNTER("storage_raft_handle_append_response_request");

    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    return core_.handle_append_response(from, sent_param, result, effects);
}

void RaftNode::handle_append_send_failed(const ReplicaID& from,
                                         const AppendEntriesParam& sent_param,
                                         const Status& status) {
    std::lock_guard lock(mutex_);
    core_.handle_append_send_failed(from, sent_param, status);
}

// ============================================================================
// 查询当前 raft 的状态
// ============================================================================

ReplicaRole RaftNode::role() const {
    std::lock_guard lock(mutex_);
    return core_.role();
}

Term RaftNode::current_term() const {
    std::lock_guard lock(mutex_);
    return core_.current_term();
}

LogIndex RaftNode::commit_index() const {
    std::lock_guard lock(mutex_);
    return core_.commit_index();
}

LogIndex RaftNode::last_applied() const {
    std::lock_guard lock(mutex_);
    return core_.last_applied();
}

LogIndex RaftNode::last_log_index() const {
    std::lock_guard lock(mutex_);
    return core_.last_log_index();
}

Term RaftNode::last_log_term() const {
    std::lock_guard lock(mutex_);
    return core_.last_log_term();
}

LogIndex RaftNode::snapshot_index() const {
    std::lock_guard lock(mutex_);
    return core_.snapshot_index();
}

Term RaftNode::snapshot_term() const {
    std::lock_guard lock(mutex_);
    return core_.snapshot_term();
}

int RaftNode::quorum_size() const {
    std::lock_guard lock(mutex_);
    return core_.quorum_size();
}

bool RaftNode::is_leader() const {
    std::lock_guard lock(mutex_);
    return core_.is_leader();
}

RaftMemberType RaftNode::member_type(const ReplicaID& replica_id) const {
    std::lock_guard lock(mutex_);
    return core_.member_type(replica_id);
}

std::vector<RaftMember> RaftNode::raft_members() const {
    std::lock_guard lock(mutex_);
    return core_.raft_members();
}

// ============================================================================
// apply 相关的推进
// ============================================================================

std::vector<LogEntry> RaftNode::extract_committed_entries() {
    std::lock_guard lock(mutex_);
    return core_.extract_committed_entries();
}

void RaftNode::advance_last_applied(LogIndex applied) {
    std::lock_guard lock(mutex_);
    core_.advance_last_applied(applied);
}

Status RaftNode::apply_config_entry(const LogEntry& entry) {
    std::lock_guard lock(mutex_);
    return core_.apply_config_entry(entry);
}

// ============================================================================
// 快照相关
// ============================================================================

Status RaftNode::truncate_log(LogIndex index) {
    std::lock_guard lock(mutex_);
    return core_.truncate_log(index);
}

void RaftNode::install_local_snapshot(const InstallSnapshotContext& context) {
    std::lock_guard lock(mutex_);
    core_.install_local_snapshot(context);
}

void RaftNode::commit_install_snapshot(const InstallSnapshotContext& context,
                                       RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    core_.commit_install_snapshot(context, effects);
}

void RaftNode::handle_install_snapshot_response(
    const ReplicaID& from, const InstallSnapshotParam& sent_param,
    const InstallSnapshotResult& result, RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    core_.handle_install_snapshot_response(from, sent_param, result, effects);
}

void RaftNode::handle_install_snapshot_send_failed(
    const ReplicaID& from, const InstallSnapshotParam& sent_param,
    const Status& status) {
    std::lock_guard lock(mutex_);
    core_.handle_install_snapshot_send_failed(from, sent_param, status);
}

Status RaftNode::prepare_install_snapshot(const InstallSnapshotParam& param,
                                          RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    return core_.prepare_install_snapshot(param, effects);
}

// ============================================================================
// recovery 的时候会用到的更新
// ============================================================================

void RaftNode::update_raft_meta(const RaftMeta& meta) {
    std::lock_guard lock(mutex_);
    core_.update_raft_meta(meta);
}

void RaftNode::update_log_entries(const std::vector<LogEntry>& entries) {
    std::lock_guard lock(mutex_);
    core_.update_log_entries(entries);
}

void RaftNode::update_membership(const std::vector<RaftMember>& members) {
    std::lock_guard lock(mutex_);
    core_.update_membership(members);
}

void RaftNode::enter_recovering() {
    std::lock_guard lock(mutex_);
    core_.enter_recovering();
}

Status RaftNode::ensure_add_learner(const PeerMember& member,
                                    RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    return core_.ensure_add_learner(member, effects);
}

Status RaftNode::ensure_remove_member(const ReplicaID& replica_id,
                                      RaftEffects& effects) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    return core_.ensure_remove_member(replica_id, effects);
}

bool RaftNode::is_recovering() const {
    std::lock_guard lock(mutex_);
    return core_.is_recovering();
}

bool RaftNode::is_ready() const {
    std::lock_guard lock(mutex_);
    return core_.is_ready();
}

// ============================================================================
// 读一致性准备心跳
// ============================================================================

Status RaftNode::build_append_entries_for_read(RaftEffects& effects,
                                               LogIndex& read_index,
                                               Term& read_term) {
    std::lock_guard lock(mutex_);
    effects = RaftEffects{};
    return core_.build_append_entries_for_read(effects, read_index, read_term);
}

}  // namespace adviskv::storage