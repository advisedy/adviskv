#include <algorithm>
#include <utility>

#include <fmt/format.h>

#include "common/define.h"
#include "common/log.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::storage {

Status RaftCore::truncate_log(LogIndex new_snapshot_index) {
    if (new_snapshot_index > raft_apply_.last_applied()) {
        LOG_WARN(
                "[RaftCore Snapshot] new_snap_index > last_applied, "
                "new_snapshhot_index:{}, "
                "last_applied:{}",
                new_snapshot_index, raft_apply_.last_applied());
        return Status::ERROR("new_snap_index > last_applied");
    }
    return raft_log_.truncate(new_snapshot_index);
}

RaftLog::InstallSnapshotResult RaftCore::install_snapshot_into_log(const SnapshotHeader& header) {
    RaftLog::InstallSnapshotResult result = raft_log_.install_snapshot(header.apply_index, header.apply_term);
    raft_apply_.install_snapshot(header.apply_index);
    if (!header.members.empty()) {
        membership_.update_raft_members(header.members);
    }
    finish_recovering();
    return result;
}

void RaftCore::install_snapshot(const SnapshotHeader& header, RaftEffects& effects) {
    RaftLog::InstallSnapshotResult result = install_snapshot_into_log(header);
    effects.entries_to_rewrite = std::move(result.retained_entries);
}

void RaftCore::handle_install_snapshot_response(const ReplicaID& from, const InstallSnapshotParam& sent_param,
                                                const InstallSnapshotResult& result, RaftEffects& effects) {
    if (ensure_ready().fail()) return;

    if (result.term > election_.current_term()) {
        become_follower(result.term, effects);
        return;
    }

    if (!election_.is_leader()) return;

    if (sent_param.term != election_.current_term()) {
        LOG_WARN(
                "[RaftCore Snapshot] raft replica_id:{} handle snapshot response "
                "replica_id:{}, "
                "sent_param.term:{} != election_.current_term:{}",
                self_id_.to_string(), from.to_string(), sent_param.term, election_.current_term());
        return;
    }

    replication_.clear_snapshot_inflight(from, sent_param.snapshot_index);

    if (result.status.ok()) {
        LogIndex watermark = std::max(result.snapshot_watermark, sent_param.snapshot_index);
        replication_.update_snapshot_watermark(from, watermark);
        return;
    }

    if (result.status.fail()) {
        LOG_WARN(
                "[RaftCore Snapshot] raft replica_id:{} handle snapshot response "
                "replica_id:{}, result "
                "status.fail, status:{}",
                self_id_.to_string(), from.to_string(), result.status.to_string());
        return;
    }
}

void RaftCore::handle_install_snapshot_send_failed(const ReplicaID& from, const InstallSnapshotParam& sent_param,
                                                   const Status& status) {
    if (ensure_ready().fail()) return;

    if (!election_.is_leader()) return;

    if (sent_param.term != election_.current_term()) {
        LOG_WARN(
                "[RaftCore Snapshot] raft replica_id:{} handle snapshot send "
                "failed replica_id:{}, "
                "sent_param.term:{} != election_.current_term:{}, status:{}",
                self_id_.to_string(), from.to_string(), sent_param.term, election_.current_term(), status.to_string());
        return;
    }

    replication_.clear_snapshot_inflight(from, sent_param.snapshot_index);
    LOG_WARN(
            "[RaftCore Snapshot] raft replica_id:{} clear snapshot inflight for "
            "replica_id:{} after "
            "send failed, snapshot_index:{}, status:{}",
            self_id_.to_string(), from.to_string(), sent_param.snapshot_index, status.to_string());
}

Status RaftCore::prepare_install_snapshot(const InstallSnapshotParam& param, RaftEffects& effects,
                                          LogIndex& watermark_out) {
    watermark_out = 0;
    if (!membership_.contains(param.from_replica_id)) {
        LOG_WARN(
                "[RaftCore Snapshot] replica:{} reject InstallSnapshot from "
                "non-member replica:{}, term:{}, current_term:{}",
                self_id_.to_string(), param.from_replica_id.to_string(), param.term, election_.current_term());
        return Status::INVALID_ARGUMENT("install snapshot from non-member");
    }

    if (param.term < election_.current_term()) {
        return Status::ERROR(fmt::format("[RaftCore Snapshot] leade term:{} is oldear than current term:{}", param.term,
                                         election_.current_term()));
    }

    if (param.term > election_.current_term() || !election_.is_follower()) {
        become_follower(param.term, effects);
    }

    election_tick_trigger_.clear();
    mark_recent_leader_active();

    if (param.snapshot_index <= raft_apply_.commit_index()) {
        LOG_INFO(
                "[RaftCore Snapshot] prepare_install_snapshot: leader "
                "snapshot_index:{} <= "
                "commit_index:{}, snapshot_index:{}, last_applied:{}",
                param.snapshot_index, raft_apply_.commit_index(), raft_log_.snapshot_index(),
                raft_apply_.last_applied());
        watermark_out = raft_apply_.commit_index();
        return Status::OK();
    }

    if (!recovering_ && param.snapshot_index <= raft_log_.last_log_index() &&
        raft_log_.term_at(param.snapshot_index) == param.snapshot_term) {
        LOG_INFO(
                "[RaftCore Snapshot] follower already has the log, "
                "leader snapshot can not "
                "bring something new, snapshot_index:{}, "
                "snapshot_term:{}",
                param.snapshot_index, param.snapshot_term);
        watermark_out = param.snapshot_index;
        return Status::OK();
    }

    return Status::OK();
}
}  // namespace adviskv::storage