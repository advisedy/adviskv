#include "storage/replica/replica_read_index_checker.h"

#include <mutex>

#include "common/define.h"
#include "common/log.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/raft_node.h"
#include "storage/raft/raft_sender.h"
#include "storage/replica/replica_raft_effect_runner.h"

namespace adviskv::storage {

ReplicaReadIndexChecker::ReplicaReadIndexChecker(
    ReplicaContext& context, ReplicaRaftEffectRunner& effect_runner)
    : context_(context), effect_runner_(effect_runner) {}

// 新 leader 刚当选时，虽然它是合法 leader，但它的 commit_index 可能还没包含旧
// term 中已经成功提交的日志。Raft 通过让新 leader 提交一条当前 term 的 no-op
// entry，来安全推进 commit_index；一旦当前 term 的 entry 被提交，它前面的旧
// term 日志也会被一起提交。ReadIndex 读之前检查“当前 term 是否已有 committed
// entry”，就是为了确保 leader 的 commit_index
// 已经处在一个安全位置。否则即使心跳拿到了多数派确认，也可能因为状态机还没
// apply 到那些旧的已提交写，导致读到旧值。
Status ReplicaReadIndexChecker::check_self_leader_and_get_read_index(
    LogIndex& read_index) {
    if (!context_.raft_node.is_leader()) return Status::NOT_LEADER();

    // 我们需要检测一下自己是不是leader，并且需要发送给followers自己的心跳，主动发送一次
    RaftEffects read_effects;
    Term read_term;
    RETURN_IF_INVALID_STATUS(context_.raft_node.build_append_entries_for_read(
        read_effects, read_index, read_term))

    int success_cnt = 1;
    int limit = context_.raft_node.quorum_size();
    // 达到limit就可以了

    for (const RaftMessage& msg : read_effects.messages) {
        // if (msg.type != RaftMessageType::APPEND_ENTRIES) {
        //     continue;
        // }

        if (msg.type == RaftMessageType::APPEND_ENTRIES) {
            AppendEntriesResult res;
            Status status = context_.raft_sender.send_append_entries(
                msg.target, msg.append_param, res);
            if (status.fail()) continue;

            // 这个handle如果失败了，就说明自己不是leader
            RETURN_IF_INVALID_STATUS(effect_runner_.run_raft_step([&](
                RaftEffects& effects) {
                    return context_.raft_node.handle_append_response(
                        msg.target.replica_id, msg.append_param, res, effects);
                }))
            if (res.term == read_term)
                success_cnt++;  // 这里不用写关于判断res.success，
            // 因为我们只是需要确认leader这个身份，success可能是fail，因为prev没对齐
        } else if (msg.type == RaftMessageType::INSTALL_SNAPSHOT) {
            InstallSnapshotResult res;
            Status status;
            {
                std::lock_guard locker{context_.persist_snapshot_mutex};
                status = context_.raft_sender.send_install_snapshot(
                    msg.target, msg.snapshot_param, context_.persist, res);
            }
            if (status.fail()) {
                context_.raft_node.handle_install_snapshot_send_failed(
                    msg.target.replica_id, msg.snapshot_param, status);
                continue;
            }
            RETURN_IF_INVALID_STATUS(effect_runner_.run_raft_step([&](
                RaftEffects& effects) {
                    context_.raft_node.handle_install_snapshot_response(
                        msg.target.replica_id, msg.snapshot_param, res,
                        effects);
                    return Status::OK();
                }))
            if (res.term == read_term) success_cnt++;
        } else {
            LOG_WARN("replica:{} check self leader, but have request vote msg!",
                     context_.replica_id.to_string());
        }
    }

    if (success_cnt >= limit) {
        return Status::OK();
    }
    return Status::NOT_LEADER("failed to confirm leader with quorum");
}

}  // namespace adviskv::storage