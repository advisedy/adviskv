#include "storage/replica/replica_read_index_checker.h"

#include "common/define.h"
#include "common/log.h"
#include "storage/raft/core/raft_core.h"
#include "storage/replica/replica_loop.h"
#include "storage/replica/replica_message_dispatcher.h"

namespace adviskv::storage {

ReplicaReadIndexChecker::ReplicaReadIndexChecker(ReplicaContext& context, ReplicaLoop& loop)
        : context_(context), loop_(loop) {}

// 新 leader 刚当选时，虽然它是合法 leader，但它的 commit_index 可能还没包含旧
// term 中已经成功提交的日志。Raft 通过让新 leader 提交一条当前 term 的 no-op
// entry，来安全推进 commit_index；一旦当前 term 的 entry 被提交，它前面的旧
// term 日志也会被一起提交。ReadIndex 读之前检查“当前 term 是否已有 committed
// entry”，就是为了确保 leader 的 commit_index
// 已经处在一个安全位置。否则即使心跳拿到了多数派确认，也可能因为状态机还没
// apply 到那些旧的已提交写，导致读到旧值。
Status ReplicaReadIndexChecker::check_self_leader_and_get_read_index(LogIndex& read_index) {
    BuildReadIndexCall read_call;
    int limit;
    {
        std::lock_guard lock(context_.raft_core_mutex);
        limit = context_.raft_core.quorum_size();
    }
    loop_.sync_submit(&read_call);
    RETURN_IF_INVALID_STATUS(read_call.status)
    read_index = read_call.read_index;

    int success_cnt = 1;
    // 达到limit就可以了

    for (const RaftMessage& msg : read_call.effects.messages) {
        // if (msg.type != RaftMessageType::APPEND_ENTRIES) {
        //     continue;
        // }
        if (msg.type == RaftMessageType::APPEND_ENTRIES) {
            AppendEntriesResult res;
            Status status = context_.message_dispatcher.sync_send_append_entries(msg.target, msg.append_param, res);
            if (status.fail()) {
                LOG_WARN(
                        "[ReplicaReadIndexChecker] sync_send_append_entries "
                        "failed: status:{}",
                        status.to_string());
                continue;
            }

            AppendResponseCall call{AppendResponseEvent{msg.target.replica_id, msg.append_param, res}};
            loop_.sync_submit(&call);
            // 这个handle如果失败了，就说明自己不是leader
            RETURN_IF_INVALID_STATUS(call.status)
            if (res.term == read_call.read_term)
                success_cnt++;  // 这里不用写关于判断res.success，
                                // 因为我们只是需要确认leader这个身份，success可能是fail，因为prev没对齐
        } else if (msg.type == RaftMessageType::INSTALL_SNAPSHOT) {
            InstallSnapshotResult res;
            Status status = context_.message_dispatcher.sync_send_install_snapshot(msg.target, msg.snapshot_param, res);
            if (status.fail()) {
                SnapshotSendFailedCall call{SnapshotSendFailedEvent{msg.target.replica_id, msg.snapshot_param, status}};
                loop_.sync_submit(&call);
                RETURN_IF_INVALID_STATUS(call.status)
                continue;
            }
            SnapshotResponseCall call{SnapshotResponseEvent{msg.target.replica_id, msg.snapshot_param, res}};
            loop_.sync_submit(&call);
            RETURN_IF_INVALID_STATUS(call.status)
            if (res.term == read_call.read_term) success_cnt++;
        } else {
            LOG_WARN("replica:{} check self leader, but have request vote msg!", context_.replica_id.to_string());
        }
    }

    if (success_cnt >= limit) {
        return Status::OK();
    }
    return Status::NOT_LEADER("failed to confirm leader with quorum");
}

}  // namespace adviskv::storage