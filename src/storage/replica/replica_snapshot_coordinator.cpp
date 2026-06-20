#include "storage/replica/replica_snapshot_coordinator.h"

#include <memory>
#include <mutex>

#include "common/define.h"
#include "common/log.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/raft_node.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/replica_raft_effect_runner.h"

namespace adviskv::storage {
namespace {

static constexpr int SNAPSHOT_LIMIT = 1000;

}  // namespace

ReplicaSnapshotCoordinator::ReplicaSnapshotCoordinator(
    ReplicaContext& context, ReplicaRaftEffectRunner& effect_runner)
    : context_(context), effect_runner_(effect_runner) {}

Status ReplicaSnapshotCoordinator::handle_install_snapshot(
    const InstallSnapshotParam& param) {
    if (param.offset == 0) {
        LOG_INFO(
            "[Replica Snapshot] replica:{} start handle to install snapshot from replica:{}. "
            "The "
            "snapshot index:{}, snapshot term:{}",
            param.to_replica_id.to_string(), param.from_replica_id.to_string(),
            param.snapshot_index, param.snapshot_term);
    }
    
    LOG_DEBUG(
        "[Replica Snapshot] replica:{} is handling to install snapshot from replica:{}. The "
        "snapshot index:{}, snapshot term:{}",
        param.to_replica_id.to_string(), param.from_replica_id.to_string(),
        param.snapshot_index, param.snapshot_term);

    RETURN_IF_INVALID_STATUS(
        effect_runner_.run_raft_step([&](RaftEffects& effects) {
            return context_.raft_node.prepare_install_snapshot(
                param.term, param.snapshot_index, param.snapshot_term, effects);
        }))
    /*
        std::lock_guard snapshot_lock(persist_snapshot_mutex_);
        std::lock_guard state_machine_lock(state_machine_mutex_);
    */
    {
        std::lock_guard locker{context_.persist_snapshot_mutex};
        // 这个地方有可能会被截断
        // TODO
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(
            context_.persist.append_snapshot_chunk(param)))
        if (!param.done) return Status::OK();

        RETURN_IF_INVALID_STATUS(finish_install_snapshot(param))
        LOG_INFO(
            "[Replica Snapshot] replica:{} finish install snapshot from replica:{}. "
            "The "
            "snapshot index:{}, snapshot term:{}",
            param.to_replica_id.to_string(), param.from_replica_id.to_string(),
            param.snapshot_index, param.snapshot_term);
    }

    return Status::OK();
}

Status ReplicaSnapshotCoordinator::finish_install_snapshot(
    const InstallSnapshotParam& param) {
    SnapshotPtr snap = std::make_shared<Snapshot>();
    snap->apply_index = param.snapshot_index;
    snap->apply_term = param.snapshot_term;

    std::unique_lock raft_lock(context_.raft_step_mutex);

    SnapshotInstallPlan plan{};
    RaftEffects prepare_effects{};
    // 这里再check一下是否满足，因为中间有段时间没有锁，有可能本地这边会有更新操作
    Status prepare_status = context_.raft_node.build_install_snapshot_plan(
        param.term, param.snapshot_index, param.snapshot_term, plan,
        prepare_effects);
    RETURN_IF_INVALID_STATUS(
        effect_runner_.persist_raft_effects(prepare_effects))
    RETURN_IF_INVALID_STATUS(prepare_status)

    RETURN_IF_INVALID_STATUS(
        context_.fault_if_fail(context_.persist.finish_snapshot_receive()))

    {
        std::lock_guard lock(context_.state_machine_mutex);
        RETURN_IF_INVALID_STATUS(
            context_.fault_if_fail(context_.state_machine.restore(
                snap, [this](const KvVisitor& visitor) -> Status {
                    return context_.persist.for_each_snapshot_kv(visitor);
                })))
    }

    RaftEffects install_effects;
    context_.raft_node.commit_install_snapshot(plan, install_effects);
    RETURN_IF_INVALID_STATUS(
        effect_runner_.persist_raft_effects(install_effects))

    LOG_INFO(
        "[Replica Snapshot] replica:{} finish snapshot, snapshot_index:{}, "
        "snapshot_term:{}",
        context_.replica_id.to_string(), param.snapshot_index,
        param.snapshot_term);

    return Status::OK();
}

void ReplicaSnapshotCoordinator::try_take_snapshot() {
    // LogIndex last_index = raft_node_->last_log_index();
    // LogIndex apply_index = state_machine_->apply_index();
    // //我勒个雷，这里写成狗屎了啊，还一直没看出来，一直到e2e测试才发现

    LogIndex last_apply_index = context_.raft_node.last_applied(),
             snapshot_index = context_.raft_node.snapshot_index();
    if (last_apply_index - snapshot_index < SNAPSHOT_LIMIT) return;

    std::lock_guard snapshot_lock(context_.persist_snapshot_mutex);
    std::lock_guard state_machine_lock(context_.state_machine_mutex);
    last_apply_index = context_.raft_node.last_applied();
    snapshot_index = context_.raft_node.snapshot_index();
    if (last_apply_index - snapshot_index < SNAPSHOT_LIMIT) return;

    LOG_INFO(
        "[Replica Snapshot] replica_id:{} try take snapshot start, "
        "snapshot_index:{}, snapshot_term:{}",
        context_.replica_id.to_string(), context_.state_machine.apply_index(),
        context_.state_machine.apply_term());

    Status status = context_.persist.do_snapshot(context_.state_machine);

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
        context_.raft_node.truncate_log(context_.state_machine.apply_index());

        LOG_INFO(
            "[Replica Snapshot] replica_id:{} try take snapshot finish, "
            "snapshot_index:{}, snapshot_term:{}",
            context_.replica_id.to_string(),
            context_.state_machine.apply_index(),
            context_.state_machine.apply_term());

    } else {
        LOG_WARN("[Replica Snapshot] replica:try_take_snapshot: status:{}", status.to_string());
        context_.enter_faulted();
    }
}

}  // namespace adviskv::storage