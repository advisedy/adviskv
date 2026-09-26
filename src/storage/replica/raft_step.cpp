#include "storage/replica/raft_step.h"

#include <mutex>
#include <utility>

#include "common/define.h"
#include "common/metrics/metrics.h"
#include "storage/replica/replica.h"
#include "storage/replica/replica_message_dispatcher.h"

namespace adviskv::storage {

namespace {

Status persist_raft_effects(ReplicaContext& context, const RaftEffects& effects) {
    if (effects.entries_to_rewrite.has_value() && !effects.entries_to_append.empty()) {
        return Status::INVALID_ARGUMENT("cannot both rewrite and append");
    }
    if (effects.hard_state.has_value()) {
        RETURN_IF_INVALID_STATUS(context.fault_if_fail(context.persist.save_raft_meta(*effects.hard_state)))
    }
    if (effects.entries_to_rewrite.has_value()) {
        RETURN_IF_INVALID_STATUS(context.fault_if_fail(context.persist.rewrite_wal(*effects.entries_to_rewrite)))
    }
    if (!effects.entries_to_append.empty()) {
        RETURN_IF_INVALID_STATUS(context.fault_if_fail(context.persist.append_wal_batch(effects.entries_to_append)))
    }
    return Status::OK();
}

}  // namespace

Status run_raft_step(ReplicaContext& context, std::function<Status(RaftEffects&)> step) {
    ADVISKV_METRICS_TIMER("storage_replica_raft_loop_step");
    ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_request");

    RaftEffects effects;
    {
        std::lock_guard lock(context.raft_core_mutex);
        Status status = step(effects);
        RETURN_IF_INVALID_STATUS(persist_raft_effects(context, effects))
        RETURN_IF_INVALID_STATUS(status)
    }
    Status send_status = context.message_dispatcher.async_send(std::move(effects.messages));
    if (send_status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_failure");
        return send_status;
    }
    ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_success");
    return Status::OK();
}

}  // namespace adviskv::storage
