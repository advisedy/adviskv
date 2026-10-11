#include "storage/replica/raft_step.h"

#include <utility>

#include "common/define.h"
#include "common/metrics/metrics.h"
#include "storage/persist/persist_engine.h"
#include "storage/replica/replica_message_dispatcher.h"

namespace adviskv::storage {

Status RaftEffectExecutor::persist_effects(const RaftEffects& effects) const {
    if (effects.entries_to_rewrite.has_value() && !effects.entries_to_append.empty()) {
        return Status::INVALID_ARGUMENT("cannot both rewrite and append");
    }
    if (effects.hard_state.has_value()) {
        RETURN_IF_INVALID_STATUS(fault_reporter_.fail_if(persist_.raft_meta().save(*effects.hard_state)))
    }
    if (effects.entries_to_rewrite.has_value()) {
        RETURN_IF_INVALID_STATUS(fault_reporter_.fail_if(persist_.wal().rewrite(*effects.entries_to_rewrite)))
    }
    if (!effects.entries_to_append.empty()) {
        RETURN_IF_INVALID_STATUS(fault_reporter_.fail_if(persist_.wal().append_batch(effects.entries_to_append)))
    }
    return Status::OK();
}

Status RaftEffectExecutor::execute(const std::function<Status(RaftEffects&)>& step) const {
    ADVISKV_METRICS_TIMER("storage_replica_raft_loop_step");
    ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_request");

    RaftEffects effects;
    {
        Status status = step(effects);
        RETURN_IF_INVALID_STATUS(persist_effects(effects))
        RETURN_IF_INVALID_STATUS(status)
    }
    Status send_status = dispatcher_.async_send(std::move(effects.messages));
    if (send_status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_failure");
        return send_status;
    }
    ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_success");
    return Status::OK();
}

}  // namespace adviskv::storage
