#include "storage/replica/replica_applier.h"

#include "common/crash_injection.h"
#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/model/type.h"

namespace adviskv::storage {

ReplicaApplier::ReplicaApplier(ReplicaContext& context) : context_(context) {}

Status ReplicaApplier::apply_committed_entries() {
    ADVISKV_METRICS_TIMER("storage_replica_apply_committed_entries");
    ADVISKV_METRICS_COUNTER("storage_replica_apply_committed_entries_request");

    std::vector<LogEntry> entries;
    {
        std::lock_guard lock(context_.raft_core_mutex);
        entries = context_.raft_core.extract_committed_entries();
    }
    ADVISKV_METRICS_COUNTER("storage_replica_apply_entry", to<int64_t>(entries.size()));
    for (const LogEntry& entry : entries) {
        RETURN_IF_INVALID_STATUS(apply_log_entry(entry))
    }
    return Status::OK();
}

Status ReplicaApplier::apply_log_entry(const LogEntry& entry) {
    switch (entry.op_type) {
        case WriteOpType::PUT:
        case WriteOpType::DEL:
        case WriteOpType::NONE:
            return apply_kv_log_entry(entry);
        case WriteOpType::ADD_LEARNER:
        case WriteOpType::PROMOTE_VOTER:
        case WriteOpType::REMOVE_MEMBER:
            return apply_config_log_entry(entry);
    }

    return Status{StatusCode::ERROR, "unsupported raft log entry type"};
}

Status ReplicaApplier::apply_kv_log_entry(const LogEntry& entry) {
    Status status = context_.state_machine.apply(entry);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_failure");
        LOG_WARN("apply_kv_log_entry failed, index={}, msg={}", entry.index, status.msg());
        return status;
    }
    testhook::crash_point("replica.apply.after_state_machine_before_progress");
    ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_success");
    {
        std::lock_guard lock(context_.raft_core_mutex);
        context_.raft_core.advance_last_applied(entry.index);
    }

    return Status::OK();
}

Status ReplicaApplier::apply_config_log_entry(const LogEntry& entry) {
    {
        std::lock_guard lock(context_.raft_core_mutex);
        Status status = context_.raft_core.apply_config_entry(entry);
        if (status.fail()) {
            ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_failure");
            LOG_WARN("apply_config_log_entry failed, index={}, msg={}", entry.index, status.msg());
            return status;
        }
    }
    Status status = context_.state_machine.apply(entry);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_failure");
        LOG_WARN("apply_config_log_entry state_machine apply failed, index={}, msg={}", entry.index, status.msg());
        return status;
    }
    ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_success");
    return Status::OK();
}

}  // namespace adviskv::storage
