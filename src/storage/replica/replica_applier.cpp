#include "storage/replica/replica_applier.h"

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/type.h"

namespace adviskv::storage {

ReplicaApplier::ReplicaApplier(RaftNode& raft_node,
                               StateMachine& state_machine)
    : raft_node_(raft_node), state_machine_(state_machine) {}

Status ReplicaApplier::apply_committed_entries() {
    ADVISKV_METRICS_TIMER("storage_replica_apply_committed_entries");
    ADVISKV_METRICS_COUNTER("storage_replica_apply_committed_entries_request");

    std::vector<LogEntry> entries = raft_node_.extract_committed_entries();
    ADVISKV_METRICS_COUNTER("storage_replica_apply_entry",
                            to<int64_t>(entries.size()));
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
    }

    return Status{StatusCode::ERROR, "unsupported raft log entry type"};
}

Status ReplicaApplier::apply_kv_log_entry(const LogEntry& entry) {
    Status status = state_machine_.apply(entry);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_failure");
        LOG_WARN("apply_kv_log_entry failed, index={}, msg={}", entry.index,
                 status.msg());
        return status;
    }
    ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_success");
    raft_node_.advance_last_applied(entry.index);

    return Status::OK();
}

}  // namespace adviskv::storage
