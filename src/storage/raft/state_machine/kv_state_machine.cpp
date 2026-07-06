#include "storage/raft/state_machine/kv_state_machine.h"

#include <memory>

#include "common/define.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "common/model/type.h"
#include "storage/engine/map_engine.h"
#include "storage/model/model.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::storage {
namespace {

void record_state_machine_apply_op(WriteOpType op) {
    switch (op) {
        case WriteOpType::PUT:
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_put");
            break;
        case WriteOpType::DEL:
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_del");
            break;
        case WriteOpType::ADD_LEARNER:
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_add_learner");
            break;
        case WriteOpType::PROMOTE_VOTER:
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_promote_voter");
            break;
        case WriteOpType::REMOVE_MEMBER:
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_remove_member");
            break;
        case WriteOpType::NONE:
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_none");
            break;
        default:
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_unknown");
            break;
    }
}

}  // namespace

KvStateMachine::KvStateMachine(EngineType engine_type) {
    switch (engine_type) {
        case EngineType::MAP:
            engine_ = std::make_unique<MapEngine>();
            break;
        default:
            break;
    }
}

Status KvStateMachine::apply(const LogEntry& entry) {
    ADVISKV_METRICS_TIMER("storage_state_machine_apply");
    ADVISKV_METRICS_COUNTER("storage_state_machine_apply_request");
    record_state_machine_apply_op(entry.op_type);

    Status status{Status::OK()};
    switch (entry.op_type) {
        case WriteOpType::PUT: {
            status = engine_->put(entry.key, entry.value);
            if (status.fail()) {
                ADVISKV_METRICS_COUNTER("storage_state_machine_apply_failure");
                return status;
            }
            break;
        }
        case WriteOpType::DEL: {
            status = engine_->del(entry.key);
            if (status.fail()) {
                ADVISKV_METRICS_COUNTER("storage_state_machine_apply_failure");
                return status;
            }
            break;
        }
        case WriteOpType::ADD_LEARNER:
        case WriteOpType::PROMOTE_VOTER:
        case WriteOpType::REMOVE_MEMBER:
        case WriteOpType::NONE:
            break;

        default: {
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_failure");
            return Status{StatusCode::ERROR};
        }
    }
    apply_term_ = entry.term;
    apply_index_ = entry.index;
    ADVISKV_METRICS_COUNTER("storage_state_machine_apply_success");
    return Status::OK();
}
Status KvStateMachine::restore(const SnapshotPtr& snap, const KvForEach& for_each_kv) {
    if (!snap) {
        return Status{StatusCode::ERROR, "snapshot is nullptr"};
    }
    if (!engine_) {
        return Status{StatusCode::ERROR, "engine is nullptr"};
    }

    engine_->clear();
    RETURN_IF_INVALID_STATUS(
            for_each_kv([this](const Key& key, const Value& value) -> Status { return engine_->put(key, value); }))

    apply_index_ = snap->apply_index;
    apply_term_ = snap->apply_term;
    return Status::OK();
}
LogIndex KvStateMachine::apply_index() const {
    return apply_index_;
}
LogIndex KvStateMachine::apply_term() const {
    return apply_term_;
}

Status KvStateMachine::get(const Key& key, Value& value) const {
    return engine_->get(key, value);
}

Status KvStateMachine::for_each_kv(const std::function<Status(const Key&, const Value&)>& fn) const {
    if (!engine_) {
        return Status{StatusCode::ERROR, "engine is nullptr"};
    }
    return engine_->for_each_kv(fn);
}

}  // namespace adviskv::storage
