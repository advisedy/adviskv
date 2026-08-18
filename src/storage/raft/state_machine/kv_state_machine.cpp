#include "storage/raft/state_machine/kv_state_machine.h"

#include <memory>
#include <utility>

#include "common/define.h"
#include "common/metrics/metrics.h"
#include "common/model/type.h"
#include "common/status.h"
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

    if (!entry.request_id.empty() && entry.op_type != WriteOpType::PUT && entry.op_type != WriteOpType::DEL) {
        return Status::INVALID_ARGUMENT("request_id is only valid for PUT or DEL");
    }

    if (!entry.request_id.empty() && (entry.op_type == WriteOpType::PUT || entry.op_type == WriteOpType::DEL)) {
        RequestRecord existing;
        if (find_request(entry.request_id, existing).ok()) {
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_request_id_skipped");
            apply_term_ = entry.term;
            apply_index_ = entry.index;
            ADVISKV_METRICS_COUNTER("storage_state_machine_apply_success");
            return Status::OK();
        }
    }

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

    if (!entry.request_id.empty() && (entry.op_type == WriteOpType::PUT || entry.op_type == WriteOpType::DEL)) {
        auto result = request_records_.emplace(
                entry.request_id,
                RequestRecord{entry.request_id, entry.op_type,
                              RequestRecord::request_payload_hash(entry.op_type, entry.key, entry.value)});
        if (result.second) {
            ADVISKV_METRICS_COUNTER("storage_state_machine_request_record_insert");
        }
    }
    apply_term_ = entry.term;
    apply_index_ = entry.index;
    ADVISKV_METRICS_COUNTER("storage_state_machine_apply_success");
    return Status::OK();
}
Status KvStateMachine::restore(const SnapshotPtr& snap, const KvForEach& for_each_kv,
                               const RequestForEach& for_each_request) {
    if (!snap) {
        return Status{StatusCode::ERROR, "snapshot is nullptr"};
    }
    if (!engine_) {
        return Status{StatusCode::ERROR, "engine is nullptr"};
    }

    engine_->clear();
    request_records_.clear();

    if (for_each_kv) {
        RETURN_IF_INVALID_STATUS(
                for_each_kv([this](const Key& key, const Value& value) -> Status { return engine_->put(key, value); }))
    }

    if (for_each_request) {
        RETURN_IF_INVALID_STATUS(for_each_request([this](const RequestRecord& record) -> Status {
            if (record.request_id.empty()) {
                return Status::INVALID_ARGUMENT("snapshot contains an empty request_id");
            }
            request_records_[record.request_id] = record;
            return Status::OK();
        }))
    }

    apply_index_ = snap->apply_index;
    apply_term_ = snap->apply_term;
    return Status::OK();
}
LogIndex KvStateMachine::apply_index() const { return apply_index_; }
LogIndex KvStateMachine::apply_term() const { return apply_term_; }

Status KvStateMachine::get(const Key& key, Value& value) const { return engine_->get(key, value); }

Status KvStateMachine::capture_snapshot(SnapshotData& snapshot) const {
    ADVISKV_METRICS_TIMER("storage_state_machine_capture_snapshot");

    if (!engine_) {
        return Status{StatusCode::ERROR, "engine is nullptr"};
    }

    SnapshotData data;
    data.apply_index = apply_index_;
    data.apply_term = apply_term_;
    data.kvs = engine_->dump_all();
    data.request_records.reserve(request_records_.size());
    for (const auto& [_, record] : request_records_) {
        data.request_records.push_back(record);
    }

    snapshot = std::move(data);
    return Status::OK();
}

Status KvStateMachine::for_each_kv(const std::function<Status(const Key&, const Value&)>& fn) const {
    if (!engine_) {
        return Status{StatusCode::ERROR, "engine is nullptr"};
    }
    return engine_->for_each_kv(fn);
}

Status KvStateMachine::find_request(const std::string& request_id, RequestRecord& record) const {
    if (request_id.empty()) {
        return Status::INVALID_ARGUMENT("request_id is empty");
    }
    auto it = request_records_.find(request_id);
    if (it == request_records_.end()) {
        return Status::KEY_NOT_FOUND("request_id not found");
    }
    record = it->second;
    return Status::OK();
}

Status KvStateMachine::for_each_request(const std::function<Status(const RequestRecord&)>& fn) const {
    if (!fn) {
        return Status::INVALID_ARGUMENT("request visitor is empty");
    }
    for (const auto& [_, record] : request_records_) {
        RETURN_IF_INVALID_STATUS(fn(record))
    }
    return Status::OK();
}

}  // namespace adviskv::storage
