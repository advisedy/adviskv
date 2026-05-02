#include "storage/raft/state_machine/kv_state_machine.h"

#include <memory>

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/engine/map_engine.h"
#include "storage/model/param.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::storage {

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
    Status status{Status::OK()};
    switch (entry.op_type) {
        case WriteOpType::PUT: {
            RETURN_IF_INVALID_STATUS(engine_->put(entry.key, entry.value))
            break;
        }
        case WriteOpType::DEL: {
            RETURN_IF_INVALID_STATUS(engine_->del(entry.key))
            break;
        }
        case WriteOpType::NONE: {
            break;
        }
        default: {
            return Status{StatusCode::ERROR};
        }
    }
    apply_term_ = entry.term;
    apply_index_ = entry.index;
    return Status::OK();
}
SnapshotPtr KvStateMachine::snapshot() const {
    auto snap = std::make_shared<Snapshot>();
    snap->apply_index = apply_index_;
    snap->apply_term = apply_term_;
    std::vector<KV> kvs = engine_->dump_all();
    snap->kvs = std::move(kvs);
    return snap;
}
Status KvStateMachine::restore(const SnapshotPtr& snap) {
    apply_index_ = snap->apply_index;
    apply_term_ = snap->apply_term;
    engine_->clear();
    for (const KV& kv : snap->kvs) {
        Status status = engine_->put(kv.first, kv.second);
        if (status.fail()) {
            WARN("restore warn");
        }
    }
    return Status::OK();
}
LogIndex KvStateMachine::apply_index() const { return apply_index_; }
LogIndex KvStateMachine::apply_term() const { return apply_term_; }

Status KvStateMachine::get(const Key& key, Value& value) const {
    return engine_->get(key, value);
}

}  // namespace adviskv::storage