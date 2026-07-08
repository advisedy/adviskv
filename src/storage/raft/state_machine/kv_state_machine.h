#pragma once

#include <functional>
#include <memory>

#include "common/model/type.h"
#include "storage/engine/kv_engine.h"
#include "storage/model/model.h"
#include "storage/raft/state_machine/state_machine.h"
namespace adviskv::storage {

class KvStateMachine : public StateMachine {
public:
    explicit KvStateMachine(EngineType engine_type);

    Status apply(const LogEntry& entry) override;
    Status restore(const SnapshotPtr& snap, const KvForEach& for_each_kv) override;
    LogIndex apply_index() const override;
    LogIndex apply_term() const override;
    Status get(const Key& key, Value& value) const override;

    Status for_each_kv(const std::function<Status(const Key&, const Value&)>& fn) const override;

    KVEngine* engine() { return engine_.get(); }

private:
    std::unique_ptr<KVEngine> engine_;
    LogIndex apply_index_{0};
    Term apply_term_{0};
};

}  // namespace adviskv::storage
