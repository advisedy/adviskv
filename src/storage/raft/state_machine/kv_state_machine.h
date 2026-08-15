#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/model/type.h"
#include "storage/engine/kv_engine.h"
#include "storage/model/model.h"
#include "storage/raft/state_machine/state_machine.h"
namespace adviskv::storage {

class KvStateMachine : public StateMachine {
public:
    explicit KvStateMachine(EngineType engine_type);

    Status apply(const LogEntry& entry) override;
    Status restore(const SnapshotPtr& snap, const KvForEach& for_each_kv,
                   const RequestForEach& for_each_request = {}) override;
    LogIndex apply_index() const override;
    LogIndex apply_term() const override;
    Status get(const Key& key, Value& value) const override;
    Status capture_snapshot(SnapshotData& snapshot) const override;

    Status for_each_kv(const std::function<Status(const Key&, const Value&)>& fn) const override;
    Status find_request(const std::string& request_id, RequestRecord& record) const override;
    Status for_each_request(const std::function<Status(const RequestRecord&)>& fn) const override;

    KVEngine* engine() { return engine_.get(); }

private:
    std::unique_ptr<KVEngine> engine_;
    LogIndex apply_index_{0};
    Term apply_term_{0};
    std::unordered_map<std::string, RequestRecord> request_records_;
};

}  // namespace adviskv::storage
