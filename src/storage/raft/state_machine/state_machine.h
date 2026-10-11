#pragma once
#include <functional>
#include <memory>
#include <vector>

#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"

namespace adviskv::storage {

using KvVisitor = std::function<Status(const Key&, const Value&)>;
using KvForEach = std::function<Status(const KvVisitor&)>;
using RequestVisitor = std::function<Status(const RequestRecord&)>;
using RequestForEach = std::function<Status(const RequestVisitor&)>;

class StateMachine {
public:
    virtual ~StateMachine() = default;

    virtual Status apply(const LogEntry& entry) = 0;
    virtual Status restore(const SnapshotHeaderPtr& snap, const KvForEach& for_each_kv,
                           const RequestForEach& for_each_request = {}) = 0;
    virtual LogIndex apply_index() const = 0;
    virtual LogIndex apply_term() const = 0;
    virtual Status get(const Key& key, Value& value) const = 0;

    // 需要在状态机锁内调用，返回状态机快照数据
    virtual Status capture_snapshot(SnapshotData& snapshot) const = 0;

    // 状态机给外部提供的一个接口，传递一个函数，可以操作状态机内部持有的KV
    virtual Status for_each_kv(const std::function<Status(const Key&, const Value&)>& fn) const = 0;

    virtual Status find_request(const std::string& request_id, RequestRecord& record) const = 0;
    virtual Status for_each_request(const std::function<Status(const RequestRecord&)>& fn) const = 0;
};

}  // namespace adviskv::storage
