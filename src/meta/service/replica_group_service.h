#pragma once

#include <optional>

#include "common/model/type.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/model/param.h"

namespace adviskv::meta {

class MetaStore;
class MetaStoreTxn;
class NodeSelector;

struct ReplicaGroupReconcileContext {
    MetaStore* store{nullptr};
    NodeSelector* selector{nullptr};
};

inline bool is_healthy(const Replica& replica) {
    return replica.state.desired == ReplicaDesired::PRESENT && replica.state.phase != ReplicaPhase::LOST &&
           replica.state.phase != ReplicaPhase::ERROR && replica.state.phase != ReplicaPhase::DELETING &&
           replica.state.phase != ReplicaPhase::DELETED;
}

// 从 replica group 的 desired_members 里按优先级挑 count 个待删的 replica。
// filter_healthy 代表是否跳过健康副本
Status select_remove_member_victims(const MetaStoreTxn& txn, const ReplicaGroup& group, int32_t count,
                                    bool filter_healthy, std::vector<ReplicaID>& victims);

class ReplicaGroupService {
public:
    explicit ReplicaGroupService(MetaStore* store, NodeSelector* selector = nullptr);

    Status reconcile_all();
    Status build_heartbeat_result(const HeartBeatParam& param, HeartBeatResult& result) const;

private:
    MetaStore* store_{nullptr};
    NodeSelector* selector_{nullptr};
};

}  // namespace adviskv::meta