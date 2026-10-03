#pragma once

#include <vector>

#include "common/status.h"
#include "meta/model/model.h"

namespace adviskv::meta {

class MetaStoreTxn;

inline bool is_healthy(const Replica& replica) {
    return !is_absent(replica.state.phase) && replica.state.phase != ReplicaPhase::LOST &&
           replica.state.phase != ReplicaPhase::ERROR;
}

// 从 desired_members 中选待移除的副本；filter_healthy 为 true 时只选异常副本。
Status select_remove_member_victims(const MetaStoreTxn& txn, const ReplicaGroup& group, int32_t count,
                                    bool filter_healthy, std::vector<ReplicaID>& victims);

}  // namespace adviskv::meta
