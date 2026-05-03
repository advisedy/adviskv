#include "sdm/background/capacity_check_task.h"

#include <algorithm>
#include <cstdint>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

void CapacityCheckTask::run() {
    std::vector<TablePtr> table_list;
    Status status = sdm_store_.list_tables(table_list);
    if (status.fail()) {
        LOG_WARN("1");
        return;
    }
    for (TablePtr table : table_list) {
        for (int i = 0; i < table->spec.shard_count; i++) {
            status = check_replica_list(*table, static_cast<ShardIndex>(i));
            if (status.fail()) {
                LOG_WARN("3");
                return;
            }
        }
    }
}

Status CapacityCheckTask::check_replica_list(const Table& table,
                                             ShardIndex shard_index) {
    Status status{Status::OK()};
    const ShardID shard_id{.table_id = table.table_id,
                           .shard_index = shard_index};

    std::vector<ReplicaPtr> replicas;
    status = sdm_store_.list_replicas_by_shard(shard_id, replicas);
    RETURN_IF_INVALID_STATUS(status)

    int32_t replica_count = table.spec.replica_count;

    // 检测一下有没有lost的replica，直接把他们删掉吧.
    std::vector<ReplicaPtr> lost_replicas;

    std::for_each(replicas.begin(), replicas.end(),
                  [&lost_replicas](const ReplicaPtr& replica) {
                      if (replica->spec.status == ReplicaStatus::LOST) {
                          lost_replicas.emplace_back(replica);
                      }
                  });

    for (const ReplicaPtr& replica : lost_replicas) {
        status = sdm_store_.del_replica(replica->replica_id);
        RETURN_IF_INVALID_STATUS(status)
    }

    ad_erase_if(replicas, [](const ReplicaPtr& replica) {
        return replica->spec.status == ReplicaStatus::LOST;
    });

    // 检测是否有没有replica超出replica_count了
    for (ReplicaPtr& replica : replicas) {
        if (replica->replica_id.replica_index < replica_count) {
            continue;
        }
        status = sdm_store_.del_replica(replica->replica_id);
        RETURN_IF_INVALID_STATUS(status)
    }

    // 检测有没有漏掉的
    for (int i = 0; i < replica_count; i++) {
        ReplicaID key{table.table_id, shard_index, i};
        ReplicaPtr ptr;
        status = sdm_store_.get_replica(key, ptr);
        RETURN_IF_INVALID_STATUS(status)
        if (ptr) {
            continue;
        }
        // 说明缺失了，我们需要补回来
        ReplicaSpec spec{
            .dc = "",
            .assign_node_id = "",
        };
        Replica replica{
            .replica_id = key,
            .spec = std::move(spec),
        };
        status = sdm_store_.put_replica(replica);
        RETURN_IF_INVALID_STATUS(status)
    }
    return status;
}

}  // namespace adviskv::sdm
