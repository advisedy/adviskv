#include "sdm/background/capacity_check_task.h"

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
        WARN("1");
        return;
    }
    for (TablePtr table : table_list) {
        for (int i = 0; i < table->spec.shard_count; i++) {
            status = check_replica_list(*table, i);
            if (status.fail()) {
                WARN("3");
                return;
            }
        }
    }
}

Status CapacityCheckTask::check_replica_list(const Table& table,
                                             ShardID shard_id) {
    Status status{Status::OK()};

    std::vector<ReplicaPtr> replicas;
    status =
        sdm_store_.list_replicas_by_shard(table.table_id, shard_id, replicas);
    RETURN_IF_INVALID_STATUS(status)

    int32_t replica_count = table.spec.replica_count;

    // 检测是否有没有replica超出replica_count了
    for (ReplicaPtr& replica : replicas) {
        if (replica->replica_key.replica_index < replica_count) {
            continue;
        }
        status = sdm_store_.del_replica(replica->replica_key);
        RETURN_IF_INVALID_STATUS(status)
    }

    // 检测有没有漏掉的
    for (int i = 0; i < replica_count; i++) {
        ReplicaKey key{table.table_id, shard_id, i};
        ReplicaPtr ptr;
        status = sdm_store_.get_replica(key, ptr);
        RETURN_IF_INVALID_STATUS(status)
        if (ptr) {
            continue;
        }
        // 说明缺失了，我们需要补回来
        ReplicaSpec spec{
            .dc = "",
        };
        ReplicaState state{
            .assign_node_id = "",
            .endpoint = {},
            .role = ReplicaRole::FOLLOWER,
            .status = ReplicaStatus::PENDING,
        };
        Replica replica{
            .replica_key = key,
            .spec = std::move(spec),
            .state = std::move(state),
        };
        status = sdm_store_.put_replica(replica);
        RETURN_IF_INVALID_STATUS(status)
    }
    return status;
}

}  // namespace adviskv::sdm