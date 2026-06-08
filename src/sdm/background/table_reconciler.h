#pragma once

#include <vector>

#include "common/background_task.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/client/storage_client.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/store.h"
#include "sdm/selector/node_selector/node_selector.h"

namespace adviskv::sdm {

class TableReconciler : public BackgroundTask {
   public:
    TableReconciler(SdmStore* store, IStorageClient* storage_client,
                    NodeSelector* selector);

    Status reconcile_once();

   protected:
    void run() override;

   private:
    Status reconcile_table(Table& table);
    Status reconcile_present(Table& table);
    Status reconcile_absent(Table& table);

    // 确保我们这边的replicas是设置好了的
    Status ensure_replica_metadata(Table& table);

    // 确保storage那边的replicas都是搞好了
    Status ensure_storage_replicas(Table& table);
    Status refresh_storage_replica_info(Table& table);
    Status ensure_routes_absent(const Table& table);
    Status ensure_storage_replicas_absent(const Table& table);
    Status ensure_replica_metadata_absent(const Table& table);
    Status get_assigned_node_endpoint(const Replica& replica,
                                      Endpoint& endpoint) const;

    bool all_replicas_ready(const Table& table);
    bool all_routes_ready(const Table& table);
    Status mark_table_error(Table& table, const Status& status);

    std::vector<PeerMember> build_members(const std::vector<Node>& nodes,
                                          TableID table_id,
                                          ShardIndex shard_index);
    Status build_replicas(Table& table, ShardIndex shard_index,
                          const std::vector<Node>& nodes,
                          const std::vector<PeerMember>& members);
    SdmStore* store_{nullptr};
    IStorageClient* storage_client_{nullptr};
    NodeSelector* selector_{nullptr};
};

}  // namespace adviskv::sdm
