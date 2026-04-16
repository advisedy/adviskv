#pragma once

#include <memory>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/route_model.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

/*
这里暂时统一一下，关于get没有找到的情况，错误码返回的还是Status::OK，只不过指针是nullptr.
*/
class SdmStore {
   public:
    Status put_table(const Table& table);
    Status get_table(TableID table_id, std::shared_ptr<Table>& out) const;
    Status get_table_by_name(const std::string& db_name,
                             const std::string& table_name,
                             std::shared_ptr<Table>& out) const;
    Status list_tables(std::vector<std::shared_ptr<Table>>& out) const;

    Status get_shard_route(TableID table_id, ShardID shard_id,
                           std::shared_ptr<ShardRoute>& out) const;
    Status put_shard_route(const ShardRoute& route);
    Status del_shard_route_entry(TableID table_id, ShardID shard_id,
                                 const ReplicaKey& replica_key);

    Status put_node(const Node& node);
    Status get_node(const NodeID& node_id, NodePtr& out) const;

    Status get_resource_pool(const std::string& name,
                             std::shared_ptr<ResourcePool>& out) const;

    Status list_resource_pools(
        std::vector<std::shared_ptr<ResourcePool>>& pools) const;

    Status get_replica(const ReplicaKey& replica_key, ReplicaPtr& out) const;
    Status put_replica(const Replica& replica);
    Status del_replica(const ReplicaKey& replica_key);

    // Status get_table_shard(TableID table_id, ShardID shard_id) const;
    Status list_replicas_by_shard(TableID table_id, ShardID shard_id,
                                  std::vector<ReplicaPtr>& out) const;
};

}  // namespace adviskv::sdm
