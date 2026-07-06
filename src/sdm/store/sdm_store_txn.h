#pragma once

#include <string>
#include <vector>

#include "common/status.h"
#include "common/model/type.h"
#include "sdm/store/runtime_index.h"
#include "sdm/model/model.h"

namespace adviskv::sdm {

class SdmStore;

class SdmStoreTxn {
   public:
    Status put_table(const Table& table);
    Status get_table(TableID table_id, TableOr& out) const;
    Status get_table_by_name(const std::string& db_name,
                             const std::string& table_name, TableOr& out) const;
    Status list_tables(std::vector<Table>& out) const;
    Status delete_table(TableID table_id);

    Status put_node(const Node& node);
    Status get_node(const NodeID& node_id, NodeOr& out) const;
    Status list_nodes(std::vector<Node>& out) const;
    Status list_nodes_by_resource_pool(const std::string& pool_name,
                                       std::vector<Node>& out) const;

    Status get_resource_pool(const std::string& name,
                             ResourcePoolOr& out) const;
    Status list_resource_pools(std::vector<ResourcePool>& pools) const;

    Status put_replica(const Replica& replica);
    Status put_replicas(const std::vector<Replica>& replicas);
    Status get_replica(const ReplicaID& replica_key, ReplicaOr& out) const;
    Status del_replica(const ReplicaID& replica_key);
    Status list_replicas(std::vector<Replica>& out) const;
    Status list_replicas_by_shard(const ShardID& shard_id,
                                  std::vector<Replica>& out) const;
    Status list_replicas_by_node(NodeID node_id,
                                 std::vector<Replica>& out) const;

    Status put_shard_route(const ShardRoute& route);
    Status get_shard_route(const ShardID& shard_id, ShardRouteOr& out) const;
    Status delete_shard_route(const ShardID& shard_id);
    Status del_shard_route_entry(const ShardID& shard_id,
                                 const ReplicaKey& replica_key);

    Status put_replica_group(const ReplicaGroup& group);
    Status get_replica_group(const ShardID& shard_id,
                             ReplicaGroupOr& out) const;
    Status delete_replica_group(const ShardID& shard_id);
    Status list_replica_groups(std::vector<ReplicaGroup>& out) const;

   private:
    explicit SdmStoreTxn(SdmStore* store);

    SdmStore* store_{nullptr};

    friend class SdmStore;
};

}  // namespace adviskv::sdm