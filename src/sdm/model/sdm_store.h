#pragma once

#include <memory>
#include <shared_mutex>
#include <string>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/i_sdm_metastore.h"
#include "sdm/model/sdm_runtime_index.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

/*
这里暂时统一一下，关于get没有找到的情况，错误码返回的还是Status::OK，只不过指针是nullptr.
*/
class SdmStore {
   public:
    explicit SdmStore(SdmMetaStoreType type,
                      const std::string& persistent_data_dir = "");

    Status put_table(const Table& table);
    Status get_table(TableID table_id, TableOr& out) const;
    Status get_table_by_name(const std::string& db_name,
                             const std::string& table_name,
                             TableOr& out) const;
    Status list_tables(std::vector<Table>& out) const;

    Status list_nodes_by_resource_pool(const std::string& pool_name,
                                       std::vector<Node>& out) const;
    Status list_nodes(std::vector<Node>& out) const;

    Status get_shard_route(const ShardID& shard_id,
                           ShardRouteOr& out) const;
    Status put_shard_route(const ShardRoute& route);
    Status delete_shard_route(const ShardID& shard_id);
    Status del_shard_route_entry(const ShardID& shard_id,
                                 const ReplicaKey& replica_key);

    Status put_node(const Node& node);
    Status get_node(const NodeID& node_id, NodeOr& out) const;

    Status get_resource_pool(const std::string& name,
                             ResourcePoolOr& out) const;

    Status list_resource_pools(std::vector<ResourcePool>& pools) const;

    Status get_replica(const ReplicaID& replica_key, ReplicaOr& out) const;
    Status put_replica(const Replica& replica);
    Status del_replica(const ReplicaID& replica_key);

    // Status get_table_shard(TableID table_id, ShardID shard_id) const;
    Status list_replicas_by_shard(const ShardID& shard_id,
                                  std::vector<Replica>& out) const;

    Status list_replicas_by_node(NodeID node_id,
                                 std::vector<Replica>& out) const;
    Status delete_table(TableID table_id);

   private:
    Status rebuild_runtime_index();

    mutable std::shared_mutex mutex_;
    std::unique_ptr<ISdmMetaStore> meta_store_;
    SdmRuntimeIndex runtime_index_;
};

}  // namespace adviskv::sdm
