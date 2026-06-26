#pragma once

#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/i_sdm_metastore.h"
#include "sdm/model/sdm_runtime_index.h"
#include "sdm/model/sdm_store_txn.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

/*
这里暂时统一一下，关于get没有找到的情况，错误码返回的还是Status::OK，只不过指针是nullptr.
*/

// TODO111 感觉这个SdmStore和metastore的内容有点混合，职责有一点不清楚了。
class SdmStore {
   public:
    explicit SdmStore(SdmMetaStoreType type,
                      const std::string& persistent_data_dir = "");
    SdmStore(SdmMetaStoreType type, std::unique_ptr<SdmRuntimeIndex> index,
             const std::string& persistent_data_dir = "");

    Status read_with(const std::function<Status(const SdmStoreTxn&)>& fn) const;
    Status write_with(const std::function<Status(SdmStoreTxn&)>& fn);

   private:
    SdmStore(std::unique_ptr<ISdmMetaStore> meta_store,
             std::unique_ptr<SdmRuntimeIndex> runtime_index);

    Status rebuild_runtime_index();
    Status maybe_repair_runtime_index(Status index_status);

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

    Status get_replica(const ReplicaID& replica_key, ReplicaOr& out) const;
    Status put_replica(const Replica& replica);
    Status put_replicas(const std::vector<Replica>& replicas);
    Status del_replica(const ReplicaID& replica_key);
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

    mutable std::shared_mutex mutex_;
    std::unique_ptr<ISdmMetaStore> meta_store_;
    std::unique_ptr<SdmRuntimeIndex> runtime_index_;

    friend class SdmStoreTxn;
};

}  // namespace adviskv::sdm