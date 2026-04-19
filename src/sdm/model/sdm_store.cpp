#include "sdm/model/sdm_store.h"

#include "common/status.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

Status SdmStore::put_table(const Table& table) { return Status::OK(); }
Status SdmStore::get_table(TableID table_id,
                           std::shared_ptr<Table>& out) const {
    return Status::OK();
}
Status SdmStore::get_table_by_name(const std::string& db_name,
                                   const std::string& table_name,
                                   std::shared_ptr<Table>& out) const {
    return Status::OK();
}
Status SdmStore::list_tables(std::vector<std::shared_ptr<Table>>& out) const {
    return Status::OK();
}

Status SdmStore::get_shard_route(TableID table_id, ShardID shard_id,
                                 std::shared_ptr<ShardRoute>& out) const {
    return Status::OK();
}
Status SdmStore::put_shard_route(const ShardRoute& route) {
    return Status::OK();
}
Status SdmStore::del_shard_route_entry(TableID table_id, ShardID shard_id,
                                       const ReplicaKey& replica_key) {
    return Status::OK();
}

Status SdmStore::put_node(const Node& node) { return Status::OK(); }
Status SdmStore::get_node(const NodeID& node_id, NodePtr& out) const {
    return Status::OK();
}

Status SdmStore::get_resource_pool(const std::string& name,
                                   std::shared_ptr<ResourcePool>& out) const {
    return Status::OK();
}

Status SdmStore::list_resource_pools(
    std::vector<std::shared_ptr<ResourcePool>>& pools) const {
    return Status::OK();
}

Status SdmStore::get_replica(const ReplicaKey& replica_key,
                             ReplicaPtr& out) const {
    return Status::OK();
}
Status SdmStore::put_replica(const Replica& replica) { return Status::OK(); }
Status SdmStore::del_replica(const ReplicaKey& replica_key) {
    return Status::OK();
}

// Status get_table_shard(TableID table_id, ShardID shard_id) const;
Status SdmStore::list_replicas_by_shard(TableID table_id, ShardID shard_id,
                                        std::vector<ReplicaPtr>& out) const {
    return Status::OK();
}

Status SdmStore::list_replicas_by_node(NodeID node_id,
                                       std::vector<ReplicaPtr>& out) const {
    return Status::OK();
}
}  // namespace adviskv::sdm
