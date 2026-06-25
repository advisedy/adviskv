#include "sdm/model/sdm_store_txn.h"

#include "sdm/model/sdm_store.h"

namespace adviskv::sdm {

SdmStoreTxn::SdmStoreTxn(SdmStore* store) : store_(store) {}

Status SdmStoreTxn::put_table(const Table& table) {
    return store_->put_table_unlocked(table);
}
Status SdmStoreTxn::get_table(TableID table_id, TableOr& out) const {
    return store_->get_table_unlocked(table_id, out);
}
Status SdmStoreTxn::get_table_by_name(const std::string& db_name,
                                      const std::string& table_name,
                                      TableOr& out) const {
    return store_->get_table_by_name_unlocked(db_name, table_name, out);
}
Status SdmStoreTxn::list_tables(std::vector<Table>& out) const {
    return store_->list_tables_unlocked(out);
}
Status SdmStoreTxn::delete_table(TableID table_id) {
    return store_->delete_table_unlocked(table_id);
}

Status SdmStoreTxn::put_node(const Node& node) {
    return store_->put_node_unlocked(node);
}
Status SdmStoreTxn::get_node(const NodeID& node_id, NodeOr& out) const {
    return store_->get_node_unlocked(node_id, out);
}
Status SdmStoreTxn::list_nodes(std::vector<Node>& out) const {
    return store_->list_nodes_unlocked(out);
}
Status SdmStoreTxn::list_nodes_by_resource_pool(const std::string& pool_name,
                                                std::vector<Node>& out) const {
    return store_->list_nodes_by_resource_pool_unlocked(pool_name, out);
}

Status SdmStoreTxn::get_resource_pool(const std::string& name,
                                      ResourcePoolOr& out) const {
    return store_->get_resource_pool_unlocked(name, out);
}
Status SdmStoreTxn::list_resource_pools(
    std::vector<ResourcePool>& pools) const {
    return store_->list_resource_pools_unlocked(pools);
}

Status SdmStoreTxn::put_replica(const Replica& replica) {
    return store_->put_replica_unlocked(replica);
}
Status SdmStoreTxn::put_replicas(const std::vector<Replica>& replicas) {
    return store_->put_replicas_unlocked(replicas);
}
Status SdmStoreTxn::get_replica(const ReplicaID& replica_key,
                                ReplicaOr& out) const {
    return store_->get_replica_unlocked(replica_key, out);
}
Status SdmStoreTxn::del_replica(const ReplicaID& replica_key) {
    return store_->del_replica_unlocked(replica_key);
}
Status SdmStoreTxn::list_replicas_by_shard(const ShardID& shard_id,
                                           std::vector<Replica>& out) const {
    return store_->list_replicas_by_shard_unlocked(shard_id, out);
}
Status SdmStoreTxn::list_replicas_by_node(NodeID node_id,
                                          std::vector<Replica>& out) const {
    return store_->list_replicas_by_node_unlocked(node_id, out);
}

Status SdmStoreTxn::put_shard_route(const ShardRoute& route) {
    return store_->put_shard_route_unlocked(route);
}
Status SdmStoreTxn::get_shard_route(const ShardID& shard_id,
                                    ShardRouteOr& out) const {
    return store_->get_shard_route_unlocked(shard_id, out);
}
Status SdmStoreTxn::delete_shard_route(const ShardID& shard_id) {
    return store_->delete_shard_route_unlocked(shard_id);
}
Status SdmStoreTxn::del_shard_route_entry(const ShardID& shard_id,
                                          const ReplicaKey& replica_key) {
    return store_->del_shard_route_entry_unlocked(shard_id, replica_key);
}

Status SdmStoreTxn::put_replica_group(const ReplicaGroup& group) {
    return store_->put_replica_group_unlocked(group);
}
Status SdmStoreTxn::get_replica_group(const ShardID& shard_id,
                                      ReplicaGroupOr& out) const {
    return store_->get_replica_group_unlocked(shard_id, out);
}
Status SdmStoreTxn::delete_replica_group(const ShardID& shard_id) {
    return store_->delete_replica_group_unlocked(shard_id);
}
Status SdmStoreTxn::list_replica_groups(std::vector<ReplicaGroup>& out) const {
    return store_->list_replica_groups_unlocked(out);
}

}  // namespace adviskv::sdm