#pragma once

#include <string>
#include <vector>

#include "common/model/type.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/store/meta_data.h"
#include "meta/store/meta_index.h"

namespace adviskv::meta {

class MetaStoreTxn {
public:
    explicit MetaStoreTxn(const MetaWorkingSet* ws);
    explicit MetaStoreTxn(MetaWorkingSet* ws);

    bool durable_dirty() const;

    Status put_database(const Database& db);
    Status get_database(DatabaseID db_id, DatabaseOr& out) const;
    Status get_database_by_name(const std::string& name, DatabaseOr& out) const;
    Status delete_database(DatabaseID db_id);
    DatabaseID allocate_db_id();
    TableID allocate_table_id();

    Status put_table(const Table& table);
    Status get_table(TableID table_id, TableOr& out) const;
    Status get_table_by_name(const std::string& db_name, const std::string& table_name, TableOr& out) const;
    Status list_tables(std::vector<Table>& out) const;
    Status delete_table(TableID table_id);

    Status put_node(const Node& node);
    Status get_node(const NodeID& node_id, NodeOr& out) const;
    Status list_nodes(std::vector<Node>& out) const;
    Status list_nodes_by_resource_pool(const std::string& pool_name, std::vector<Node>& out) const;

    Status put_replica(const Replica& replica);
    Status put_replicas(const std::vector<Replica>& replicas);
    Status get_replica(const ReplicaID& replica_key, ReplicaOr& out) const;
    Status delete_replica(const ReplicaID& replica_key);
    Status list_replicas(std::vector<Replica>& out) const;
    Status list_replicas_by_shard(const ShardID& shard_id, std::vector<Replica>& out) const;
    Status list_replicas_by_node(NodeID node_id, std::vector<Replica>& out) const;

    // 只写 RuntimeData，不落盘。
    Status put_replica_observed(const ReplicaID& replica_key, const ReplicaObserved& observed);
    Status get_replica_observed(const ReplicaID& replica_key, ReplicaObservedOr& out) const;

    Status put_shard_route(const ShardRoute& route);
    Status get_shard_route(const ShardID& shard_id, ShardRouteOr& out) const;
    Status delete_shard_route(const ShardID& shard_id);

    Status put_replica_group(const ReplicaGroup& group);
    Status get_replica_group(const ShardID& shard_id, ReplicaGroupOr& out) const;
    Status delete_replica_group(const ShardID& shard_id);
    Status list_replica_groups(std::vector<ReplicaGroup>& out) const;

private:
    const DurableData& durable() const { return ws_->durable; }
    const RuntimeData& runtime() const { return ws_->runtime; }
    const MetaIndex& index() const { return ws_->index; }
    DurableData& mutable_durable() { return mutable_ws_->durable; }
    RuntimeData& mutable_runtime() { return mutable_ws_->runtime; }
    MetaIndex& mutable_index() { return mutable_ws_->index; }
    void mark_durable_dirty() { durable_dirty_ = true; }

    const MetaWorkingSet* ws_{nullptr};
    MetaWorkingSet* mutable_ws_{nullptr};
    bool durable_dirty_{false};
};

}  // namespace adviskv::meta
