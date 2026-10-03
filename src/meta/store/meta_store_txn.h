#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/model/model.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/store/meta_data.h"
#include "meta/store/meta_index.h"
#include "meta/store/meta_snapshot.h"

namespace adviskv::meta {

class MetaStoreTxn {
public:
    explicit MetaStoreTxn(std::shared_ptr<const MetaSnapshot> snapshot);

    bool durable_dirty() const;

    std::shared_ptr<const MetaSnapshot> finish() const;

    bool durable_cloned_for_test() const { return durable_.cloned(); }
    bool runtime_cloned_for_test() const { return runtime_.cloned(); }
    bool index_cloned_for_test() const { return index_.cloned(); }

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

    // 观测，只写 RuntimeData，不落盘。
    Status put_replica_observed(const ReplicaID& replica_key, const ReplicaObserved& observed);
    Status get_replica_observed(const ReplicaID& replica_key, ReplicaObservedOr& out) const;

    Status put_membership_observed(const ShardID& shard_id, const MembershipObserved& observed);
    Status get_membership_observed(const ShardID& shard_id, MembershipObservedOr& out) const;

    Status put_shard_route(const ShardRoute& route);
    Status get_shard_route(const ShardID& shard_id, ShardRouteOr& out) const;
    Status delete_shard_route(const ShardID& shard_id);

    Status put_replica_group(const ReplicaGroup& group);
    Status get_replica_group(const ShardID& shard_id, ReplicaGroupOr& out) const;
    Status delete_replica_group(const ShardID& shard_id);
    Status list_replica_groups(std::vector<ReplicaGroup>& out) const;

private:
    const DurableData& durable() const { return durable_.get(); }
    const RuntimeData& runtime() const { return runtime_.get(); }
    const MetaIndex& index() const { return index_.get(); }
    DurableData& mutable_durable() { return durable_.mutable_of(); }
    RuntimeData& mutable_runtime() { return runtime_.mutable_of(); }
    MetaIndex& mutable_index() { return index_.mutable_of(); }
    void mark_durable_dirty() { durable_dirty_ = true; }

    CowValue<DurableData> durable_;
    CowValue<RuntimeData> runtime_;
    CowValue<MetaIndex> index_;
    bool durable_dirty_{false};
};

}  // namespace adviskv::meta
