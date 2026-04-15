#pragma once

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/route_model.h"
#include "sdm/model/store.h"
#include <memory>
namespace adviskv::sdm{



class SdmStore{

public:
    Status put_table(const Table& table);
    Status get_table(TableID table_id, std::shared_ptr<Table>& out) const;
    Status get_table_by_name(const std::string& db_name,
                            const std::string& table_name,
                            std::shared_ptr<Table>& out) const;
    Status list_tables(std::vector<std::shared_ptr<Table>>& out) const;

    Status get_shard_route(TableID table_id, ShardID shard_id, std::shared_ptr<ShardRoute>& out) const;

    Status put_node(const Node& node);
    Status get_node(const NodeID& node_id, NodePtr& out) const;

    Status get_resource_pool(const std::string& name, std::shared_ptr<ResourcePool>& out) const;

    Status list_resource_pools(std::vector<std::shared_ptr<ResourcePool>>& pools) const;

    Status get_replica(const ReplicaKey& replica_key, ReplicaPtr& out) const;

};

}