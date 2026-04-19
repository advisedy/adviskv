#pragma once
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {


enum class SdmMetaStoreType{
    DEFAULT = 0,
};

class ISdmMetaStore {
   public:
    virtual ~ISdmMetaStore() = default;

    virtual Status upsert_table(const Table& table) = 0;
    virtual Status get_table(TableID table_id, TablePtr& out) const = 0;
    virtual Status list_tables(std::vector<TablePtr>& out) const = 0;

    virtual Status upsert_node(const Node& node) = 0;
    virtual Status get_node(const NodeID& node_id, NodePtr& out) const = 0;
    virtual Status list_nodes(std::vector<NodePtr>& out) const = 0;

    virtual Status upsert_replica(const Replica& replica) = 0;
    virtual Status get_replica(const ReplicaKey& key,
                               ReplicaPtr& out) const = 0;
    virtual Status delete_replica(const ReplicaKey& key) = 0;
    virtual Status list_replicas(std::vector<ReplicaPtr>& out) const = 0;

    virtual Status upsert_resource_pool(const ResourcePool& pool) = 0;
    virtual Status get_resource_pool(const std::string& name,
                                     ResourcePoolPtr& out) const = 0;
    virtual Status list_resource_pools(
        std::vector<ResourcePoolPtr>& out) const = 0;
};

class DefaultMetaStore : public ISdmMetaStore {};

}  // namespace adviskv::sdm
