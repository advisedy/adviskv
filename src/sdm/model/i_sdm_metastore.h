#pragma once
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"
#include <unordered_map>

namespace adviskv::sdm {


enum class SdmMetaStoreType{
    MEMORY = 0,
};

class ISdmMetaStore {
   public:
    virtual ~ISdmMetaStore() = default;

    virtual Status upsert_table(const Table& table) = 0; // update or insert
    virtual Status get_table(TableID table_id, TablePtr& out) const = 0;
    virtual Status list_tables(std::vector<TablePtr>& out) const = 0;

    virtual Status upsert_node(const Node& node) = 0;
    virtual Status get_node(const NodeID& node_id, NodePtr& out) const = 0;
    virtual Status list_nodes(std::vector<NodePtr>& out) const = 0;

    virtual Status upsert_replica(const Replica& replica) = 0;
    virtual Status get_replica(const ReplicaID& key,
                               ReplicaPtr& out) const = 0;
    virtual Status delete_replica(const ReplicaID& key) = 0;
    virtual Status list_replicas(std::vector<ReplicaPtr>& out) const = 0;

    virtual Status upsert_resource_pool(const ResourcePool& pool) = 0;
    virtual Status get_resource_pool(const std::string& name,
                                     ResourcePoolPtr& out) const = 0;
    virtual Status list_resource_pools(
        std::vector<ResourcePoolPtr>& out) const = 0;
};

class MemoryMetaStore : public ISdmMetaStore {
   public:
    Status upsert_table(const Table& table) override;
    Status get_table(TableID table_id, TablePtr& out) const override;
    Status list_tables(std::vector<TablePtr>& out) const override;

    Status upsert_node(const Node& node) override;
    Status get_node(const NodeID& node_id, NodePtr& out) const override;
    Status list_nodes(std::vector<NodePtr>& out) const override;

    Status upsert_replica(const Replica& replica) override;
    Status get_replica(const ReplicaID& key, ReplicaPtr& out) const override;
    Status delete_replica(const ReplicaID& key) override;
    Status list_replicas(std::vector<ReplicaPtr>& out) const override;

    Status upsert_resource_pool(const ResourcePool& pool) override;
    Status get_resource_pool(const std::string& name,
                             ResourcePoolPtr& out) const override;
    Status list_resource_pools(std::vector<ResourcePoolPtr>& out) const override;

private:
    std::unordered_map<TableID, TablePtr> tables_;
    std::unordered_map<NodeID, NodePtr> nodes_;
    std::unordered_map<ReplicaID, ReplicaPtr, ReplicaIDHash> replicas_;
    std::unordered_map<std::string, ResourcePoolPtr> resource_pools_;
};

}  // namespace adviskv::sdm
