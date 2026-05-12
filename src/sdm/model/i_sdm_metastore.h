#pragma once
#include <filesystem>
#include <unordered_map>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"
#include "sdm/persist/sdm_persist_engine.h"

namespace adviskv::sdm {

enum class SdmMetaStoreType {
    MEMORY = 0,
    PERSISTENT = 1,
};

//这边依旧搞了个宏定义， 只能说这种时候用起来很舒服

#define ISDM_METASTORE_METHODS(X)                                                 \
    X(Status upsert_table(const Table& table))                                    \
    X(Status get_table(TableID table_id, TablePtr& out) const)                    \
    X(Status delete_table(TableID table_id))                                      \
    X(Status list_tables(std::vector<TablePtr>& out) const)                       \
    X(Status list_tables_by_lifecycle(TableLifecycle lifecycle,                   \
                                      std::vector<TablePtr>& out) const)          \
    X(Status upsert_node(const Node& node))                                       \
    X(Status get_node(const NodeID& node_id, NodePtr& out) const)                 \
    X(Status list_nodes(std::vector<NodePtr>& out) const)                         \
    X(Status upsert_replica(const Replica& replica))                              \
    X(Status get_replica(const ReplicaID& key, ReplicaPtr& out) const)            \
    X(Status delete_replica(const ReplicaID& key))                                \
    X(Status list_replicas(std::vector<ReplicaPtr>& out) const)                   \
    X(Status upsert_resource_pool(const ResourcePool& pool))                      \
    X(Status get_resource_pool(const std::string& name,                           \
                               ResourcePoolPtr& out) const)                       \
    X(Status list_resource_pools(std::vector<ResourcePoolPtr>& out) const)        \
    X(Status delete_resource_pool(const std::string& name))                       \
    X(Status upsert_shard_route(const ShardRoute& route))                         \
    X(Status get_shard_route(const ShardID& shard_id,                             \
                             ShardRoutePtr& out) const)                           \
    X(Status delete_shard_route(const ShardID& shard_id))                         \
    X(Status list_shard_routes(std::vector<ShardRoutePtr>& out) const)





class ISdmMetaStore {
   public:
    virtual ~ISdmMetaStore() = default;

#define X(...) virtual __VA_ARGS__ = 0;
    ISDM_METASTORE_METHODS(X)
#undef X
};

class MemoryMetaStore : public ISdmMetaStore {
   public:
#define X(...) __VA_ARGS__ override;
    ISDM_METASTORE_METHODS(X)
#undef X

   protected:
    std::unordered_map<TableID, TablePtr> tables_;
    std::unordered_map<NodeID, NodePtr> nodes_;
    std::unordered_map<ReplicaID, ReplicaPtr, ReplicaIDHash> replicas_;
    std::unordered_map<std::string, ResourcePoolPtr> resource_pools_;
    std::unordered_map<ShardID, ShardRoutePtr, ShardIDHash> shard_routes_;
};

class PersistentMetaStore : public ISdmMetaStore {
   public:
    explicit PersistentMetaStore(std::filesystem::path data_dir);
    PersistentMetaStore(std::unique_ptr<ISdmMetaStore> inner,
                        std::filesystem::path data_dir);

#define X(...) __VA_ARGS__ override;
    ISDM_METASTORE_METHODS(X)
#undef X

   private:
    Status load();
    Status persist();

    std::unique_ptr<ISdmMetaStore> inner_;
    SdmPersistEngine persist_engine_;
};

#undef ISDM_METASTORE_METHODS
}  // namespace adviskv::sdm
