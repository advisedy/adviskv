#pragma once
#include <memory>
#include <vector>

#include "common/status.h"
#include "common/model/type.h"
#include "sdm/model/model.h"

namespace adviskv::sdm {

enum class SdmMetaStoreType {
    MEMORY = 0,
    PERSISTENT = 1,
};

// 这边依旧搞了个宏定义， 只能说这种时候用起来很舒服

#define ISDM_METASTORE_METHODS(X)                                             \
    X(Status upsert_table(const Table& table))                                \
    X(Status get_table(TableID table_id, TablePtr& out) const)                \
    X(Status delete_table(TableID table_id))                                  \
    X(Status list_tables(std::vector<TablePtr>& out) const)                   \
    X(Status upsert_replica(const Replica& replica))                          \
    X(Status get_replica(const ReplicaID& key, ReplicaPtr& out) const)        \
    X(Status delete_replica(const ReplicaID& key))                            \
    X(Status upsert_replicas(const std::vector<Replica>& replicas))           \
    X(Status list_replicas(std::vector<ReplicaPtr>& out) const)               \
    X(Status upsert_resource_pool(const ResourcePool& pool))                  \
    X(Status get_resource_pool(const std::string& name, ResourcePoolPtr& out) \
          const)                                                              \
    X(Status list_resource_pools(std::vector<ResourcePoolPtr>& out) const)    \
    X(Status delete_resource_pool(const std::string& name))                   \
    X(Status upsert_replica_group(const ReplicaGroup& group))                 \
    X(Status delete_replica_group(const ShardID& shard_id))                   \
    X(Status get_replica_group(const ShardID& shard_id, ReplicaGroupPtr& out) \
          const)                                                              \
    X(Status list_replica_groups(std::vector<ReplicaGroupPtr>& out) const)    \
    X(std::unique_ptr<ISdmMetaStore> clone_memory_snapshot()                  \
          const) /*内存数据快照，用来做恢复处理的*/                           \
    X(Status commit_memory_snapshot(                                          \
        std::unique_ptr<ISdmMetaStore> next_memory_store))

class ISdmMetaStore {
   public:
    virtual ~ISdmMetaStore() = default;
    virtual Status init() = 0;

#define X(...) virtual __VA_ARGS__ = 0;
    ISDM_METASTORE_METHODS(X)
#undef X
};

}  // namespace adviskv::sdm
