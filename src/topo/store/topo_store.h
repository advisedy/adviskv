#pragma once

#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <variant>

#include "common/model/type.h"
#include "common/status.h"
#include "topo/model/model.h"
#include "topo/store/i_topo_metastore.h"
#include "topo/store/runtime_index.h"
#include "topo/store/runtime_store.h"
#include "topo/store/topo_store_txn.h"

namespace adviskv::topo {

/*
这里暂时统一一下，关于get没有找到的情况，错误码返回的还是Status::OK，只不过指针是nullptr.
*/
struct MemoryMetaStoreParam {};

struct PersistentMetaStoreParam {
    std::string data_dir;
};

using MetaStoreParam = std::variant<MemoryMetaStoreParam, PersistentMetaStoreParam>;

class TopoStore {
public:
    explicit TopoStore(const MetaStoreParam& param);
    TopoStore(const MetaStoreParam& param, std::unique_ptr<TopoRuntimeIndex> runtime_index,
             std::unique_ptr<TopoRuntimeStore> runtime_store);

    Status init();

    Status read_with(const std::function<Status(const TopoStoreTxn&)>& fn) const;
    Status write_with(const std::function<Status(TopoStoreTxn&)>& fn);

private:
    TopoStore(std::unique_ptr<ITopoMetaStore> meta_store, std::unique_ptr<TopoRuntimeStore> runtime_store,
             std::unique_ptr<TopoRuntimeIndex> runtime_index);

    Status rebuild_runtime_index();
    Status maybe_repair_runtime_index(Status index_status);
    void mark_meta_dirty();
    void mark_runtime_store_dirty();
    void mark_runtime_index_dirty();

    Status put_table(const Table& table);
    Status get_table(TableID table_id, TableOr& out) const;
    Status get_table_by_name(const std::string& db_name, const std::string& table_name, TableOr& out) const;
    Status list_tables(std::vector<Table>& out) const;
    Status delete_table(TableID table_id);

    Status put_node(const Node& node);
    Status get_node(const NodeID& node_id, NodeOr& out) const;
    Status list_nodes(std::vector<Node>& out) const;
    Status list_nodes_by_resource_pool(const std::string& pool_name, std::vector<Node>& out) const;

    Status get_resource_pool(const std::string& name, ResourcePoolOr& out) const;
    Status list_resource_pools(std::vector<ResourcePool>& pools) const;

    Status get_replica(const ReplicaID& replica_key, ReplicaOr& out) const;
    Status put_replica(const Replica& replica);
    Status put_replicas(const std::vector<Replica>& replicas);
    Status delete_replica(const ReplicaID& replica_key);
    Status list_replicas(std::vector<Replica>& out) const;
    Status list_replicas_by_shard(const ShardID& shard_id, std::vector<Replica>& out) const;
    Status list_replicas_by_node(NodeID node_id, std::vector<Replica>& out) const;

    Status put_shard_route(const ShardRoute& route);
    Status get_shard_route(const ShardID& shard_id, ShardRouteOr& out) const;
    Status delete_shard_route(const ShardID& shard_id);
    Status del_shard_route_entry(const ShardID& shard_id, const ReplicaID& replica_id);

    Status put_replica_group(const ReplicaGroup& group);
    Status get_replica_group(const ShardID& shard_id, ReplicaGroupOr& out) const;
    Status delete_replica_group(const ShardID& shard_id);
    Status list_replica_groups(std::vector<ReplicaGroup>& out) const;

    mutable std::shared_mutex mutex_;
    std::unique_ptr<ITopoMetaStore> meta_store_;
    std::unique_ptr<TopoRuntimeStore> runtime_store_;
    std::unique_ptr<TopoRuntimeIndex> runtime_index_;  // 维护的是一个二级索引，方便检索
    bool meta_dirty_{false};
    bool runtime_store_dirty_{false};
    bool runtime_index_dirty_{false};

    friend class TopoStoreTxn;
};

}  // namespace adviskv::topo