#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

using ReplicaKey = ReplicaID;
using ShardKey = ShardID;

struct TableNameKey {
    std::string db_name;
    std::string table_name;

    bool operator==(const TableNameKey& other) const {
        return db_name == other.db_name and table_name == other.table_name;
    }

    DEFINE_OPERATOR_NOT_EQUAL(TableNameKey)
};

using ReplicaKeyHash = ReplicaIDHash;
using ShardKeyHash = ShardIDHash;

struct TableNameKeyHash {
    size_t operator()(const TableNameKey& key) const {
        size_t h1 = std::hash<std::string>{}(key.db_name);
        size_t h2 = std::hash<std::string>{}(key.table_name);
        return h1 ^ (h2 << 1);
    }
};

class SdmRuntimeIndex {
   public:
    virtual ~SdmRuntimeIndex() = default;

    virtual Status on_table_upsert(const Table* old_table,
                                   const Table& new_table);
    virtual Status on_node_upsert(const Node* old_node, const Node& new_node);
    virtual Status on_replica_upsert(const Replica* old_replica,
                                     const Replica& new_replica);

    virtual Status on_table_delete(const Table& table);
    virtual Status on_node_delete(const Node& node);
    virtual Status on_replica_delete(const Replica& replica);

    virtual Status find_table_by_name(const std::string& db_name,
                                      const std::string& table_name,
                                      TableID& table_id) const;

    virtual Status list_nodes_by_resource_pool(const std::string& pool_name,
                                               std::vector<NodeID>& out) const;

    virtual Status list_replicas_by_shard(const ShardID& shard_id,
                                          std::vector<ReplicaKey>& out) const;

    virtual Status list_replicas_by_node(const NodeID& node_id,
                                         std::vector<ReplicaKey>& out) const;

    virtual Status get_shard_route(const ShardID& shard_id,
                                   ShardRoutePtr& out) const;
    virtual Status put_shard_route(const ShardRoute& route);
    virtual Status delete_shard_route(const ShardID& shard_id);
    virtual Status del_shard_route_entry(const ShardID& shard_id,
                                         const ReplicaKey& replica_key);

   private:
    std::unordered_map<TableNameKey, TableID, TableNameKeyHash>
        table_name_index_;
    std::unordered_map<std::string, std::unordered_set<NodeID>>
        pool_nodes_index_;
    std::unordered_map<ShardKey, std::unordered_set<ReplicaKey, ReplicaKeyHash>,
                       ShardKeyHash>
        shard_replicas_index_;
    std::unordered_map<NodeID, std::unordered_set<ReplicaKey, ReplicaKeyHash>>
        node_replicas_index_;

    std::unordered_map<NodeID, std::string> node_pool_index_;

    std::unordered_map<ShardKey, ShardRoutePtr, ShardKeyHash>
        shard_route_cache_;
};
}  // namespace adviskv::sdm