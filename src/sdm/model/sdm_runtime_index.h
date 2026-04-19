#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

using ReplicaKey = ReplicaID;

struct ShardKey {
    TableID table_id;
    ShardID shard_id;

    bool operator==(const ShardKey& other) const {
        return table_id == other.table_id and shard_id == other.shard_id;
    }
};

struct TableNameKey {
    std::string db_name;
    std::string table_name;

    bool operator==(const TableNameKey& other) const {
        return db_name == other.db_name and table_name == other.table_name;
    }
};

using ReplicaKeyHash = ReplicaIDHash;

struct ShardKeyHash {
    size_t operator()(const ShardKey& key) const {
        size_t h1 = std::hash<TableID>{}(key.table_id);
        size_t h2 = std::hash<ShardID>{}(key.shard_id);
        return h1 ^ (h2 << 1);
    }
};

struct TableNameKeyHash {
    size_t operator()(const TableNameKey& key) const {
        size_t h1 = std::hash<std::string>{}(key.db_name);
        size_t h2 = std::hash<std::string>{}(key.table_name);
        return h1 ^ (h2 << 1);
    }
};

/*

Status on_table_upsert(const Table& table);
Status on_node_upsert(const Node& node);
Status on_replica_upsert(const Replica& replica);
Status on_replica_delete(const ReplicaKey& key);
*/

class SdmRuntimeIndex {
   public:
    Status on_table_upsert(const Table* old_table, const Table& new_table);
    Status on_node_upsert(const Node* old_node, const Node& new_node);
    Status on_replica_upsert(const Replica* old_replica,
                             const Replica& new_replica);

    Status on_table_delete(const Table& table);
    Status on_node_delete(const Node& node);
    Status on_replica_delete(const Replica& replica);

    Status find_table_by_name(const std::string& db_name,
                              const std::string& table_name,
                              TableID& table_id) const;

    Status list_nodes_by_resource_pool(const std::string& pool_name,
                                       std::vector<NodeID>& out) const;

    Status list_replicas_by_shard(TableID table_id, ShardID shard_id,
                                  std::vector<ReplicaKey>& out) const;

    Status list_replicas_by_node(const NodeID& node_id,
                                 std::vector<ReplicaKey>& out) const;

    Status get_shard_route(TableID table_id, ShardID shard_id,
                           ShardRoutePtr& out) const;
    Status put_shard_route(const ShardRoute& route);
    Status del_shard_route_entry(TableID table_id, ShardID shard_id,
                                 const ReplicaKey& replica_key);

   private:
    std::unordered_map<TableNameKey, TableID, TableNameKeyHash>
        table_name_index_;
    std::unordered_map<std::string, std::unordered_set<NodeID>>
        pool_nodes_index_;
    std::unordered_map<
        ShardKey,
        std::unordered_set<ReplicaKey, ReplicaKeyHash>,
        ShardKeyHash>
        shard_replicas_index_;
    std::unordered_map<
        NodeID, std::unordered_set<ReplicaKey, ReplicaKeyHash>>
        node_replicas_index_;

    std::unordered_map<NodeID, std::string> node_pool_index_;

    std::unordered_map<ShardKey, ShardRoutePtr, ShardKeyHash>
        shard_route_cache_;
};
}  // namespace adviskv::sdm
