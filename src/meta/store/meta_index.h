#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/define.h"
#include "common/model/model.h"
#include "common/status.h"
#include "meta/model/model.h"

namespace adviskv::meta {

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

class MetaIndex {
public:
    MetaIndex() = default;

    void clear();

    void on_table_upsert(const Table* old_table, const Table& new_table);
    void on_node_upsert(const Node* old_node, const Node& new_node);
    void on_replica_upsert(const Replica* old_replica, const Replica& new_replica);
    void on_db_upsert(const Database* old_db, const Database& new_db);

    void on_table_delete(const Table& table);
    void on_replica_delete(const Replica& replica);
    void on_db_delete(const Database& db);

    Status find_table_by_name(const std::string& db_name, const std::string& table_name, TableID& table_id) const;
    Status find_db_by_name(const std::string& name, DatabaseID& db_id) const;

    Status list_nodes_by_resource_pool(const std::string& pool_name, std::vector<NodeID>& out) const;

    Status list_replicas_by_shard(const ShardID& shard_id, std::vector<ReplicaKey>& out) const;

    Status list_replicas_by_node(const NodeID& node_id, std::vector<ReplicaKey>& out) const;

private:
    std::unordered_map<TableNameKey, TableID, TableNameKeyHash> table_name_index_;
    std::unordered_map<std::string, DatabaseID> db_name_index_;
    std::unordered_map<std::string, std::unordered_set<NodeID>> pool_nodes_index_;
    std::unordered_map<ShardKey, std::unordered_set<ReplicaKey, ReplicaKeyHash>, ShardKeyHash> shard_replicas_index_;
    std::unordered_map<NodeID, std::unordered_set<ReplicaKey, ReplicaKeyHash>> node_replicas_index_;
};
}  // namespace adviskv::meta
