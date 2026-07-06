#pragma once

#include <string>
#include <vector>

#include "common/status.h"
#include "common/model/type.h"
#include "sdm/store/sdm_store.h"
#include "sdm/store/sdm_store_txn.h"
#include "sdm/model/model.h"

namespace adviskv::sdm::store_test {

inline Status put_table(SdmStore& store, const Table& table) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.put_table(table);
    });
}

inline Status get_table(const SdmStore& store, TableID table_id, TableOr& out) {
    return store.read_with(
        [&](const SdmStoreTxn& txn) { return txn.get_table(table_id, out); });
}

inline Status get_table_by_name(const SdmStore& store,
                                const std::string& db_name,
                                const std::string& table_name, TableOr& out) {
    return store.read_with([&](const SdmStoreTxn& txn) {
        return txn.get_table_by_name(db_name, table_name, out);
    });
}

inline Status delete_table(SdmStore& store, TableID table_id) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.delete_table(table_id);
    });
}

inline Status put_node(SdmStore& store, const Node& node) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.put_node(node);
    });
}

inline Status get_node(const SdmStore& store, const NodeID& node_id,
                       NodeOr& out) {
    return store.read_with(
        [&](const SdmStoreTxn& txn) { return txn.get_node(node_id, out); });
}

inline Status list_nodes_by_resource_pool(const SdmStore& store,
                                          const std::string& pool_name,
                                          std::vector<Node>& out) {
    return store.read_with([&](const SdmStoreTxn& txn) {
        return txn.list_nodes_by_resource_pool(pool_name, out);
    });
}

inline Status put_replica(SdmStore& store, const Replica& replica) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.put_replica(replica);
    });
}

inline Status put_replicas(SdmStore& store,
                           const std::vector<Replica>& replicas) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.put_replicas(replicas);
    });
}

inline Status get_replica(const SdmStore& store, const ReplicaID& replica_id,
                          ReplicaOr& out) {
    return store.read_with([&](const SdmStoreTxn& txn) {
        return txn.get_replica(replica_id, out);
    });
}

inline Status delete_replica(SdmStore& store, const ReplicaID& replica_id) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.delete_replica(replica_id);
    });
}

inline Status list_replicas_by_shard(const SdmStore& store,
                                     const ShardID& shard_id,
                                     std::vector<Replica>& out) {
    return store.read_with([&](const SdmStoreTxn& txn) {
        return txn.list_replicas_by_shard(shard_id, out);
    });
}

inline Status list_replicas_by_node(const SdmStore& store,
                                    const NodeID& node_id,
                                    std::vector<Replica>& out) {
    return store.read_with([&](const SdmStoreTxn& txn) {
        return txn.list_replicas_by_node(node_id, out);
    });
}

inline Status put_shard_route(SdmStore& store, const ShardRoute& route) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.put_shard_route(route);
    });
}

inline Status get_shard_route(const SdmStore& store, const ShardID& shard_id,
                              ShardRouteOr& out) {
    return store.read_with([&](const SdmStoreTxn& txn) {
        return txn.get_shard_route(shard_id, out);
    });
}

inline Status delete_shard_route(SdmStore& store, const ShardID& shard_id) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.delete_shard_route(shard_id);
    });
}

inline Status put_replica_group(SdmStore& store, const ReplicaGroup& group) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.put_replica_group(group);
    });
}

inline Status get_replica_group(const SdmStore& store, const ShardID& shard_id,
                                ReplicaGroupOr& out) {
    return store.read_with([&](const SdmStoreTxn& txn) {
        return txn.get_replica_group(shard_id, out);
    });
}

inline Status delete_replica_group(SdmStore& store, const ShardID& shard_id) {
    return store.write_with([&](SdmStoreTxn& txn) {
        return txn.delete_replica_group(shard_id);
    });
}

}  // namespace adviskv::sdm::store_test
