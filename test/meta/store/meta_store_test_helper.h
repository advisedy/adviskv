#pragma once

#include <string>
#include <vector>

#include "common/status.h"
#include "common/model/model.h"
#include "meta/model/model.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta::store_test {

inline Status put_table(MetaStore& store, const Table& table) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.put_table(table);
    });
}

inline Status get_table(const MetaStore& store, TableID table_id, TableOr& out) {
    return store.read_with(
        [&](const MetaStoreTxn& txn) { return txn.get_table(table_id, out); });
}

inline Status get_table_by_name(const MetaStore& store,
                                const std::string& db_name,
                                const std::string& table_name, TableOr& out) {
    return store.read_with([&](const MetaStoreTxn& txn) {
        return txn.get_table_by_name(db_name, table_name, out);
    });
}

inline Status delete_table(MetaStore& store, TableID table_id) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.delete_table(table_id);
    });
}

inline Status put_node(MetaStore& store, const Node& node) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.put_node(node);
    });
}

inline Status get_node(const MetaStore& store, const NodeID& node_id,
                       NodeOr& out) {
    return store.read_with(
        [&](const MetaStoreTxn& txn) { return txn.get_node(node_id, out); });
}

inline Status list_nodes_by_resource_pool(const MetaStore& store,
                                          const std::string& pool_name,
                                          std::vector<Node>& out) {
    return store.read_with([&](const MetaStoreTxn& txn) {
        return txn.list_nodes_by_resource_pool(pool_name, out);
    });
}

inline Status put_replica(MetaStore& store, const Replica& replica) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.put_replica(replica);
    });
}

inline Status put_replicas(MetaStore& store,
                           const std::vector<Replica>& replicas) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.put_replicas(replicas);
    });
}

inline Status get_replica(const MetaStore& store, const ReplicaID& replica_id,
                          ReplicaOr& out) {
    return store.read_with([&](const MetaStoreTxn& txn) {
        return txn.get_replica(replica_id, out);
    });
}

inline Status put_replica_observed(MetaStore& store, const ReplicaID& replica_id,
                                   const ReplicaObserved& observed) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.put_replica_observed(replica_id, observed);
    });
}

inline Status get_replica_observed(const MetaStore& store,
                                   const ReplicaID& replica_id,
                                   ReplicaObservedOr& out) {
    return store.read_with([&](const MetaStoreTxn& txn) {
        return txn.get_replica_observed(replica_id, out);
    });
}

inline Status put_membership_observed(MetaStore& store, const ShardID& shard_id,
                                      const MembershipObserved& observed) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.put_membership_observed(shard_id, observed);
    });
}

inline Status get_membership_observed(const MetaStore& store, const ShardID& shard_id,
                                      MembershipObservedOr& out) {
    return store.read_with([&](const MetaStoreTxn& txn) {
        return txn.get_membership_observed(shard_id, out);
    });
}

inline Status delete_replica(MetaStore& store, const ReplicaID& replica_id) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.delete_replica(replica_id);
    });
}

inline Status list_replicas_by_shard(const MetaStore& store,
                                     const ShardID& shard_id,
                                     std::vector<Replica>& out) {
    return store.read_with([&](const MetaStoreTxn& txn) {
        return txn.list_replicas_by_shard(shard_id, out);
    });
}

inline Status list_replicas_by_node(const MetaStore& store,
                                    const NodeID& node_id,
                                    std::vector<Replica>& out) {
    return store.read_with([&](const MetaStoreTxn& txn) {
        return txn.list_replicas_by_node(node_id, out);
    });
}

inline Status put_shard_route(MetaStore& store, const ShardRoute& route) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.put_shard_route(route);
    });
}

inline Status get_shard_route(const MetaStore& store, const ShardID& shard_id,
                              ShardRouteOr& out) {
    return store.read_with([&](const MetaStoreTxn& txn) {
        return txn.get_shard_route(shard_id, out);
    });
}

inline Status delete_shard_route(MetaStore& store, const ShardID& shard_id) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.delete_shard_route(shard_id);
    });
}

inline Status put_replica_group(MetaStore& store, const ReplicaGroup& group) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.put_replica_group(group);
    });
}

inline Status get_replica_group(const MetaStore& store, const ShardID& shard_id,
                                ReplicaGroupOr& out) {
    return store.read_with([&](const MetaStoreTxn& txn) {
        return txn.get_replica_group(shard_id, out);
    });
}

inline Status delete_replica_group(MetaStore& store, const ShardID& shard_id) {
    return store.write_with([&](MetaStoreTxn& txn) {
        return txn.delete_replica_group(shard_id);
    });
}

}  // namespace adviskv::meta::store_test
