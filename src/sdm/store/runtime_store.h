#pragma once

#include <memory>
#include <vector>
#include <unordered_map>

#include "common/model/type.h"
#include "common/status.h"
#include "sdm/model/model.h"

namespace adviskv::sdm {

#define ISDM_RUNTIME_STORE_METHODS(X)                                                     \
    X(Status upsert_node(const Node& node))                                               \
    X(Status get_node(const NodeID& node_id, NodePtr& out) const)                         \
    X(Status list_nodes(std::vector<NodePtr>& out) const)                                 \
    X(Status put_shard_route(const ShardRoute& route))                                    \
    X(Status get_shard_route(const ShardID& shard_id, ShardRoutePtr& out) const)          \
    X(Status delete_shard_route(const ShardID& shard_id))                                 \
    X(Status del_shard_route_entry(const ShardID& shard_id, const ReplicaID& replica_id)) \
    X(std::unique_ptr<SdmRuntimeStore> clone() const)


class SdmRuntimeStore {
   public:
    Status init() { return Status::OK(); }

#define X(...) __VA_ARGS__;
    ISDM_RUNTIME_STORE_METHODS(X)
#undef X

   private:
    std::unordered_map<NodeID, NodePtr> nodes_;
    std::unordered_map<ShardID, ShardRoutePtr, ShardIDHash> shard_routes_;
};

}  // namespace adviskv::sdm