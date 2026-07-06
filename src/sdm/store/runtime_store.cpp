#include "sdm/store/runtime_store.h"

#include <memory>

#include "common/func.h"

namespace adviskv::sdm {

std::unique_ptr<SdmRuntimeStore> SdmRuntimeStore::clone() const {
    auto copied = std::make_unique<SdmRuntimeStore>();
    for (const auto& [id, node] : nodes_) {
        copied->nodes_[id] = std::make_shared<Node>(*node);
    }
    for (const auto& [shard_id, route] : shard_routes_) {
        copied->shard_routes_[shard_id] = std::make_shared<ShardRoute>(*route);
    }
    return copied;
}

Status SdmRuntimeStore::upsert_node(const Node& node) {
    nodes_[node.id] = std::make_shared<Node>(node);
    return Status::OK();
}

Status SdmRuntimeStore::get_node(const NodeID& node_id, NodePtr& out) const {
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status SdmRuntimeStore::list_nodes(std::vector<NodePtr>& out) const {
    out.clear();
    out.reserve(nodes_.size());
    for (const auto& [_, node] : nodes_) {
        out.push_back(node);
    }
    return Status::OK();
}

Status SdmRuntimeStore::put_shard_route(const ShardRoute& route) {
    shard_routes_[route.shard_id] = std::make_shared<ShardRoute>(route);
    return Status::OK();
}

Status SdmRuntimeStore::get_shard_route(const ShardID& shard_id,
                                     ShardRoutePtr& out) const {
    auto it = shard_routes_.find(shard_id);
    if (it == shard_routes_.end()) {
        out.reset();
        return Status::OK();
    }
    out = it->second;
    return Status::OK();
}

Status SdmRuntimeStore::delete_shard_route(const ShardID& shard_id) {
    shard_routes_.erase(shard_id);
    return Status::OK();
}

Status SdmRuntimeStore::del_shard_route_entry(const ShardID& shard_id,
                                           const ReplicaID& replica_id) {
    auto it = shard_routes_.find(shard_id);
    if (it == shard_routes_.end() || it->second == nullptr) {
        return Status::OK();
    }

    auto& replicas = it->second->replicas;
    func::ad_erase_if(replicas, [&replica_id](const RouteEntry& entry) {
        return entry.replica_id == replica_id;
    });
    return Status::OK();
}

}  // namespace adviskv::sdm