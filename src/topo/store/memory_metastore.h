#pragma once

#include <unordered_map>

#include "topo/store/i_topo_metastore.h"

namespace adviskv::topo {

class MemoryMetaStore : public ITopoMetaStore {
public:
    Status init() override { return Status::OK(); }

#define X(...) __VA_ARGS__ override;
    ITOPO_METASTORE_METHODS(X)
#undef X

protected:
    std::unordered_map<TableID, TablePtr> tables_;
    std::unordered_map<ReplicaID, ReplicaPtr, ReplicaIDHash> replicas_;
    std::unordered_map<std::string, ResourcePoolPtr> resource_pools_;
    std::unordered_map<ShardID, ReplicaGroupPtr, ShardIDHash> replica_groups_;
};

}  // namespace adviskv::topo
