#pragma once

#include <unordered_map>

#include "sdm/store/i_sdm_metastore.h"

namespace adviskv::sdm {

class MemoryMetaStore : public ISdmMetaStore {
   public:
    Status init() override { return Status::OK(); }

#define X(...) __VA_ARGS__ override;
    ISDM_METASTORE_METHODS(X)
#undef X

   protected:
    std::unordered_map<TableID, TablePtr> tables_;
    std::unordered_map<ReplicaID, ReplicaPtr, ReplicaIDHash> replicas_;
    std::unordered_map<std::string, ResourcePoolPtr> resource_pools_;
    std::unordered_map<ShardID, ReplicaGroupPtr, ShardIDHash> replica_groups_;
};

}  // namespace adviskv::sdm
