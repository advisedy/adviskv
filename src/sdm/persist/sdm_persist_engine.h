#pragma once

#include <string>
#include <unordered_map>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

struct SdmPersistedRecord {
    std::unordered_map<TableID, Table> tables;
    std::unordered_map<NodeID, Node> nodes;
    std::unordered_map<ReplicaID, Replica, ReplicaIDHash> replicas;
    std::unordered_map<std::string, ResourcePool> resource_pools;
    std::unordered_map<ShardID, ShardRoute, ShardIDHash> shard_routes;
};

class SdmPersistEngine {
   public:
    explicit SdmPersistEngine(const std::string& data_dir);
    ~SdmPersistEngine();

    Status init();
    Status close();

    Status save_sdm_meta(const SdmPersistedRecord& record);
    Status load_sdm_meta(SdmPersistedRecord& record);

   private:
    std::string data_dir_;
    std::string meta_path_;
    std::string meta_tmp_path_;
};

}  // namespace adviskv::sdm
