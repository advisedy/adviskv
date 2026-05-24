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

class ISdmPersistEngine {
   public:
    virtual ~ISdmPersistEngine() = default;

    virtual Status init() = 0;
    virtual Status close() = 0;
    virtual Status save_sdm_meta(const SdmPersistedRecord& record) = 0;
    virtual Status load_sdm_meta(SdmPersistedRecord& record) = 0;
};

class SdmPersistEngine : public ISdmPersistEngine {
   public:
    explicit SdmPersistEngine(const std::string& data_dir);
    ~SdmPersistEngine();

    Status init() override;
    Status close() override;

    Status save_sdm_meta(const SdmPersistedRecord& record) override;
    Status load_sdm_meta(SdmPersistedRecord& record) override;

   private:
    std::string data_dir_;
    std::string meta_path_;
    std::string meta_tmp_path_;
    bool init_flag_{false};
};

}  // namespace adviskv::sdm
