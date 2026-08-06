#pragma once

#include <string>
#include <unordered_map>

#include "common/model/type.h"
#include "common/status.h"
#include "topo/model/model.h"

namespace adviskv::topo {

struct TopoPersistedRecord {
    std::unordered_map<TableID, Table> tables;
    std::unordered_map<ReplicaID, Replica, ReplicaIDHash> replicas;
    std::unordered_map<std::string, ResourcePool> resource_pools;
    std::unordered_map<ShardID, ReplicaGroup, ShardIDHash> replica_groups;
};

class ITopoPersistEngine {
public:
    virtual ~ITopoPersistEngine() = default;

    virtual Status init() = 0;
    virtual Status close() = 0;
    virtual Status save_topo_meta(const TopoPersistedRecord& record) = 0;
    virtual Status load_topo_meta(TopoPersistedRecord& record) = 0;
};

class TopoPersistEngine : public ITopoPersistEngine {
public:
    explicit TopoPersistEngine(const std::string& data_dir);
    ~TopoPersistEngine();

    Status init() override;
    Status close() override;

    Status save_topo_meta(const TopoPersistedRecord& record) override;
    Status load_topo_meta(TopoPersistedRecord& record) override;

private:
    std::string data_dir_;
    std::string meta_path_;
    std::string meta_tmp_path_;
    bool init_flag_{false};
};

}  // namespace adviskv::topo