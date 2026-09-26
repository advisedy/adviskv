#pragma once

#include <string>
#include <vector>

#include "common/model/type.h"
#include "common/status.h"
#include "meta/model/model.h"

namespace adviskv::meta {

struct MetaPersistedRecord {
    DatabaseID next_db_id{0};
    TableID next_table_id{0};
    std::vector<Database> databases;
    std::vector<Table> tables;
    std::vector<Replica> replicas;
    std::vector<ResourcePool> resource_pools;
    std::vector<ReplicaGroup> replica_groups;
};

class IMetaPersistEngine {
public:
    virtual ~IMetaPersistEngine() = default;
    virtual Status init() = 0;
    virtual Status save_meta(const MetaPersistedRecord& record) = 0;
    virtual Status load_meta(MetaPersistedRecord& record) = 0;
};

class MetaPersistEngine : public IMetaPersistEngine {
public:
    explicit MetaPersistEngine(const std::string& data_dir);
    ~MetaPersistEngine() override = default;

    Status init() override;
    Status save_meta(const MetaPersistedRecord& record) override;
    Status load_meta(MetaPersistedRecord& record) override;

private:
    std::string data_dir_;
    std::string meta_path_;
    bool init_flag_{false};
};

}  // namespace adviskv::meta
