#pragma once 
#include "common/status.h"
#include "common/define.h"
#include "sdm/manager/meta_cache_manager.h"
#include "sdm/manager/route_manager.h"
#include "sdm/manager/node_manager.h"
#include "sdm/selector/leader_selector/leader_selector.h"
#include "sdm/selector/node_selector/node_selector.h"
#include <cstdint>


namespace adviskv {


struct PlaceTableParam{
    int32_t db_id;
    int32_t table_id;
    std::string db_name;
    std::string table_name;
    int32_t replica_count;
    int32_t shard_count;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        RETURN_IF_INVALID_CONDITION(replica_count > 0, "replica_count should be greater than 0")
        RETURN_IF_INVALID_CONDITION(shard_count >= 0, "shard_count should be greater than 0")
        return Status::OK();
    }
};

struct PlaceDBParam{
    int32_t db_id{-1};
    std::string db_name;
    std::string zone;
    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(db_id!=-1, "db_id should not is -1")
        return Status::OK();
    }
};

class OperationFactory;

class PlacementService{

public:
    explicit PlacementService(OperationFactory* factorys);

    Status place_table(const PlaceTableParam& param, TableMetaCache* table_meta_cache);
    Status place_db(const PlaceDBParam& param, DBMetaCache* db_meta_cache);


private:



    // RouteManager* route_manager_;
    // NodeManager* node_manager_;

    // NodeSelector* node_selector_;
    // LeaderSelector* leader_selector_;

    // MetaCacheManager* meta_cache_manager_;
    OperationFactory* factory_;
};

}