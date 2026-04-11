#pragma once 
#include "common/status.h"
#include "common/define.h"
#include "sdm/manager/route_manager.h"
#include "sdm/manager/node_manager.h"
#include <cstdint>


namespace adviskv {


struct PlaceTableParam{
    int32_t db_id;
    int32_t table_id;
    int32_t replica_count;
    int32_t shard_count;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(replica_count > 0, "replica_count should be greater than 0")
        RETURN_IF_INVALID_CONDITION(shard_count >= 0, "shard_count should be greater than 0")
        return Status::OK();
    }
};

class PlacementService{

public:
    explicit PlacementService(RouteManager* route_manager, NodeManager* node_manager);

    Status place_table(const PlaceTableParam& param, TableMetaCache* table_meta_cache);


private:



    RouteManager* route_manager_;
    NodeManager* node_manager_;
};

}