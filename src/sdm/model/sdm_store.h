#pragma once

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/route_model.h"
#include "sdm/model/store.h"
namespace adviskv::sdm{

class SdmStore{

public:
    Status put_table(const Table& table);
    Status get_table(TableID table_id, Table* out) const;
    Status get_table_by_name(const std::string& db_name,
                            const std::string& table_name,
                            Table* out) const;
    Status list_tables(std::vector<Table>* out) const;

    Status get_shard_route(TableID table_id, ShardID shard_id, ShardRoute* route) const;

    Status put_node(const Node& node);

};

}