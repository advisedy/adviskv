#pragma once

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"

#include "sdm/plan/iplan.h"
#include "sdm/model/route_model.h"

#include <string>
#include <vector>

namespace adviskv {


struct ReplicaPlacement {
    int32_t replica_index = -1;
    NodeID node_id;
    std::string ip;
    int32_t port = 0;
    ReplicaRole role = ReplicaRole::FOLLOWER;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(replica_index >= 0,
                                    "replica_index should >= 0");
        RETURN_IF_INVALID_CONDITION(!node_id.empty(),
                                    "node_id should not be empty");
        RETURN_IF_INVALID_CONDITION(!ip.empty(),
                                    "ip should not be empty");
        RETURN_IF_INVALID_CONDITION(port > 0,
                                    "port should > 0");
        return Status::OK();
    }
};


struct ShardPlacement {
    TableID table_id = -1;
    ShardID shard_id = -1;
    std::vector<ReplicaPlacement> replicas;

    Status validate(int32_t expected_replica_count) const {
        RETURN_IF_INVALID_CONDITION(table_id != -1,
                                    "table_id should be valid");
        RETURN_IF_INVALID_CONDITION(shard_id >= 0,
                                    "shard_id should >= 0");
        RETURN_IF_INVALID_CONDITION(
            static_cast<int32_t>(replicas.size()) == expected_replica_count,
            "replicas size should equal expected_replica_count");

        int has_leader = 0;
        for (const auto& replica : replicas) {
            RETURN_IF_INVALID_STATUS(replica.validate());
            if (replica.role == ReplicaRole::LEADER) {
                has_leader++;
            }
        }

        RETURN_IF_INVALID_CONDITION(has_leader == 1,
                                    "each shard placement should have one leader");
        return Status::OK();
    }
};

//////////上面是一些placement会公共使用的一些model， 下面是plan



class PlaceTablePlan : public IPlan {
public:
    DatabaseID db_id = -1;
    TableID table_id = -1;
    std::string db_name;
    std::string table_name;
    int32_t shard_count = 0;
    int32_t replica_count = 0;
    std::vector<ShardPlacement> shard_placements;

    Status validate() const override {
        RETURN_IF_INVALID_CONDITION(db_id != -1,
                                    "db_id should be valid");
        RETURN_IF_INVALID_CONDITION(table_id != -1,
                                    "table_id should be valid");
        RETURN_IF_INVALID_CONDITION(!db_name.empty(),
                                    "db_name should not be empty");
        RETURN_IF_INVALID_CONDITION(!table_name.empty(),
                                    "table_name should not be empty");
        RETURN_IF_INVALID_CONDITION(shard_count > 0,
                                    "shard_count should > 0");
        RETURN_IF_INVALID_CONDITION(replica_count > 0,
                                    "replica_count should > 0");
        RETURN_IF_INVALID_CONDITION(
            static_cast<int32_t>(shard_placements.size()) == shard_count,
            "shard_placements size should equal shard_count");

        for (const auto& shard : shard_placements) {
            RETURN_IF_INVALID_STATUS(shard.validate(replica_count));
        }

        return Status::OK();
    }
};


} // namespace adviskv