#pragma once


#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include "common.pb.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/engine/kv_engine.h"
#include "storage.pb.h"

namespace adviskv{


struct ReplicaInitParam{
    int32_t table_id;
    int32_t shard_id;
    pb::ReplicaRole role;
    pb::EngineType engine_type;
};

struct ReplicaID{
    int32_t table_id;
    int32_t shard_id;
    bool operator==(const ReplicaID& one) const {
        return table_id == one.table_id and shard_id == one.shard_id;
    }
};

struct ReplicaIDHash{
    uint64_t operator()(const ReplicaID& one) const {
        return (std::hash<int32_t>{}(one.table_id)*1000) + (std::hash<int32_t>{}(one.shard_id));
    }
};


class Replica{

public:
    ReplicaID get_replica_id() const{
        return replica_id_;
    }
    int32_t get_table_id() const{
        return replica_id_.table_id;
    }
    int32_t get_shard_id() const{
        return replica_id_.shard_id;
    }

    Status put(const rpc::PutRequest& req);
    Status get(const rpc::GetRequest& req, rpc::GetResponse& res);


private:
    
    friend class ReplicaManager;

    Status init(const ReplicaInitParam& param);

    // Replica() = default;

    ReplicaID replica_id_;
    pb::ReplicaRole role_;
    std::unique_ptr<KVEngine> engine_;

};

}
