#pragma once


#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include "common.pb.h"
#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/engine/kv_engine.h"
#include "storage.pb.h"
#include "storage/raft/raft_node.h"

namespace adviskv::storage{


struct ReplicaInitParam{
    ShardID shard_id;
    pb::EngineType engine_type;
};

struct PutParam{
    const Key& key;
    const Value& value;

    Status validate() const {
        return Status::OK();
    }
};

struct GetParam{
    const Key& key;
    Status validate() const {
        return Status::OK();
    }    
};

class Replica{

public:

    ShardKey get_shard_key() const{
        return shard_key_;
    }
    TableID get_table_id() const{
        return shard_key_.table_id;
    }
    ShardID get_shard_id() const{
        return shard_key_;
    }

    Status put(const PutParam& param);
    Status get(const GetParam& param, Value& value);
    

private:
    
    friend class ReplicaManager;

    Status init(const ReplicaInitParam& param);

    // Replica() = default;

    ShardKey shard_key_;
    // pb::ReplicaRole role_;
    std::unique_ptr<KVEngine> engine_;
    std::unique_ptr<RaftNode> raft_node_;

};

}
