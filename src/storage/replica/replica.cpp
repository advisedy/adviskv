#include "storage/replica/replica.h"
#include "common.pb.h"
#include "common/common.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "storage.pb.h"
#include "storage/engine/map_engine.h"
#include <cstdint>
#include <memory>


namespace adviskv{

Status Replica::init(const ReplicaInitParam& param){

    replica_id_ = {param.table_id, param.shard_id};

    role_ = param.role;

    switch (param.engine_type) {
        case pb::EngineType::ENGINE_MAP:{
            engine_ = std::make_unique<MapEngine>();
            break;
        }
        case pb::EngineType::ENGINE_ROCKSDB:{
            // engine_ = std::make_unique<>()
            break;
        }
        default: {
            return {StatusCode::INVALID_ARGUMENT, "engine type is invaiid"};
            break;
        }
    }
    
    return Status::OK();
}


Status Replica::put(const rpc::PutRequest& req){
    
    if(!engine_){
        WARN("engine is nullptr, replica: table_id = {}, shard_id = {}", replica_id_.table_id, replica_id_.shard_id);
        return Status{StatusCode::ERROR,"engine is nullptr"};
    }
    // TODO 
    // 判断一下是否还可以写入（空间是否够）
    // 第一版就先不添加expire_ts了，后续考虑
    Status res = engine_->put(req.key(), req.value());
    if(!res.ok()){
        WARN("engine put is not ok, key = {}, value = {}, msg = {}", req.key(), req.value(), res.msg());
    }
    return res;

}

Status Replica::get(const rpc::GetRequest& req, rpc::GetResponse& rsp){
    if(!engine_){
        WARN("engine is nullptr, replica: table_id = {}, shard_id = {}", replica_id_.table_id, replica_id_.shard_id);
        return Status{StatusCode::ERROR,"engine is nullptr"};
    }
    Value value;
    Status status = engine_->get(req.key(), value);
    if(!status.ok()){
        WARN("engine get is not ok, key = {}, msg = {}", req.key(), status.msg());
        return status;
    }
    fill_base_rsp(rsp, status);
    rsp.set_value(std::move(value));
    return Status{StatusCode::OK};
}

}