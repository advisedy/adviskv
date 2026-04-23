#include "storage/replica/replica.h"
#include "common.pb.h"
#include "common/common.h"
#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "storage.pb.h"
#include "storage/engine/map_engine.h"
#include <cstdint>
#include <memory>


namespace adviskv::storage{

Status Replica::init(const ReplicaInitParam& param){

    shard_key_ = param.shard_id;

    // role_ = param.role;

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


Status Replica::put(const PutParam& param){
    
    if(!engine_){
        WARN("engine is nullptr, replica: table_id = {}, shard_index = {}", shard_key_.table_id, shard_key_.shard_index);
        return Status{StatusCode::ERROR,"engine is nullptr"};
    }
    RETURN_IF_INVALID_PARAM(param)
    // TODO 
    // 判断一下是否还可以写入（空间是否够）
    // 第一版就先不添加expire_ts了，后续考虑
    Status status = engine_->put(param.key, param.value);
    if(status.fail()){
        WARN("engine put is not ok, key = {}, value = {}, msg = {}", param.key, param.value, status.msg());
    }
    return status;

}

Status Replica::get(const GetParam& param, Value& value){
    if(!engine_){
        WARN("engine is nullptr, replica: table_id = {}, shard_index = {}", shard_key_.table_id, shard_key_.shard_index);
        return Status{StatusCode::ERROR,"engine is nullptr"};
    }

    RETURN_IF_INVALID_PARAM(param)

    Status status = engine_->get(param.key, value);
    if(status.fail()){
        WARN("engine get is not ok, key = {}, msg = {}", param.key, status.msg());
    }
    return status;
}

}
