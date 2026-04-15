#pragma once

#include "common/type.h"
#include "sdm/background/background_task.h"
#include "sdm/model/sdm_store.h"
namespace adviskv::sdm{


//这个的职责就是检测一下shard里面的合法的replica是否满足了容量
// 如果没有满足的话，就创建出来（但是按照设计记录来说，不会没有，只会多了需要缩容）
class CapacityCheckTask : public BackgroundTask{

protected:
    void run() override;

private:
    Status check_replica_list(const Table& table, ShardID shard_id);


    SdmStore sdm_store_;

};

}