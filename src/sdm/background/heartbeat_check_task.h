#pragma once

#include "sdm/background/background_task.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/store.h"
namespace adviskv::sdm{

class HeartBeatCheckTask : public BackgroundTask{
protected:

    void run()override;

private:

    Status check_resource_pool(const ResourcePool& pool);
    Status check_and_modify_node(Node& node);

    Status mark_node_offline(Node& node);

    SdmStore* sdm_store_;

};

}