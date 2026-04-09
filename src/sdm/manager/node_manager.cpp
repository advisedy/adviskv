#include "sdm/manager/node_manager.h"

namespace adviskv{

Status NodeManager::add_node(const pb::NodeInfo& node_info){
    if(node_map_.count(node_info.node_name())){
        return Status{StatusCode::INVALID_ARGUMENT, "node_id already exists"};
    }
    node_map_[node_info.node_name()] = node_info;
    return Status::OK();
}

}