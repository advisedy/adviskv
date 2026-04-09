#pragma once

#include "common.pb.h"
#include "common/status.h"

#include "common/type.h"

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <string>

namespace adviskv{



struct NodeMeta{
    NodeID node_id;
    std::string ip;
    int32_t port;
};

class NodeManager{

public:
    NodeManager() = default;
    Status add_node(const pb::NodeInfo& node_info);
private:
    std::shared_mutex node_map_mtx_;
    std::unordered_map<std::string, pb::NodeInfo> node_map_;    
};

}