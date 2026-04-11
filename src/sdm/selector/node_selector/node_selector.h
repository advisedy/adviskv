#pragma once

#include "common/status.h"

namespace adviskv{

struct NodeSelectorParam{
    std::string param;
    
};

    class NodeSelector{
        static Status select_nodes(std::string zone);
    };
}