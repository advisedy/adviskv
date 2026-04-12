#pragma once

#include "common/type.h"

namespace adviskv {

struct NodeMeta {
  NodeID node_id;
  std::string ip;
  int32_t port;
  std::string zone; // 只有同一个zone的，才可以被放在一起
};

struct NodeStats {
  NodeID node_id;

  int32_t owned_replica_count{0};
  int32_t leader_count{0};
};

} // namespace adviskv