#pragma once

#include <functional>

#include "common/status.h"

namespace adviskv::storage {

struct ReplicaContext;
struct RaftEffects;

// 放到这里是因为Loop和Tracker都会调用到，内容就是执行Raft操作，完成持久化，然后发送消息。
Status run_raft_step(ReplicaContext& context, std::function<Status(RaftEffects&)> step);

}  // namespace adviskv::storage
