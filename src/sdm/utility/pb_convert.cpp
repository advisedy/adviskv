
#include <string>
#include <unordered_map>
#include <vector>

#include "common.pb.h"
#include "common/type.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

/*
如果以后有新的类型要转换，首先在ConvertMapList里面补充Map。
然后在define_pb_convert里面写等价条件，
最后补充convert_xxxx_to_pb，和convert_pb_to_xxxx函数。

如果只是要补充等价条件，直接在define_pb_convert里面补充就好。

*/

struct ConvertMapList {
    std::unordered_map<pb::NodeStatus, sdm::NodeStatus> pb2node_status{};
    std::unordered_map<sdm::NodeStatus, pb::NodeStatus> node_status2pb{};

    std::unordered_map<pb::ReplicaRole, sdm::ReplicaRole> pb2replica_role{};
    std::unordered_map<sdm::ReplicaRole, pb::ReplicaRole> replica_role2pb{};

    std::unordered_map<pb::ReplicaStatus, sdm::ReplicaStatus>
        pb2replica_status{};
    std::unordered_map<sdm::ReplicaStatus, pb::ReplicaStatus>
        replica_status2pb{};
};

namespace {

#define DEFINE_PB_EQUAL_NODE_STATUS(pb, sdm)       \
    {                                              \
        convert_map_list.pb2node_status[pb] = sdm; \
        convert_map_list.node_status2pb[sdm] = pb; \
    }

#define DEFINE_PB_EQUAL_REPLICA_STATUS(pb, sdm)       \
    {                                                 \
        convert_map_list.pb2replica_status[pb] = sdm; \
        convert_map_list.replica_status2pb[sdm] = pb; \
    }

#define DEFINE_PB_EQUAL_REPLICA_ROLE(pb, sdm)       \
    {                                               \
        convert_map_list.pb2replica_role[pb] = sdm; \
        convert_map_list.replica_role2pb[sdm] = pb; \
    }
void define_pb_convert(ConvertMapList& convert_map_list) {
    // 这个宏定义是代表 pb 的这个类型，等价于sdm这边定义的类型，反向的也会处理

    // pb to node_status
    DEFINE_PB_EQUAL_NODE_STATUS(pb::NodeStatus::NODE_ONLINE,
                                sdm::NodeStatus::ONLINE)
    DEFINE_PB_EQUAL_NODE_STATUS(pb::NodeStatus::NODE_OFFLINE,
                                sdm::NodeStatus::OFFLINE)
    DEFINE_PB_EQUAL_NODE_STATUS(pb::NodeStatus::NODE_SUSPECT,
                                sdm::NodeStatus::SUSPECT)

    // replica status
    DEFINE_PB_EQUAL_REPLICA_STATUS(pb::ReplicaStatus::ADDING,
                                   sdm::ReplicaStatus::ADDING)
    DEFINE_PB_EQUAL_REPLICA_STATUS(pb::ReplicaStatus::READY,
                                   sdm::ReplicaStatus::READY)
    DEFINE_PB_EQUAL_REPLICA_STATUS(pb::ReplicaStatus::LOST,
                                   sdm::ReplicaStatus::LOST)
    DEFINE_PB_EQUAL_REPLICA_STATUS(pb::ReplicaStatus::ERROR,
                                   sdm::ReplicaStatus::ERROR)

    // replica role
    DEFINE_PB_EQUAL_REPLICA_ROLE(pb::ReplicaRole::LEADER,
                                 sdm::ReplicaRole::LEADER)
    DEFINE_PB_EQUAL_REPLICA_ROLE(pb::ReplicaRole::FOLLOWER,
                                 sdm::ReplicaRole::FOLLOWER)
}

#undef DEFINE_CONVERT_REPLICA_ROLE
#undef DEFINE_CONVERT_REPLICA_STATUS
#undef DEFINE_CONVERT_NODE_STATUS
}  // namespace

class PbConvertHelper {
   public:
    static PbConvertHelper& get_instance() {
        static PbConvertHelper instance;
        return instance;
    }

    PbConvertHelper(const PbConvertHelper& one) = delete;
    PbConvertHelper operator=(const PbConvertHelper& one) = delete;

    ConvertMapList convert_map_list_{};

   private:
    PbConvertHelper() { define_pb_convert(convert_map_list_); }
};

#define GET_MAP_LIST PbConvertHelper::get_instance().convert_map_list_

 bool convert_pb_to_node_status(pb::NodeStatus in, sdm::NodeStatus& out) {
    if (auto it = GET_MAP_LIST.pb2node_status.find(in);
        it != GET_MAP_LIST.pb2node_status.end()) {
        out = it->second;
        return true;
    }
    return false;
}

 bool convert_node_status_to_pb(sdm::NodeStatus in, pb::NodeStatus& out) {
    if (auto it = GET_MAP_LIST.node_status2pb.find(in);
        it != GET_MAP_LIST.node_status2pb.end()) {
        out = it->second;
        return true;
    }
    return false;
}

 bool convert_pb_to_replica_role(pb::ReplicaRole in,
                                       sdm::ReplicaRole& out) {
    if (auto it = GET_MAP_LIST.pb2replica_role.find(in);
        it != GET_MAP_LIST.pb2replica_role.end()) {
        out = it->second;
        return true;
    }
    return false;
}

 bool convert_replica_role_to_pb(sdm::ReplicaRole in,
                                       pb::ReplicaRole& out) {
    if (auto it = GET_MAP_LIST.replica_role2pb.find(in);
        it != GET_MAP_LIST.replica_role2pb.end()) {
        out = it->second;
        return true;
    }
    return false;
}

 bool convert_pb_to_replica_status(pb::ReplicaStatus in,
                                         sdm::ReplicaStatus& out) {
    if (auto it = GET_MAP_LIST.pb2replica_status.find(in);
        it != GET_MAP_LIST.pb2replica_status.end()) {
        out = it->second;
        return true;
    }
    return false;
}
bool convert_replica_status_to_pb(sdm::ReplicaStatus in,
                                         pb::ReplicaStatus& out) {
    if (auto it = GET_MAP_LIST.replica_status2pb.find(in);
        it != GET_MAP_LIST.replica_status2pb.end()) {
        out = it->second;
        return true;
    }
    return false;
}
#undef GET_MAP_LIST


// #undef CONVERT_IN_TO_OUT

}  // namespace adviskv::sdm
