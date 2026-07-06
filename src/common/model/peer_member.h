#pragma once

#include <string>

#include <fmt/format.h>

#include "common/define.h"
#include "common/model/endpoint.h"
#include "common/model/ids.h"
#include "common/types.h"

namespace adviskv {

struct PeerMember {
    NodeID node_id;
    ReplicaID replica_id;
    Endpoint endpoint;

    bool operator==(const PeerMember& other) const {
        if (node_id != other.node_id)
            return false;
        if (!(replica_id == other.replica_id))
            return false;
        if (!(endpoint == other.endpoint))
            return false;
        return true;
    }

    DEFINE_OPERATOR_NOT_EQUAL(PeerMember)

    std::string to_string() const {
        return fmt::format("node_id:{}, replica_id:{}, endpoint:{}", node_id, replica_id.to_string(),
                           endpoint.to_string());
    }
};

}  // namespace adviskv