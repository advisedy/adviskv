#pragma once

#include "common.pb.h"
#include "common/model/endpoint.h"

namespace adviskv {

inline bool encode_pb_endpoint(const Endpoint& in, pb::Endpoint& out) {
    out.set_ip(in.ip);
    out.set_port(in.port);
    return true;
}

inline bool decode_pb_endpoint(const pb::Endpoint& in, Endpoint& out) {
    out = Endpoint{in.ip(), in.port()};
    return true;
}

}  // namespace adviskv