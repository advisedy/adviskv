#pragma once

#include "common.pb.h"
#include "common/model/engine_type.h"

namespace adviskv {

inline bool decode_pb_engine_type(pb::EngineType in, EngineType& out) {
    switch (in) {
        case pb::ENGINE_TYPE_MAP:
            out = EngineType::MAP;
            return true;
        default:
            return false;
    }
}

inline bool encode_pb_engine_type(EngineType in, pb::EngineType& out) {
    switch (in) {
        case EngineType::MAP:
            out = pb::ENGINE_TYPE_MAP;
            return true;
        default:
            return false;
    }
}

}  // namespace adviskv