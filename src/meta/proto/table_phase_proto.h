#pragma once

#include "common.pb.h"
#include "meta/model/model.h"

namespace adviskv {

inline bool decode_pb_table_phase(pb::TablePhase in, meta::TablePhase& out) {
    switch (in) {
        case pb::TablePhase::TABLE_PHASE_CREATING:
            out = meta::TablePhase::CREATING;
            return true;
        case pb::TablePhase::TABLE_PHASE_READY:
            out = meta::TablePhase::READY;
            return true;
        case pb::TablePhase::TABLE_PHASE_DELETING:
            out = meta::TablePhase::DELETING;
            return true;
        case pb::TablePhase::TABLE_PHASE_DELETED:
            out = meta::TablePhase::DELETED;
            return true;
        case pb::TablePhase::TABLE_PHASE_FAILED:
            out = meta::TablePhase::FAILED;
            return true;
        case pb::TablePhase::TABLE_PHASE_RESIZING:
            out = meta::TablePhase::RESIZING;
            return true;
        default:
            return false;
    }
}

inline bool encode_pb_table_phase(meta::TablePhase in, pb::TablePhase& out) {
    switch (in) {
        case meta::TablePhase::CREATING:
            out = pb::TablePhase::TABLE_PHASE_CREATING;
            return true;
        case meta::TablePhase::READY:
            out = pb::TablePhase::TABLE_PHASE_READY;
            return true;
        case meta::TablePhase::DELETING:
            out = pb::TablePhase::TABLE_PHASE_DELETING;
            return true;
        case meta::TablePhase::DELETED:
            out = pb::TablePhase::TABLE_PHASE_DELETED;
            return true;
        case meta::TablePhase::FAILED:
            out = pb::TablePhase::TABLE_PHASE_FAILED;
            return true;
        case meta::TablePhase::RESIZING:
            out = pb::TablePhase::TABLE_PHASE_RESIZING;
            return true;
        default:
            return false;
    }
}

}  // namespace adviskv
