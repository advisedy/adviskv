#pragma once

#include "common.pb.h"
#include "common/define.h"
#include "common/model/sdm_table_status.h"

namespace adviskv {

inline bool decode_pb_sdm_table_desired(pb::SdmTableDesired in,
                                        SdmTableDesired& out) {
    switch (in) {
        case pb::SdmTableDesired::SDM_TABLE_DESIRED_PRESENT:
            out = SdmTableDesired::PRESENT;
            return true;
        case pb::SdmTableDesired::SDM_TABLE_DESIRED_ABSENT:
            out = SdmTableDesired::ABSENT;
            return true;
        case pb::SdmTableDesired::SDM_TABLE_DESIRED_UNSPECIFIED:
        default:
            return false;
    }
}

inline bool encode_pb_sdm_table_desired(SdmTableDesired in,
                                        pb::SdmTableDesired& out) {
    switch (in) {
        case SdmTableDesired::PRESENT:
            out = pb::SdmTableDesired::SDM_TABLE_DESIRED_PRESENT;
            return true;
        case SdmTableDesired::ABSENT:
            out = pb::SdmTableDesired::SDM_TABLE_DESIRED_ABSENT;
            return true;
        default:
            return false;
    }
}

inline pb::SdmTableDesired to_pb_sdm_table_desired(SdmTableDesired desired) {
    pb::SdmTableDesired out =
        pb::SdmTableDesired::SDM_TABLE_DESIRED_UNSPECIFIED;
    IGNORE_RESULT(encode_pb_sdm_table_desired(desired, out));
    return out;
}

inline bool decode_pb_sdm_table_phase(pb::SdmTablePhase in,
                                      SdmTablePhase& out) {
    switch (in) {
        case pb::SdmTablePhase::SDM_TABLE_PHASE_CREATING:
            out = SdmTablePhase::CREATING;
            return true;
        case pb::SdmTablePhase::SDM_TABLE_PHASE_READY:
            out = SdmTablePhase::READY;
            return true;
        case pb::SdmTablePhase::SDM_TABLE_PHASE_DELETING:
            out = SdmTablePhase::DELETING;
            return true;
        case pb::SdmTablePhase::SDM_TABLE_PHASE_DELETED:
            out = SdmTablePhase::DELETED;
            return true;
        case pb::SdmTablePhase::SDM_TABLE_PHASE_FAILED:
            out = SdmTablePhase::FAILED;
            return true;
        case pb::SdmTablePhase::SDM_TABLE_PHASE_UNSPECIFIED:
        default:
            return false;
    }
}

inline bool encode_pb_sdm_table_phase(SdmTablePhase in,
                                      pb::SdmTablePhase& out) {
    switch (in) {
        case SdmTablePhase::CREATING:
            out = pb::SdmTablePhase::SDM_TABLE_PHASE_CREATING;
            return true;
        case SdmTablePhase::READY:
            out = pb::SdmTablePhase::SDM_TABLE_PHASE_READY;
            return true;
        case SdmTablePhase::DELETING:
            out = pb::SdmTablePhase::SDM_TABLE_PHASE_DELETING;
            return true;
        case SdmTablePhase::DELETED:
            out = pb::SdmTablePhase::SDM_TABLE_PHASE_DELETED;
            return true;
        case SdmTablePhase::FAILED:
            out = pb::SdmTablePhase::SDM_TABLE_PHASE_FAILED;
            return true;
        default:
            return false;
    }
}

inline pb::SdmTablePhase to_pb_sdm_table_phase(SdmTablePhase phase) {
    pb::SdmTablePhase out = pb::SdmTablePhase::SDM_TABLE_PHASE_UNSPECIFIED;
    IGNORE_RESULT(encode_pb_sdm_table_phase(phase, out));
    return out;
}

}  // namespace adviskv