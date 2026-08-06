#pragma once

#include "common.pb.h"
#include "common/define.h"
#include "common/model/topo_table_status.h"

namespace adviskv {

inline bool decode_pb_topo_table_desired(pb::TopoTableDesired in, TopoTableDesired& out) {
    switch (in) {
        case pb::TopoTableDesired::TOPO_TABLE_DESIRED_PRESENT:
            out = TopoTableDesired::PRESENT;
            return true;
        case pb::TopoTableDesired::TOPO_TABLE_DESIRED_ABSENT:
            out = TopoTableDesired::ABSENT;
            return true;
        default:
            return false;
    }
}

inline bool encode_pb_topo_table_desired(TopoTableDesired in, pb::TopoTableDesired& out) {
    switch (in) {
        case TopoTableDesired::PRESENT:
            out = pb::TopoTableDesired::TOPO_TABLE_DESIRED_PRESENT;
            return true;
        case TopoTableDesired::ABSENT:
            out = pb::TopoTableDesired::TOPO_TABLE_DESIRED_ABSENT;
            return true;
        default:
            return false;
    }
}

inline bool decode_pb_topo_table_phase(pb::TopoTablePhase in, TopoTablePhase& out) {
    switch (in) {
        case pb::TopoTablePhase::TOPO_TABLE_PHASE_CREATING:
            out = TopoTablePhase::CREATING;
            return true;
        case pb::TopoTablePhase::TOPO_TABLE_PHASE_READY:
            out = TopoTablePhase::READY;
            return true;
        case pb::TopoTablePhase::TOPO_TABLE_PHASE_DELETING:
            out = TopoTablePhase::DELETING;
            return true;
        case pb::TopoTablePhase::TOPO_TABLE_PHASE_DELETED:
            out = TopoTablePhase::DELETED;
            return true;
        case pb::TopoTablePhase::TOPO_TABLE_PHASE_FAILED:
            out = TopoTablePhase::FAILED;
            return true;
        case pb::TopoTablePhase::TOPO_TABLE_PHASE_RESIZING:
            out = TopoTablePhase::RESIZING;
            return true;
        default:
            return false;
    }
}

inline bool encode_pb_topo_table_phase(TopoTablePhase in, pb::TopoTablePhase& out) {
    switch (in) {
        case TopoTablePhase::CREATING:
            out = pb::TopoTablePhase::TOPO_TABLE_PHASE_CREATING;
            return true;
        case TopoTablePhase::READY:
            out = pb::TopoTablePhase::TOPO_TABLE_PHASE_READY;
            return true;
        case TopoTablePhase::DELETING:
            out = pb::TopoTablePhase::TOPO_TABLE_PHASE_DELETING;
            return true;
        case TopoTablePhase::DELETED:
            out = pb::TopoTablePhase::TOPO_TABLE_PHASE_DELETED;
            return true;
        case TopoTablePhase::FAILED:
            out = pb::TopoTablePhase::TOPO_TABLE_PHASE_FAILED;
            return true;
        case TopoTablePhase::RESIZING:
            out = pb::TopoTablePhase::TOPO_TABLE_PHASE_RESIZING;
            return true;
        default:
            return false;
    }
}

}  // namespace adviskv
