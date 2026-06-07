#pragma once

#include "common.pb.h"
#include "meta/catalog/meta_types.h"

namespace adviskv::meta {

inline bool decode_pb_meta_table_state(pb::MetaTableState in, TableState& out) {
    switch (in) {
        case pb::MetaTableState::META_TABLE_STATE_ADDING:
            out = TableState::ADDING;
            return true;
        case pb::MetaTableState::META_TABLE_STATE_NORMAL:
            out = TableState::NORMAL;
            return true;
        case pb::MetaTableState::META_TABLE_STATE_FAILED:
            out = TableState::FAILED;
            return true;
        case pb::MetaTableState::META_TABLE_STATE_DROPPING:
            out = TableState::DROPPING;
            return true;
        case pb::MetaTableState::META_TABLE_STATE_DELETED:
            out = TableState::DELETED;
            return true;
        case pb::MetaTableState::META_TABLE_STATE_UNSPECIFIED:
        default:
            return false;
    }
}

inline bool encode_pb_meta_table_state(TableState in, pb::MetaTableState& out) {
    switch (in) {
        case TableState::ADDING:
            out = pb::MetaTableState::META_TABLE_STATE_ADDING;
            return true;
        case TableState::NORMAL:
            out = pb::MetaTableState::META_TABLE_STATE_NORMAL;
            return true;
        case TableState::FAILED:
            out = pb::MetaTableState::META_TABLE_STATE_FAILED;
            return true;
        case TableState::DROPPING:
            out = pb::MetaTableState::META_TABLE_STATE_DROPPING;
            return true;
        case TableState::DELETED:
            out = pb::MetaTableState::META_TABLE_STATE_DELETED;
            return true;
        default:
            return false;
    }
}

inline pb::MetaTableState to_pb_meta_table_state(TableState state) {
    pb::MetaTableState out = pb::MetaTableState::META_TABLE_STATE_UNSPECIFIED;
    (void)encode_pb_meta_table_state(state, out);
    return out;
}

}  // namespace adviskv::meta
