#pragma once

#include "common.pb.h"
#include "common/define.h"
#include "catalog/model/model.h"

namespace adviskv::catalog {

inline bool decode_pb_catalog_table_state(pb::CatalogTableState in, TableState& out) {
    switch (in) {
        case pb::CatalogTableState::CATALOG_TABLE_STATE_ADDING:
            out = TableState::ADDING;
            return true;
        case pb::CatalogTableState::CATALOG_TABLE_STATE_NORMAL:
            out = TableState::NORMAL;
            return true;
        case pb::CatalogTableState::CATALOG_TABLE_STATE_FAILED:
            out = TableState::FAILED;
            return true;
        case pb::CatalogTableState::CATALOG_TABLE_STATE_DROPPING:
            out = TableState::DROPPING;
            return true;
        case pb::CatalogTableState::CATALOG_TABLE_STATE_DELETED:
            out = TableState::DELETED;
            return true;
        case pb::CatalogTableState::CATALOG_TABLE_STATE_ALTERING:
            out = TableState::ALTERING;
            return true;
        default:
            return false;
    }
}

inline bool encode_pb_catalog_table_state(TableState in, pb::CatalogTableState& out) {
    switch (in) {
        case TableState::ADDING:
            out = pb::CatalogTableState::CATALOG_TABLE_STATE_ADDING;
            return true;
        case TableState::NORMAL:
            out = pb::CatalogTableState::CATALOG_TABLE_STATE_NORMAL;
            return true;
        case TableState::FAILED:
            out = pb::CatalogTableState::CATALOG_TABLE_STATE_FAILED;
            return true;
        case TableState::DROPPING:
            out = pb::CatalogTableState::CATALOG_TABLE_STATE_DROPPING;
            return true;
        case TableState::DELETED:
            out = pb::CatalogTableState::CATALOG_TABLE_STATE_DELETED;
            return true;
        case TableState::ALTERING:
            out = pb::CatalogTableState::CATALOG_TABLE_STATE_ALTERING;
            return true;
        default:
            return false;
    }
}

}  // namespace adviskv::catalog
