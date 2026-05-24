#include "sdm/service/table_service.h"

#include <fmt/format.h>

#include "common/define.h"
#include "common/func.h"
#include "common/status.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

TableService::TableService(SdmStore* store) : store_(store) {}

Status TableService::place_table(const PlaceTableParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    TableOr existing;
    Status status = store_->get_table(param.table_id, existing);
    RETURN_IF_INVALID_STATUS(status)
    if (!existing.is_empty()) {
        if (existing->spec.operation_id != param.operation_id) {
            // 就代表是别的请求，正常处理就好
            return Status::ALREADY_EXIST(
                fmt::format("table_id {} already exists with operation_id {}",
                            param.table_id, existing->spec.operation_id));
        }
        // operation_id相等，要保证好幂等
        return Status::OK();
    }

    Table table{
        .table_id = param.table_id,
        .spec{
            .table_name = param.table_name,
            .db_id = param.db_id,
            .db_name = param.db_name,
            .shard_count = param.shard_count,
            .replica_count = param.replica_count,
            .resource_pool = param.resource_pool,
            .operation_id = param.operation_id,
        },
        .state{
            .desired = TableDesired::PRESENT,
            .phase = TablePhase::CREATING,
            .update_ts = func::get_current_ts_ms(),
        },
    };
    return store_->put_table(table);
}

Status TableService::drop_table(const DropTableParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    TableOr existing;
    RETURN_IF_INVALID_STATUS(store_->get_table(param.table_id, existing))
    if (existing.is_empty()) {
        return Status::OK();
    }

    if (existing->state.phase == TablePhase::DELETED &&
        existing->state.desired == TableDesired::ABSENT) {
        return Status::OK();
    }

    if (existing->state.desired == TableDesired::ABSENT) {
        if (existing->spec.operation_id == param.operation_id) {
            return Status::OK();
        }
        return Status::ALREADY_EXIST(
            fmt::format("table_id {} is already dropping with operation_id {}",
                        param.table_id, existing->spec.operation_id));
    }

    if (existing->state.phase != TablePhase::READY) {
        return Status::ALREADY_EXIST(
            fmt::format("table_id {} is not READY for drop", param.table_id));
    }

    existing->state.desired = TableDesired::ABSENT;
    existing->state.phase = TablePhase::DELETING;
    existing->spec.operation_id = param.operation_id;
    existing->state.last_error_msg.clear();
    existing->state.update_ts = func::get_current_ts_ms();
    return store_->put_table(*existing);
}

Status TableService::get_table_status(const GetTableStatusParam& param,
                                      Table* table) {
    RETURN_IF_INVALID_PARAM(param)

    TableOr existing;
    Status status = store_->get_table(param.table_id, existing);
    RETURN_IF_INVALID_STATUS(status)
    if (existing.is_empty()) {
        return Status::TABLE_NOT_FOUND(
            fmt::format("table_id {} not found", param.table_id));
    }
    if (!param.operation_id.empty() &&
        existing->spec.operation_id != param.operation_id) {
        return Status::INVALID_ARGUMENT(fmt::format(
            "operation_id mismatch for table_id {}", param.table_id));
    }
    if (table != nullptr) {
        *table = *existing;
    }
    return Status::OK();
}

}  // namespace adviskv::sdm