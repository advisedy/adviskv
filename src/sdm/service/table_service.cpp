#include "sdm/service/table_service.h"

#include <fmt/format.h>

#include "common/define.h"
#include "common/status.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {
namespace {

bool is_same_table_request(const Table& table, const PlaceTableParam& param) {
    return table.spec.db_id == param.db_id &&
           table.spec.db_name == param.db_name &&
           table.spec.table_name == param.table_name &&
           table.spec.replica_count == param.replica_count &&
           table.spec.shard_count == param.shard_count &&
           table.spec.resource_pool == param.resource_pool;
}

}  // namespace

TableService::TableService(SdmStore* store, PlaceTableWorkflow* workflow)
    : store_(store), workflow_(workflow) {}

Status TableService::place_table(const PlaceTableParam& param) {
    RETURN_IF_INVALID_PARAM(param)

    TablePtr existing;
    Status status = store_->get_table(param.table_id, existing);
    RETURN_IF_INVALID_STATUS(status)
    if (existing != nullptr) {
        if (existing->spec.operation_id == param.operation_id) {
            if (!is_same_table_request(*existing, param)) {
                return Status::INVALID_ARGUMENT(fmt::format(
                    "same operation_id {} carries different table spec",
                    param.operation_id));
            }
            return Status::OK();
        }
        return Status::ALREADY_EXIST(
            fmt::format("table_id {} already exists with operation_id {}",
                        param.table_id, existing->spec.operation_id));
    }

    Table table{
        .table_id = param.table_id,
        .spec{
            .table_name = param.table_name,
            .db_name = param.db_name,
            .db_id = param.db_id,
            .replica_count = param.replica_count,
            .shard_count = param.shard_count,
            .resource_pool = param.resource_pool,
            .operation_id = param.operation_id,
        },
        .state{
            .status = TableStatus::CREATEING,
            .lifecycle = TableLifecycle::CREATING,
        },
    };
    return workflow_->step(table);
}

Status TableService::get_table_status(const GetTableStatusParam& param,
                                      Table* table) {
    RETURN_IF_INVALID_PARAM(param)

    TablePtr existing;
    Status status = store_->get_table(param.table_id, existing);
    RETURN_IF_INVALID_STATUS(status)
    if (existing == nullptr) {
        return Status::TABLE_NOT_FOUND(
            fmt::format("table_id {} not found", param.table_id));
    }
    if (!param.operation_id.empty() &&
        existing->spec.operation_id != param.operation_id) {
        return Status::INVALID_ARGUMENT(
            fmt::format("operation_id mismatch for table_id {}", param.table_id));
    }
    if (table != nullptr) {
        *table = *existing;
    }
    return Status::OK();
}

}  // namespace adviskv::sdm