#include "meta/background/table_ddl_reconciler.h"

#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "meta/catalog/meta_types.h"
#include "sdm/model/store.h"

namespace adviskv::meta {

namespace {

bool is_sdm_phase(const SdmTableStatus& status, sdm::TablePhase phase) {
    return status.phase == static_cast<int32_t>(phase);
}

std::string fallback_msg(const std::string& msg, const char* fallback) {
    return msg.empty() ? fallback : msg;
}

void update_table_state_or_log(CatalogManager& catalog_manager,
                               const TableMeta& table, TableState state,
                               const std::string& last_error_msg,
                               const char* action) {
    Status status = catalog_manager.update_table_state(table.table_id, state,
                                                       last_error_msg);
    if (status.fail()) {
        LOG_WARN("{} failed, table_id={}, operation_id={}, msg={}", action,
                 table.table_id, table.operation_id, status.msg());
    }
}

void keep_table_state_with_error(CatalogManager& catalog_manager,
                                 const TableMeta& table, TableState state,
                                 const Status& status, const char* action) {
    LOG_WARN("{} failed, table_id={}, operation_id={}, msg={}", action,
             table.table_id, table.operation_id, status.msg());

    IGNORE_RESULT(catalog_manager.update_table_state(table.table_id, state,
                                                     status.to_string()));
}

enum class ReconcileAction {
    DONE,
    RESUBMIT,
    FAILED,
    WAIT,
    ERROR,
};

struct AddTablePolicy {
    static constexpr const char* source_name = "ADDING";
    static constexpr TableState source_state = TableState::ADDING;

    static constexpr TableState done_state = TableState::NORMAL;

    static constexpr const char* resubmit_action = "resubmit SDM place table";
    static constexpr const char* done_action = "mark table NORMAL";
    static constexpr const char* failed_msg = "SDM table placement failed";

    static Status resubmit(ISdmClient& client, const TableMeta& table) {
        return client.call_place_table(table);
    }

    static ReconcileAction decide(const Status& status,
                                  const SdmTableStatus& sdm_status) {
        if (status.code() == StatusCode::TABLE_NOT_FOUND) {
            return ReconcileAction::RESUBMIT;
        }
        if (status.fail()) {
            return ReconcileAction::ERROR;
        }
        if (is_sdm_phase(sdm_status, sdm::TablePhase::READY)) {
            return ReconcileAction::DONE;
        }
        if (is_sdm_phase(sdm_status, sdm::TablePhase::FAILED)) {
            return ReconcileAction::FAILED;
        }
        if (is_sdm_phase(sdm_status, sdm::TablePhase::CREATING)) {
            return ReconcileAction::WAIT;
        }
        return ReconcileAction::RESUBMIT;
    }
};

struct DropTablePolicy {
    static constexpr const char* source_name = "DROPPING";
    static constexpr TableState source_state = TableState::DROPPING;

    static constexpr TableState done_state = TableState::DELETED;

    static constexpr const char* resubmit_action = "resubmit SDM drop table";
    static constexpr const char* done_action = "mark table DELETED";
    static constexpr const char* failed_msg = "SDM table drop failed";

    static Status resubmit(ISdmClient& client, const TableMeta& table) {
        return client.call_drop_table(table);
    }

    static ReconcileAction decide(const Status& status,
                                  const SdmTableStatus& sdm_status) {
        if (status.code() == StatusCode::TABLE_NOT_FOUND) {
            return ReconcileAction::DONE;
        }
        if (status.fail()) {
            return ReconcileAction::ERROR;
        }
        if (is_sdm_phase(sdm_status, sdm::TablePhase::DELETED)) {
            return ReconcileAction::DONE;
        }
        if (is_sdm_phase(sdm_status, sdm::TablePhase::FAILED)) {
            return ReconcileAction::FAILED;
        }
        if (is_sdm_phase(sdm_status, sdm::TablePhase::DELETING)) {
            return ReconcileAction::WAIT;
        }
        return ReconcileAction::RESUBMIT;
    }
};

template <typename Policy>
void resubmit_or_record_error(CatalogManager& catalog_manager,
                              ISdmClient& sdm_client, const TableMeta& table) {
    Status resubmit_status = Policy::resubmit(sdm_client, table);
    if (resubmit_status.fail()) {
        keep_table_state_with_error(catalog_manager, table,
                                    Policy::source_state, resubmit_status,
                                    Policy::resubmit_action);
    }
}

template <typename Policy>
void reconcile_table(CatalogManager& catalog_manager, ISdmClient& sdm_client,
                     const TableMeta& table) {
    SdmTableStatus sdm_status;
    Status status = sdm_client.get_table_status(table, &sdm_status);
    ReconcileAction action = Policy::decide(status, sdm_status);

    switch (action) {
        case ReconcileAction::DONE:
            update_table_state_or_log(catalog_manager, table,
                                      Policy::done_state, "",
                                      Policy::done_action);
            return;
        case ReconcileAction::RESUBMIT:
            resubmit_or_record_error<Policy>(catalog_manager, sdm_client,
                                             table);
            return;
        case ReconcileAction::FAILED:
            update_table_state_or_log(
                catalog_manager, table, TableState::FAILED,
                fallback_msg(sdm_status.last_error_msg, Policy::failed_msg),
                "mark table FAILED");
            return;
        case ReconcileAction::ERROR:
            keep_table_state_with_error(catalog_manager, table,
                                        Policy::source_state, status,
                                        "get SDM table status");
            return;
        case ReconcileAction::WAIT:
            return;
    }
}

template <typename Policy>
void reconcile_tables(CatalogManager& catalog_manager, ISdmClient& sdm_client) {
    std::vector<TableMeta> tables;
    Status status =
        catalog_manager.list_tables_by_state(Policy::source_state, &tables);
    if (status.fail()) {
        LOG_WARN("list {} tables failed: {}", Policy::source_name,
                 status.msg());
        return;
    }

    for (const TableMeta& table : tables) {
        reconcile_table<Policy>(catalog_manager, sdm_client, table);
    }
}

}  // namespace

TableDdlReconciler::TableDdlReconciler(CatalogManager* catalog_manager,
                                       ISdmClient* sdm_client)
    : catalog_manager_(catalog_manager), sdm_client_(sdm_client) {}

void TableDdlReconciler::run() {
    if (!catalog_manager_ or !sdm_client_) {
        return;
    }

    reconcile_tables<AddTablePolicy>(*catalog_manager_, *sdm_client_);
    reconcile_tables<DropTablePolicy>(*catalog_manager_, *sdm_client_);
}

}  // namespace adviskv::meta