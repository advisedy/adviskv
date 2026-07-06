#include "meta/background/table_ddl_reconciler.h"

#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "sdm/model/model.h"

namespace adviskv::meta {

namespace {

bool is_sdm_phase(const SdmTableStatus& status, SdmTablePhase phase) {
    return status.phase == phase;
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

/*
Policy:

前情提要：

在TableDdlReconciler::run里面添加新加入的Policy

source_state: （由于catalog_manager.list_tables_by_state(Policy::source_state,
&tables)，要利用这个函数去遍历所有处于我们这个状态的Table，所以需要添加source_state）

done_state: 规定好这个Policy认定的可以结束的状态

source_name
resubmit_action
done_action
failed_msg
以上这几个是给日志打印用的，随便填就好

static Status resubmit(ISdmClient& client, const TableMeta& table):
    代表你需要再一次用SdmClient执行一遍你的policy要代表的操作，
    例如AddTablePolicy的是client.call_place_table(table)

tatic ReconcileAction decide(const Status& status,const SdmTableStatus&
sdm_status)函数:
    前面的status代表的是get_table_status返回的status，后者是这次函数返回的 const
    SdmTableStatus&
    sdm_status，也就是说要实现一个decide函数，针对status和sdm_status进行判断，该进入ReconcileAction的哪一个环节
*/

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
        if (is_sdm_phase(sdm_status, SdmTablePhase::READY)) {
            return ReconcileAction::DONE;
        }
        if (is_sdm_phase(sdm_status, SdmTablePhase::FAILED)) {
            return ReconcileAction::FAILED;
        }
        if (is_sdm_phase(sdm_status, SdmTablePhase::CREATING)) {
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
        if (is_sdm_phase(sdm_status, SdmTablePhase::DELETED)) {
            return ReconcileAction::DONE;
        }
        if (is_sdm_phase(sdm_status, SdmTablePhase::FAILED)) {
            return ReconcileAction::FAILED;
        }
        if (is_sdm_phase(sdm_status, SdmTablePhase::DELETING)) {
            return ReconcileAction::WAIT;
        }
        return ReconcileAction::RESUBMIT;
    }
};

struct AlterTablePolicy {
    static constexpr const char* source_name = "ALTERING";
    static constexpr TableState source_state = TableState::ALTERING;

    static constexpr TableState done_state = TableState::NORMAL;

    static constexpr const char* resubmit_action =
        "resubmit SDM alter table replica_count";
    static constexpr const char* done_action =
        "mark table NORMAL, finish change replica count";
    static constexpr const char* failed_msg = "SDM table alter failed";

    static Status resubmit(ISdmClient& client, const TableMeta& table) {
        return client.call_alter_table_replica_count(table);
    }

    static ReconcileAction decide(const Status& status,
                                  const SdmTableStatus& sdm_status) {
        if (status.code() == StatusCode::TABLE_NOT_FOUND) {
            return ReconcileAction::FAILED;
        }
        if (status.fail()) {
            return ReconcileAction::ERROR;
        }
        if (is_sdm_phase(sdm_status, SdmTablePhase::READY)) {
            return ReconcileAction::DONE;
        }
        if (is_sdm_phase(sdm_status, SdmTablePhase::FAILED)) {
            return ReconcileAction::FAILED;
        }
        if (is_sdm_phase(sdm_status, SdmTablePhase::RESIZING)) {
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
            LOG_DEBUG(
                "{} table waits SDM convergence, table_id={}, "
                "operation_id={}, sdm_phase={}, sdm_last_error={}",
                Policy::source_name, table.table_id, table.operation_id,
                static_cast<int32>(sdm_status.phase),
                sdm_status.last_error_msg);
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
    reconcile_tables<AlterTablePolicy>(*catalog_manager_, *sdm_client_);
    reconcile_tables<DropTablePolicy>(*catalog_manager_, *sdm_client_);
}

}  // namespace adviskv::meta