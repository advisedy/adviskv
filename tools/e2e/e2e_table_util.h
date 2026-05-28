#pragma once

#include <string>

#include "e2e_context.h"

namespace adviskv::e2e {

// 发起创建db
bool create_db(E2EContext* context, std::string* error);

// 发起创建table
bool create_table(E2EContext* context, std::string* error);

// 发起drop table
bool drop_table(E2EContext* context, std::string* error);

// 对于一个creating的table，等待到他状态是normal
bool wait_table_normal(E2EContext* context);

// 对于一个deleting的table，等待到他状态是deleted
bool wait_table_deleted(E2EContext* context);

// create_db + create_table + wait_table_normal
bool prepare_table(E2EContext* context);

// drop_table + wait_table_deleted
bool remove_table(E2EContext* context);

}  // namespace adviskv::e2e