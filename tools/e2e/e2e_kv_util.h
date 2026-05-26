#pragma once

#include <string>
#include <vector>

#include "common/type.h"
#include "e2e_context.h"
#include "e2e_options.h"
#include "sdk/client.h"

namespace adviskv::e2e {

class E2EContext;

// 发起创建db
bool create_db(E2EContext* context, std::string* error);

// 发起创建table
bool create_table(E2EContext* context, std::string* error);

// 发起drop table
bool drop_table(E2EContext* context, std::string* error);

// 根据option直接构造出来client
sdk::KVClient make_kv_client(const Options& options);

// client 发起 get 操作，期待get到的值是expected
bool wait_get_value(sdk::KVClient* client, const Options& options,
                    const std::string& key, const std::string& expected);

// client发起get操作，起到key没有对应的value
bool wait_key_not_found(sdk::KVClient* client, const Options& options,
                        const std::string& key);

// 通过prifix和key的数量直接构造出来kvs
std::vector<KV> make_case_kvs(const std::string& prefix, int32_t key_count);

// 对于一个creating的table，等待到他状态是normal
bool wait_table_normal(E2EContext* context);

// 对于一个deleting的table，等待到他状态是deleted
bool wait_table_deleted(E2EContext* context);

// 对于一个route，等待到他的状态是ready
bool wait_route_has_leader(E2EContext* context, const Key& key,
                           sdk::RouteReplica* leader);
// 检测key的count是否合法
bool validate_key_count(const Options& options);

// 获取到route（当然是可用的
bool get_route(E2EContext* context, const Key& key, sdk::RouteInfo* route,
               std::string* error);

// create_db + create_table + wait_table_normal
bool prepare_table(E2EContext* context);

// client调用put，写入一堆数据
bool write_dataset(sdk::KVClient* client, const Options& options,
                   const std::string& prefix);

// client侧调用get，期待和write_dataset写的数据是一样的
bool verify_dataset(sdk::KVClient* client, const Options& options,
                    const std::string& prefix);

// print一下当前key对应的shard的leader的ip和port
bool print_current_leader(E2EContext* context, const std::string& key);

// prepare table + write_dataset
bool run_seed_case(const Options& options, const std::string& title,
                   const std::string& prefix, bool print_leader = false);

// wait table is normal + verify_dataset
// 并且再次尝试写入key和读取key，看看是否正常 (after_key_suffix非空的时候)
bool run_verify_case(const Options& options, const std::string& title,
                     const std::string& prefix,
                     const std::string& after_key_suffix = "");

}  // namespace adviskv::e2e