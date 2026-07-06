#pragma once

#include <string>
#include <vector>

#include "common/model/type.h"
#include "e2e_options.h"
#include "sdk/client.h"

namespace adviskv::e2e {

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
std::vector<Key> make_case_keys(const std::string& prefix, int32_t key_count);

// 大批量场景按区间输出，避免每个 key 都刷一行 PASS。
bool write_kvs(sdk::KVClient* client, const Options& options,
               const std::string& name, const std::vector<KV>& kvs);

bool verify_kvs(sdk::KVClient* client, const Options& options,
                const std::string& name, const std::vector<KV>& kvs);

// 检测key的count是否合法
bool validate_key_count(const Options& options);

// client调用put，写入一堆数据
bool write_dataset(sdk::KVClient* client, const Options& options,
                   const std::string& prefix);

// client侧调用get，期待和write_dataset写的数据是一样的
bool verify_dataset(sdk::KVClient* client, const Options& options,
                    const std::string& prefix);

}  // namespace adviskv::e2e