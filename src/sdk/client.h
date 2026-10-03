#pragma once

#include <string>
#include <string_view>

#include "common/model/model.h"
#include "common/status.h"
#include "common/types.h"
#include "sdk/config.h"
#include "sdk/meta_route_client.h"
#include "sdk/model.h"
#include "sdk/route_cache.h"
#include "sdk/storage_client.h"
namespace adviskv::sdk {

class KVClient {
public:
    explicit KVClient(const KVClientConf& conf);

    Status init();

    Status put(const Key& key, const Value& value);
    Status put(const Key& key, const Value& value, const std::string& request_id);
    Status get(const Key& key, Value* value);
    Status del(const Key& key);
    Status del(const Key& key, const std::string& request_id);
    Status cas(const Key& key, const Value& expected, const Value& value);
    Status cas(const Key& key, const Value& expected, const Value& value, const std::string& request_id);

private:
    enum class Operation {
        PUT,
        DELETE,
        GET,
        CAS,
    };

    struct RequestPolicy {
        Operation operation;
        const std::string& request_id;

        bool is_write_request() const {
            return operation == Operation::PUT || operation == Operation::DELETE || operation == Operation::CAS;
        }
    };

    static std::string_view operation_name(Operation operation);
    static void record_result(Operation operation, const Status& status);
    // write_uncertain：本请求过程中是否出现过写结果未决；决定是否收成 UNKNOWN。
    static Status finalize_status(const RequestPolicy& policy, Status status, bool write_uncertain);

    template <typename Send, typename Refresh>
    Status execute_with_retry(const RequestPolicy& policy, SteadyClock::time_point deadline, Send&& send,
                              Refresh&& refresh);

    Milliseconds remaining_rpc_timeout(SteadyClock::time_point deadline) const;

    Status resolve_route(const Key& key, RouteInfo* route, SteadyClock::time_point deadline);
    Status refresh_route(const Key& key, RouteInfo* route, SteadyClock::time_point deadline);

    template <typename Send>
    Status run_kv_request(Operation operation, const Key& key, Milliseconds total_timeout,
                          const std::string& request_id, Send&& send);

    KVClientConf conf_;
    MetaRouteClient meta_route_client_;
    RouteCache route_cache_;
    StorageClient storage_client_;
};

}  // namespace adviskv::sdk
