
#pragma once
#include <cstdint>
#include <optional>
#include <string>

#include <fmt/core.h>
#include <fmt/format.h>

#include "common.pb.h"
#include "common/define.h"

namespace adviskv {
/*
0 - 999 共用
1000 - 1999 sdm特有
2000 - 2999 storage特有
3000 - 3999 meta 特有
4000 - 4999 sdk特有
5000 - 5999 持久化特有
*/

#define ADVISKV_STATUS_CODE_LIST(X)                                                \
    X(OK, 0)                                                                       \
    X(ERROR, 1)                                                                    \
    X(KEY_NOT_FOUND, 2)                                                            \
    X(INVALID_ARGUMENT, 3)                                                         \
    X(DB_NOT_FOUND, 4)                                                             \
    X(TABLE_NOT_FOUND, 5)                                                          \
    X(REPLICA_NOT_FOUND, 6)                                                        \
    X(REPLICA_MANAGER_NOT_FOUND, 7)                                                \
    X(NOT_SUPPORTED, 8)                                                            \
    X(ALREADY_EXIST, 9)                                                            \
    X(NO_STUB, 10) /*rpc层面的,没有stub*/                                          \
    X(RPC_ERROR, 11)                                                               \
    X(RETRY_ERROR, 12)                                                             \
    X(RESOURCE_EXHAUSTED, 13)                                                      \
    X(NOT_INIT, 14)                                                                \
    X(ROUTE_NOT_FOUND, 1000)                                                       \
    X(TABLE_META_CACHE_NOT_FOUND, 1001)                                            \
    X(DB_META_CACHE_NOT_FOUND, 1002)                                               \
    X(REPLICA_ERROR, 1003)                                                         \
    X(NOT_LEADER, 2000)                                                            \
    X(NOT_YET_COMMIT, 2001) /*请求已被leader接受，但返回时尚未确认提交，结果未决*/ \
    X(IS_RECOVERING, 2002)                                                         \
    X(NOT_VOTER, 2003)                                                             \
    X(UNKNOWN, 4000)      /*SDK 对外语义：请求结果未知，调用方应重试或读校验*/     \
    X(GET_EOF, 5000)      /*读到了eof*/                                            \
    X(PARTIAL_READ, 5001) /*读取到一半遇到eof*/

enum class StatusCode : int32_t {

#define X(name, code) name = code,
    ADVISKV_STATUS_CODE_LIST(X)
#undef X
};

class Status {
public:
    Status() : code_(StatusCode::OK) {}
    Status(StatusCode code) : code_(code) {}
    explicit Status(int32_t code) : code_(static_cast<StatusCode>(code)) {}
    Status(StatusCode code, const std::string& msg) : code_(code), msg_(msg) {}
    Status(int32_t code, const std::string& msg) : code_(to<StatusCode>(code)), msg_(msg) {}

    bool ok() const { return code_ == StatusCode::OK; }
    bool fail() const { return !ok(); }

    StatusCode code() const { return code_; }
    const std::string& msg() const { return msg_; }

    bool operator==(const Status& other) const { return code_ == other.code_; }
    DEFINE_OPERATOR_NOT_EQUAL(Status)

    std::string to_string() const {  // 这里fmt里如果想要输出{}的转义，需要搞两下
        return fmt::format("Status{{code={}, msg='{}'}}", static_cast<int>(code_), msg_);
    }

    void set_msg(const std::string& msg) { msg_ = msg; }

    void set_code(StatusCode code) { code_ = code; }

#define X(name, code)                                         \
    static Status name() { return Status(StatusCode::name); } \
    static Status name(const std::string& msg) { return Status(StatusCode::name, msg); }
    ADVISKV_STATUS_CODE_LIST(X)
#undef X

private:
    StatusCode code_;
    std::string msg_;
};

using StatusOr = std::optional<Status>;

inline int32_t to_rpc_code(StatusCode code) { return static_cast<int32_t>(code); }
namespace {

inline bool try_decode_status_code(int32_t code, StatusCode& out) {
    switch (code) {
#define X(name, value)          \
    case value:                 \
        out = StatusCode::name; \
        return true;
        ADVISKV_STATUS_CODE_LIST(X)
#undef X
        default:
            return false;
    }
}

inline void encode_status_to_base_rsp(pb::BaseRsp* out, const Status& status) {
    if (out == nullptr) return;
    out->set_code(to_rpc_code(status.code()));
    out->set_msg(status.msg());
}

}  // namespace

inline Status decode_base_rsp_status(const pb::BaseRsp& rsp) {
    StatusCode code = StatusCode::ERROR;
    if (!try_decode_status_code(rsp.code(), code)) {
        return Status::ERROR(fmt::format("unknown remote status code={}, msg={}", rsp.code(), rsp.msg()));
    }
    return Status{code, rsp.msg()};
}

// 专门给RPC的response用的
template <typename Response>
inline void fill_base_rsp(Response* rsp, const Status& status) {
    if (rsp == nullptr) return;
    encode_status_to_base_rsp(rsp->mutable_base_rsp(), status);
}

}  // namespace adviskv