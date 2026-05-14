
#pragma once
#include <fmt/core.h>
#include <fmt/format.h>

#include <cstdint>
#include <optional>
#include <string>

namespace adviskv {
/*
0 - 999 共用
1000 - 1999 sdm特有
2000 - 2999 storage特有
3000 - 3999 meta 特有
4000 - 4999 sdk特有
5000 - 5999 持久化特有
*/

#define ADVISKV_STATUS_CODE_LIST(X)                                \
    X(OK, 0)                                                       \
    X(ERROR, 1)                                                    \
    X(KEY_NOT_FOUND, 2)                                            \
    X(INVALID_ARGUMENT, 3)                                         \
    X(DB_NOT_FOUND, 4)                                             \
    X(TABLE_NOT_FOUND, 5)                                          \
    X(REPLICA_NOT_FOUND, 6)                                        \
    X(REPLICA_MANAGER_NOT_FOUND, 7)                                \
    X(NOT_SUPPORTED, 8)                                            \
    X(ALREADY_EXIST, 9)                                            \
    X(ROUTE_NOT_FOUND, 1000)                                       \
    X(TABLE_META_CACHE_NOT_FOUND, 1001)                            \
    X(DB_META_CACHE_NOT_FOUND, 1002)                               \
    X(NOT_LEADER, 2000)                                            \
    X(NOT_YET_COMMIT, 2001) /*目前的写操作成功了，但是还没有提交*/ \
    X(IS_RECOVERING, 2002)                                         \
    X(GET_EOF, 5000)      /*读到了eof*/                            \
    X(PARTIAL_READ, 5001) /*读取到一半遇到eof*/

enum class StatusCode : int32_t {

#define X(name, code) name = code,
    ADVISKV_STATUS_CODE_LIST(X)
#undef X
    // // 共用
    // OK = 0,
    // ERROR = 1,
    // KEY_NOT_FOUND = 2,
    // INVALID_ARGUMENT = 3,
    // DB_NOT_FOUND = 4,
    // TABLE_NOT_FOUND = 5,
    // REPLICA_NOT_FOUND = 6,
    // REPLICA_MANAGER_NOT_FOUND = 7,
    // NOT_SUPPORTED = 8,
    // ALREADY_EXIST = 9,

    // // sdm 特有
    // ROUTE_NOT_FOUND = 1000,
    // TABLE_META_CACHE_NOT_FOUND = 1001,
    // DB_META_CACHE_NOT_FOUND = 1002,

    // // storage 特有
    // NOT_LEADER = 2000,
    // NOT_YET_COMMIT = 2001,
    // // meta 特有

    // // sdk 特有

    // // 持久化特有
    // GET_EOF = 5000,
};

class Status {
   public:
    Status() : code_(StatusCode::OK) {}
    Status(StatusCode code) : code_(code) {}
    Status(int code) : code_(static_cast<StatusCode>(code)) {}
    Status(StatusCode code, const std::string& msg) : code_(code), msg_(msg) {}

    bool ok() const { return code_ == StatusCode::OK; }
    bool fail() const { return !ok(); }

    StatusCode code() const { return code_; }
    const std::string& msg() const { return msg_; }

    bool operator==(const Status& other) const { return code_ == other.code_; }

    bool operator!=(const Status& other) const { return !(*this == other); }

    std::string to_string() const {
        return fmt::format("Status{code={}, msg='{}'}", static_cast<int>(code_),
                           msg_);
    }

    void set_msg(const std::string& msg) { msg_ = msg; }

    void set_code(StatusCode code) { code_ = code; }

#define X(name, code)                                         \
    static Status name() { return Status(StatusCode::name); } \
    static Status name(const std::string& msg) {              \
        return Status(StatusCode::name, msg);                 \
    }
    ADVISKV_STATUS_CODE_LIST(X)
#undef X

    // static Status OK() { return Status(); }

    // static Status ERROR(const std::string& msg) {
    //     return Status{StatusCode::ERROR, msg};
    // }

   private:
    StatusCode code_;
    std::string msg_;
};

using StatusOr = std::optional<Status>;

inline int32_t to_rpc_code(StatusCode code) {
    return static_cast<int32_t>(code);
}

template <typename Response>
inline void fill_base_rsp(Response* rsp, const Status& status) {
    auto* base_rsp = rsp->mutable_base_rsp();
    base_rsp->set_code(to_rpc_code(status.code()));
    base_rsp->set_msg(status.msg());
}

}  // namespace adviskv