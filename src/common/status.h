
#pragma once
#include <cstdint>
#include <fmt/format.h>
#include <string>
#include <fmt/core.h>


namespace adviskv{
    


enum class StatusCode : int32_t {
    // 共用
    OK = 0,
    ERROR = 1,
    KEY_NOT_FOUND = 2,
    INVALID_ARGUMENT = 3,
    DB_NOT_FOUND = 4,
    TABLE_NOT_FOUND = 5,
    REPLICA_NOT_FOUND = 6,
    REPLICA_MANAGER_NOT_FOUND = 7,
    NOT_SUPPORTED = 8,
    ALREADY_EXIST = 9,

    //sdm 特有
    ROUTE_NOT_FOUND = 1000,
    TABLE_META_CACHE_NOT_FOUND = 1001,
    DB_META_CACHE_NOT_FOUND = 1002,
    //meta 特有

    //storage 特有

    //sdk 特有
};


class Status{
public:
    
    Status() : code_(StatusCode::OK) {}
    Status(StatusCode code) : code_(code){}
    Status(int code) : code_(static_cast<StatusCode>(code)){}
    Status(StatusCode code, const std::string& msg) : code_(code), msg_(msg) {}

    bool ok() const { return code_ == StatusCode::OK; }
    bool fail() const {return !ok();}

    StatusCode code() const { return code_; }
    const std::string& msg() const { return msg_; }

    bool operator==(const Status& other) const {
        return code_ == other.code_;
    }

     bool operator!=(const Status& other) const {
        return !(*this == other);
    }

     std::string to_string() const {
        return fmt::format("Status{code={}, msg='{}'}", static_cast<int>(code_), msg_);
    }

    void set_msg(const std::string& msg) {
        msg_ = msg;
    }

     void set_code(StatusCode code) {
        code_ = code;
    }

    static Status OK() {
        return Status();
    }

private:

    StatusCode code_;
    std::string msg_;

};

inline int32_t to_rpc_code(StatusCode code){
    return static_cast<int32_t>(code);
}

template <typename Response>
inline void fill_base_rsp(Response* rsp, const Status& status) {
    auto* base_rsp = rsp->mutable_base_rsp();
    base_rsp->set_code(to_rpc_code(status.code()));
    base_rsp->set_msg(status.msg());
}





}