#pragma once
#include <unistd.h>

#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "common/buffer.h"
#include "common/define.h"
#include "common/status.h"
namespace adviskv {

namespace func {

inline int64_t get_current_ts_ms() {
    return std::chrono::duration_cast<Milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline int32_t get_random_int32(int32_t down, int32_t up) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int32_t> dist(down, up);
    return dist(gen);
}

template <typename T, typename U>
inline void ad_erase_if(std::vector<T>& a, U f) {
    for (auto it = a.begin(); it != a.end();) {
        if (f(*it)) {
            it = a.erase(it);
        } else {
            it++;
        }
    }
}

inline Status write_full(int fd, const void* buf, size_t len) {
    const char* b = static_cast<const char*>(buf);
    size_t have_write_len = 0;
    while (have_write_len < len) {
        ssize_t cur_write_len =
            ::write(fd, b + have_write_len, len - have_write_len);
        if (cur_write_len <= 0) {
            return StatusCode::ERROR;
        }
        have_write_len += (size_t)cur_write_len;
    }
    return Status::OK();
}

template <typename T>
inline Status write_value(int fd, const T& v) {
    return write_full(fd, &v, sizeof(T));
}

inline Status read_full(int fd, void* buf, size_t len) {
    char* b = static_cast<char*>(buf);
    size_t have_read_len = 0;
    while (have_read_len < len) {
        ssize_t cur_read_len =
            ::read(fd, b + have_read_len, len - have_read_len);
        if (cur_read_len == 0 and have_read_len == 0) {
            return StatusCode::GET_EOF;
        }
        if (cur_read_len == 0) {
            return StatusCode::PARTIAL_READ;
        }
        if (cur_read_len < 0) {
            if (errno == EINTR) {
                continue;
            }
            return StatusCode::ERROR;
        }
        have_read_len += cur_read_len;
    }
    return Status::OK();
}

inline std::optional<DecodeBuffer> read_full2buffer(int fd, size_t len) {
    std::vector<uint8_t> data(len);
    Status status = read_full(fd, data.data(), data.size());
    if (status.fail() && status.code() != StatusCode::GET_EOF) {
        return std::nullopt;
    }
    return DecodeBuffer{std::move(data)};
}

template <typename T>
inline Status read_value(int fd, T& v) {
    return read_full(fd, &v, sizeof(T));
}

inline Status write_string(int fd, const std::string& s) {
    int32_t len = static_cast<int32_t>(s.size());
    RETURN_IF_INVALID_STATUS(write_value(fd, len))
    if (len == 0) return Status::OK();
    return write_full(fd, s.data(), static_cast<size_t>(len));
}

inline Status read_string(int fd, std::string& s) {
    int32_t len{0};
    RETURN_IF_INVALID_STATUS(read_value(fd, len))
    if (len < 0) {
        return Status{StatusCode::ERROR, "invalid string len"};
    }
    if (len == 0) {
        s.clear();
        return Status::OK();
    }
    s.resize(len);
    return read_full(fd, s.data(), static_cast<size_t>(len));
}

}  // namespace func

}  // namespace adviskv
