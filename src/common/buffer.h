#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace adviskv {

class EncodeBuffer {
public:
    template <typename T>
    void write(const T& v) {
        auto* p = reinterpret_cast<const uint8_t*>(&v);
        buf_.insert(buf_.end(), p, p + sizeof(T));
    }

    void write(const std::string& s) {
        write<int32_t>(static_cast<int32_t>(s.size()));
        if (!s.empty()) {
            buf_.insert(buf_.end(), s.begin(), s.end());
        }
    }

    std::vector<uint8_t> take() { return std::move(buf_); }

    size_t size() const { return buf_.size(); }
    const uint8_t* data() const { return buf_.data(); }

private:
    std::vector<uint8_t> buf_;
};

class DecodeBuffer {
public:
    explicit DecodeBuffer(std::vector<uint8_t> data) : data_(std::move(data)) {}

    template <typename T>
    bool read(T& v) {
        if (pos_ + sizeof(T) > data_.size()) return false;
        memcpy(&v, data_.data() + pos_, sizeof(T));
        pos_ += sizeof(T);
        return true;
    }

    bool read(std::string& s) {
        int32_t len;
        if (!read(len)) return false;
        if (len < 0) return false;
        if (pos_ + static_cast<size_t>(len) > data_.size()) return false;
        if (len == 0) {
            s.clear();
            return true;
        }
        s.assign(reinterpret_cast<const char*>(data_.data() + pos_), len);
        pos_ += len;
        return true;
    }

    bool is_end() const { return pos_ >= data_.size(); }
    size_t pos() const { return pos_; }

private:
    std::vector<uint8_t> data_;
    size_t pos_{0};
};

}  // namespace adviskv
