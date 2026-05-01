#pragma once

#include <cstdint>
#include <vector>
#
namespace adviskv {

class EncodeBuffer {
   public:
    void write_int64(int64_t v) {
        auto* p = reinterpret_cast<uint8_t*>(&v);
        buf_.insert(buf_.end(), p, p + 8);
    }

    void write_int32(int32_t v) {
        auto* p = reinterpret_cast<uint8_t*>(&v);
        buf_.insert(buf_.end(), p, p + 4);
    }

    void write_str(const std::string& s) {
        write_int32(static_cast<int32_t>(s.size()));
        buf_.insert(buf_.end(), s.begin(), s.end());
    }

    void write_bool(bool v) {
        auto* p = reinterpret_cast<uint8_t*>(&v);
        buf_.insert(buf_.end(), p, p + 1);
    }

    void write_bytes(const uint8_t* data, size_t len) {
        buf_.insert(buf_.end(), data, data + len);
    }

    void insert_int32(int pos, int32_t v) {
        if (pos == buf_.size()) {
            write_int32(v);
        } else {
            auto* p = reinterpret_cast<uint8_t*>(&v);
            buf_.insert(buf_.begin() + pos, p, p + 4);
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

    bool read_int64(int64_t& v) {
        if (pos_ + 8 > data_.size()) return false;
        memcpy(&v, data_.data() + pos_, 8);
        pos_ += 8;
        return true;
    }

    bool read_int32(int32_t& v) {
        if (pos_ + 4 > data_.size()) return false;
        memcpy(&v, data_.data() + pos_, 4);
        pos_ += 4;
        return true;
    }

    bool read_str(std::string& s) {
        int32_t len;
        if (!read_int32(len)) return false;
        if (pos_ + len > data_.size()) return false;
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