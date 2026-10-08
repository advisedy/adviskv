#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/crc.h"
#include "common/define.h"
#include "common/file.h"
#include "common/status.h"

namespace adviskv {

class BinaryWriter {
public:
    virtual ~BinaryWriter() = default;
    virtual Status write_raw(const void* data, size_t size) = 0;
};

class BinaryReader {
public:
    virtual ~BinaryReader() = default;
    virtual Status read_raw(void* data, size_t size) = 0;
};

class BufferWriter : public BinaryWriter {
public:
    template <typename T>
    void write_raw(const T& v) {
        static_assert(std::is_trivially_copyable_v<T>, "raw write needs a fixed-layout type; use binary::encode");
        auto* p = reinterpret_cast<const uint8_t*>(&v);
        buf_.insert(buf_.end(), p, p + sizeof(T));
    }

    Status write_raw(const void* data, size_t size) override {
        if (size == 0) {
            return Status::OK();
        }
        const auto* p = static_cast<const uint8_t*>(data);
        buf_.insert(buf_.end(), p, p + size);
        return Status::OK();
    }

    std::vector<uint8_t> take() { return std::move(buf_); }

    size_t size() const { return buf_.size(); }
    const uint8_t* data() const { return buf_.data(); }

private:
    std::vector<uint8_t> buf_;
};

class BufferReader : public BinaryReader {
public:
    explicit BufferReader(std::vector<uint8_t> data) : data_(std::move(data)) {}
    BufferReader(const uint8_t* data, size_t size) : data_(data, data + size) {}

    template <typename T>
    bool read_raw(T& v) {
        static_assert(std::is_trivially_copyable_v<T>, "raw read needs a fixed-layout type; use binary::decode");
        if (pos_ + sizeof(T) > data_.size()) return false;
        memcpy(&v, data_.data() + pos_, sizeof(T));
        pos_ += sizeof(T);
        return true;
    }

    Status read_raw(void* data, size_t size) override {
        if (pos_ + size > data_.size()) {
            return Status::ERROR("read past end of buffer");
        }
        memcpy(data, data_.data() + pos_, size);
        pos_ += size;
        return Status::OK();
    }

    bool is_end() const { return pos_ >= data_.size(); }
    size_t pos() const { return pos_; }

private:
    std::vector<uint8_t> data_;
    size_t pos_{0};
};

// 析构故意不 flush；fsync/close 之前必须显式 flush，不然缓冲里最后一段没出手。
class FileWriter : public BinaryWriter {
public:
    static constexpr size_t K_BUFFER_BYTES = 64 * 1024;

    // 借 file 的 fd 用，不持有；file 的生命周期必须盖过 writer。
    explicit FileWriter(const File& file) : file_(file) { buf_.reserve(K_BUFFER_BYTES); }

    Status write_raw(const void* data, size_t size) override {
        if (size == 0) {
            return Status::OK();
        }
        if (size >= K_BUFFER_BYTES) {
            RETURN_IF_INVALID_STATUS(flush())
            return file_.write(data, size);
        }
        if (buf_.size() + size > K_BUFFER_BYTES) {
            RETURN_IF_INVALID_STATUS(flush())
        }
        const auto* p = static_cast<const uint8_t*>(data);
        buf_.insert(buf_.end(), p, p + size);
        return Status::OK();
    }

    Status flush() {
        if (buf_.empty()) {
            return Status::OK();
        }
        RETURN_IF_INVALID_STATUS(file_.write(buf_.data(), buf_.size()))
        buf_.clear();
        return Status::OK();
    }

private:
    const File& file_;
    std::vector<uint8_t> buf_;
};

class ChecksumWriter : public BinaryWriter {
public:
    explicit ChecksumWriter(BinaryWriter& inner) : inner_(inner) {}

    Status write_raw(const void* data, size_t size) override {
        RETURN_IF_INVALID_STATUS(inner_.write_raw(data, size))
        crc_ = update_crc32(crc_, static_cast<const uint8_t*>(data), size);
        bytes_ += size;
        return Status::OK();
    }

    uint32_t result() const { return crc_; }
    size_t bytes() const { return bytes_; }

private:
    BinaryWriter& inner_;
    uint32_t crc_{0};
    size_t bytes_{0};
};

class FileReader : public BinaryReader {
public:
    explicit FileReader(const File& file) : file_(file) {}

    Status read_raw(void* data, size_t size) override { return file_.read(data, size); }

private:
    const File& file_;
};

}  // namespace adviskv
