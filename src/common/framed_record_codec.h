#pragma once

#include <cstdint>
#include <vector>

#include "common/binary/codec.h"
#include "common/crash_injection.h"
#include "common/crc.h"
#include "common/define.h"
#include "common/file.h"
#include "common/status.h"

namespace adviskv {

template <typename ObjectType>
class FramedRecord {
public:
    static constexpr int32 K_MAX_FRAMED_PAYLOAD_BYTES = 64 * 1024 * 1024;

    static Status encode_to(const File& file, const ObjectType& obj) {
        BufferWriter payload;
        RETURN_IF_INVALID_STATUS(binary::encode(obj, payload))

        if (payload.size() > static_cast<size_t>(K_MAX_FRAMED_PAYLOAD_BYTES)) {
            return Status{StatusCode::ERROR, fmt::format("payload too large: {}, max={}", payload.size(),
                                                         K_MAX_FRAMED_PAYLOAD_BYTES)};
        }

        int32 total_len = static_cast<int32>(payload.size());
        uint32_t crc = compute_crc32(payload.data(), payload.size());

        RETURN_IF_INVALID_STATUS(file.write(&total_len, sizeof(total_len)))
        testhook::crash_point("framed_record.after_len_write");
        RETURN_IF_INVALID_STATUS(file.write(&crc, sizeof(crc)))
        testhook::crash_point("framed_record.after_crc_write");
        RETURN_IF_INVALID_STATUS(
                file.write(payload.data(), payload.size(), "framed_record.payload_write.after_partial_write"))
        testhook::crash_point("framed_record.after_payload_write");
        return Status::OK();
    }

    static Status decode_from(const File& file, ObjectType& obj) {
        size_t unused_consumed_bytes{0};
        return decode_from(file, obj, unused_consumed_bytes);
    }

    static Status decode_from(const File& file, ObjectType& obj, size_t& consumed_bytes) {
        consumed_bytes = 0;
        int32 total_len = 0;
        Status len_status = file.read(&total_len, sizeof(total_len));
        if (len_status.code() == StatusCode::GET_EOF) {
            return len_status;
        }
        RETURN_IF_INVALID_STATUS(len_status)

        if (total_len <= 0) {
            return Status{StatusCode::ERROR, fmt::format("invalid total_len: {}", total_len)};
        }
        if (total_len > K_MAX_FRAMED_PAYLOAD_BYTES) {
            return Status{StatusCode::ERROR, fmt::format("payload too large: {}, max={}", total_len,
                                                         K_MAX_FRAMED_PAYLOAD_BYTES)};
        }

        uint32_t stored_crc = 0;
        RETURN_IF_INVALID_STATUS(file.read(&stored_crc, sizeof(stored_crc)))

        std::vector<uint8_t> payload(static_cast<size_t>(total_len));
        RETURN_IF_INVALID_STATUS(file.read(payload.data(), payload.size()))

        uint32_t actual_crc = compute_crc32(payload.data(), payload.size());
        if (actual_crc != stored_crc) {
            return Status{StatusCode::ERROR, "crc mismatch"};
        }

        BufferReader buf(payload);
        RETURN_IF_INVALID_STATUS(binary::decode(buf, obj))
        if (!buf.is_end()) {
            return Status{StatusCode::ERROR, "payload decode has trailing bytes"};
        }
        consumed_bytes = sizeof(int32) + sizeof(uint32_t) + static_cast<size_t>(total_len);
        return Status::OK();
    }
};

}  // namespace adviskv
