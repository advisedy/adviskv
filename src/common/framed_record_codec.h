#pragma once

#include "common/buffer.h"
#include "common/crc.h"
#include "common/define.h"
#include "common/func.h"
#include "common/status.h"
#include "common/type.h"

#include <limits>

namespace adviskv {

/*

struct SomeCodec {
    using ObjectType = SomeObjectType;
    using LenType = int64;

    LenType max_payload_len() const {
    }

    void encode_payload(const ObjectType& obj, EncodeBuffer& buf) const {
    }

    Status decode_payload(DecodeBuffer& buf, ObjectType& obj) const {
        return Status::OK();
    }
};

Codec 只负责 payload 编解码；FramedRecord 负责 len + crc + payload 外层帧。
*/
template <class Codec>
class FramedRecord {
   public:
    using ObjectType = typename Codec::ObjectType;
    using LenType = typename Codec::LenType;

    static Status encode_to_fd(int fd, const ObjectType& obj) {
        EncodeBuffer payload;
        Codec codec;
        codec.encode_payload(obj, payload);

        if (payload.size() > static_cast<size_t>(codec.max_payload_len())) {
            return Status{StatusCode::ERROR,
                          fmt::format("payload too large: {}, max={}",
                                      payload.size(), codec.max_payload_len())};
        }

        LenType total_len = static_cast<LenType>(payload.size());
        uint32_t crc = compute_crc32(payload.data(), payload.size());

        RETURN_IF_INVALID_STATUS(func::write_value(fd, total_len))
        RETURN_IF_INVALID_STATUS(func::write_value(fd, crc))
        RETURN_IF_INVALID_STATUS(func::write_full(fd, payload.data(), payload.size()))
        return Status::OK();
    }

    static Status decode_from_fd(int fd, ObjectType& obj) {
        size_t unused_consumed_bytes{0};
        return decode_from_fd(fd, obj, unused_consumed_bytes);
    }

    static Status decode_from_fd(int fd, ObjectType& obj,
                                 size_t& consumed_bytes) {
        consumed_bytes = 0;
        Codec codec;
        LenType total_len = 0;
        Status len_status = func::read_value(fd, total_len);
        if (len_status.code() == StatusCode::GET_EOF) {
            return len_status;
        }
        RETURN_IF_INVALID_STATUS(len_status)

        if (total_len <= 0) {
            return Status{StatusCode::ERROR,
                          fmt::format("invalid total_len: {}", total_len)};
        }
        if (total_len > codec.max_payload_len()) {
            return Status{StatusCode::ERROR,
                          fmt::format("payload too large: {}, max={}", total_len,
                                      codec.max_payload_len())};
        }

        uint32_t stored_crc = 0;
        RETURN_IF_INVALID_STATUS(func::read_value(fd, stored_crc))

        std::vector<uint8_t> payload(static_cast<size_t>(total_len));
        RETURN_IF_INVALID_STATUS(func::read_full(fd, payload.data(), payload.size()))

        uint32_t actual_crc = compute_crc32(payload.data(), payload.size());
        if (actual_crc != stored_crc) {
            return Status{StatusCode::ERROR, "crc mismatch"};
        }

        DecodeBuffer buf(payload);
        Status decode_status = codec.decode_payload(buf, obj);
        RETURN_IF_INVALID_STATUS(decode_status)
        if (!buf.is_end()) {
            return Status{StatusCode::ERROR, "payload decode has trailing bytes"};
        }
        consumed_bytes =
            sizeof(LenType) + sizeof(uint32_t) + static_cast<size_t>(total_len);
        return Status::OK();
    }
};

}  // namespace adviskv
