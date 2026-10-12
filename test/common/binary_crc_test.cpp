#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "common/binary/codec.h"
#include "common/constants.h"
#include "common/crc.h"
#include "common/file.h"
#include "test/test_env.h"

namespace adviskv {
namespace {

template <typename T>
void append_raw(std::vector<uint8_t>& bytes, const T& value) {
    const auto* data = reinterpret_cast<const uint8_t*>(&value);
    bytes.insert(bytes.end(), data, data + sizeof(value));
}

// 测试用的记录头独立拼出来，免得编码和解码同时写错了还互相证明正确。
std::vector<uint8_t> make_record(const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> bytes;
    append_raw(bytes, static_cast<int32_t>(payload.size()));
    append_raw(bytes, compute_crc32(payload.data(), payload.size()));
    bytes.insert(bytes.end(), payload.begin(), payload.end());
    return bytes;
}

TEST(BinaryCrcTest, EncodingKeepsLengthCrcPayloadFormat) {
    const std::string value("a\0b", 3);
    std::vector<uint8_t> payload;
    append_raw(payload, static_cast<int32_t>(value.size()));
    payload.insert(payload.end(), value.begin(), value.end());

    BufferWriter writer;
    ASSERT_TRUE(binary::encode_crc(value, writer).ok());
    EXPECT_EQ(writer.take(), make_record(payload));
}

TEST(BinaryCrcTest, RegisteredModelUsesTheSameCodec) {
    LogEntry entry;
    entry.term = 3;
    entry.index = 7;
    entry.op_type = WriteOpType::PUT;
    entry.key = "key";
    entry.value = "value";
    entry.request_id = "request";

    BufferWriter plain;
    ASSERT_TRUE(binary::encode(entry, plain).ok());
    BufferWriter writer;
    ASSERT_TRUE(binary::encode_crc(entry, writer).ok());
    std::vector<uint8_t> bytes = writer.take();
    EXPECT_EQ(bytes, make_record(plain.take()));

    BufferReader reader(std::move(bytes));
    LogEntry restored;
    ASSERT_TRUE(binary::decode_crc(reader, restored).ok());
    EXPECT_EQ(restored, entry);
    EXPECT_EQ(binary::decode_crc(reader, restored).code(), StatusCode::GET_EOF);
}

TEST(BinaryCrcTest, EncodingRejectsOversizedPayloadBeforeWritingRecord) {
    const std::string value(static_cast<size_t>(K_MAX_FRAMED_PAYLOAD_BYTES), 'x');
    BufferWriter writer;
    EXPECT_TRUE(binary::encode_crc(value, writer).fail());
    EXPECT_EQ(writer.size(), 0U);
}

class FailingWriter : public BinaryWriter {
public:
    Status write_raw(const void*, size_t) override { return Status::ERROR("injected write failure"); }
};

class FailingReader : public BinaryReader {
public:
    Status read_raw(void*, size_t) override { return Status::ERROR("injected read failure"); }
};

TEST(BinaryCrcTest, PropagatesWriterFailure) {
    FailingWriter writer;
    EXPECT_EQ(binary::encode_crc(int32_t{7}, writer).msg(), "injected write failure");
}

TEST(BinaryCrcTest, PropagatesReaderFailure) {
    FailingReader reader;
    int32_t value{0};
    EXPECT_EQ(binary::decode_crc(reader, value).msg(), "injected read failure");
}

enum class ReaderKind { BUFFER, FILE };

class BinaryCrcReaderTest : public ::testing::TestWithParam<ReaderKind> {
protected:
    void SetUp() override {
        dir_ = test::make_unique_test_dir("binary_crc", sequence_++);
        ASSERT_TRUE(std::filesystem::create_directories(dir_));
    }

    void TearDown() override {
        reader_.reset();
        EXPECT_TRUE(file_.close().ok());
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    void set_bytes(const std::vector<uint8_t>& bytes) {
        reader_.reset();
        ASSERT_TRUE(file_.close().ok());
        if (GetParam() == ReaderKind::BUFFER) {
            reader_ = std::make_unique<BufferReader>(bytes);
            return;
        }

        const std::string path = (dir_ / "data.bin").string();
        File out;
        ASSERT_TRUE(File::open_for_write(path, out).ok());
        ASSERT_TRUE(out.write(bytes.data(), bytes.size()).ok());
        ASSERT_TRUE(out.close().ok());
        bool exists{false};
        ASSERT_TRUE(File::open_for_read(path, file_, exists).ok());
        ASSERT_TRUE(exists);
        reader_ = std::make_unique<FileReader>(file_);
    }

    static inline int sequence_{0};
    std::filesystem::path dir_;
    File file_;
    std::unique_ptr<BinaryReader> reader_;
};

TEST_P(BinaryCrcReaderTest, ReaderReportsEofAndConsumesPartialRead) {
    ASSERT_NO_FATAL_FAILURE(set_bytes({1, 2, 3}));
    uint8_t data[4] = {};
    EXPECT_EQ(reader_->read_raw(data, sizeof(data)).code(), StatusCode::PARTIAL_READ);
    EXPECT_EQ(data[0], 1);
    EXPECT_EQ(data[1], 2);
    EXPECT_EQ(data[2], 3);
    EXPECT_EQ(reader_->read_raw(data, 1).code(), StatusCode::GET_EOF);
    EXPECT_TRUE(reader_->read_raw(data, 0).ok());
}

TEST_P(BinaryCrcReaderTest, PlainDecodePropagatesReaderStatus) {
    int32_t value{0};
    ASSERT_NO_FATAL_FAILURE(set_bytes({}));
    EXPECT_EQ(binary::decode(*reader_, value).code(), StatusCode::GET_EOF);
    ASSERT_NO_FATAL_FAILURE(set_bytes({1, 2}));
    EXPECT_EQ(binary::decode(*reader_, value).code(), StatusCode::PARTIAL_READ);
}

TEST_P(BinaryCrcReaderTest, ReadsConsecutiveRecordsAndNormalEof) {
    const std::vector<std::string> values{"first", "", std::string("a\0b", 3)};
    BufferWriter writer;
    for (const std::string& value : values) {
        ASSERT_TRUE(binary::encode_crc(value, writer).ok());
    }
    ASSERT_NO_FATAL_FAILURE(set_bytes(writer.take()));
    std::string restored;
    for (const std::string& value : values) {
        ASSERT_TRUE(binary::decode_crc(*reader_, restored).ok());
        EXPECT_EQ(restored, value);
    }
    EXPECT_EQ(binary::decode_crc(*reader_, restored).code(), StatusCode::GET_EOF);
}

TEST_P(BinaryCrcReaderTest, EveryNonemptyTruncatedPrefixIsPartialRecord) {
    BufferWriter writer;
    ASSERT_TRUE(binary::encode_crc(std::string("payload"), writer).ok());
    const std::vector<uint8_t> bytes = writer.take();
    for (size_t size = 0; size < bytes.size(); ++size) {
        SCOPED_TRACE(size);
        ASSERT_NO_FATAL_FAILURE(set_bytes(std::vector<uint8_t>(bytes.begin(), bytes.begin() + size)));
        std::string value;
        EXPECT_EQ(binary::decode_crc(*reader_, value).code(),
                  size == 0 ? StatusCode::GET_EOF : StatusCode::PARTIAL_READ);
    }
}

TEST_P(BinaryCrcReaderTest, RejectsInvalidPayloadLengths) {
    for (int32_t length : {-1, 0, K_MAX_FRAMED_PAYLOAD_BYTES + 1}) {
        SCOPED_TRACE(length);
        std::vector<uint8_t> bytes;
        append_raw(bytes, length);
        ASSERT_NO_FATAL_FAILURE(set_bytes(bytes));
        int32_t value{0};
        EXPECT_EQ(binary::decode_crc(*reader_, value).code(), StatusCode::ERROR);
    }
}

TEST_P(BinaryCrcReaderTest, RejectsCrcMismatch) {
    BufferWriter writer;
    ASSERT_TRUE(binary::encode_crc(int32_t{7}, writer).ok());
    std::vector<uint8_t> bytes = writer.take();
    bytes.back() ^= 1;
    ASSERT_NO_FATAL_FAILURE(set_bytes(bytes));
    int32_t value{0};
    EXPECT_EQ(binary::decode_crc(*reader_, value).msg(), "crc mismatch");
}

TEST_P(BinaryCrcReaderTest, CompletePayloadDecodeFailureIsNotNormalEof) {
    // string 的长度字段完整，但缺正文；外层 CRC 仍然正确，不能当成文件读完了。
    std::vector<uint8_t> payload;
    append_raw(payload, int32_t{1});
    ASSERT_NO_FATAL_FAILURE(set_bytes(make_record(payload)));
    std::string value;
    EXPECT_EQ(binary::decode_crc(*reader_, value).code(), StatusCode::ERROR);
}

TEST_P(BinaryCrcReaderTest, CompletePayloadPartialFieldIsNotPartialRecord) {
    ASSERT_NO_FATAL_FAILURE(set_bytes(make_record({1, 2})));
    int32_t value{0};
    EXPECT_EQ(binary::decode_crc(*reader_, value).code(), StatusCode::ERROR);
}

TEST_P(BinaryCrcReaderTest, RejectsPayloadTrailingBytes) {
    std::vector<uint8_t> payload;
    append_raw(payload, int32_t{7});
    payload.push_back(1);
    ASSERT_NO_FATAL_FAILURE(set_bytes(make_record(payload)));
    int32_t value{0};
    EXPECT_EQ(binary::decode_crc(*reader_, value).msg(), "crc payload has trailing bytes");
}

INSTANTIATE_TEST_SUITE_P(
    Readers, BinaryCrcReaderTest, ::testing::Values(ReaderKind::BUFFER, ReaderKind::FILE),
    [](const ::testing::TestParamInfo<ReaderKind>& info) {
        return info.param == ReaderKind::BUFFER ? "Buffer" : "File";
    });

}  // namespace
}  // namespace adviskv
