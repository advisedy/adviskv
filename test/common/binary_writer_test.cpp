#include "common/binary/base.h"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <unistd.h>
#include <gtest/gtest.h>

#include "common/binary/codec.h"
#include "common/crc.h"
#include "common/define.h"
#include "common/file.h"
#include "common/status.h"

namespace adviskv {
namespace {

std::filesystem::path make_tmp_dir(const std::string& tag) {
    static int seq = 0;
    std::filesystem::path dir = std::filesystem::temp_directory_path() /
                                ("binary_writer_test_" + std::to_string(::getpid()) + "_" + tag + "_" +
                                 std::to_string(seq++));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

// 读回整个文件比对用的：测试校验路径保持独立实现，不反向依赖被测的 File::read。
std::string read_all_raw(const std::filesystem::path& path) {
    std::string out;
    File file;
    bool exists = false;
    if (!File::open_for_read(path.string(), file, exists).ok() || !exists) {
        return out;
    }
    uint64_t size = 0;
    if (!file.size(size).ok()) {
        return out;
    }
    out.resize(static_cast<size_t>(size));
    if (size > 0 && !file.read(out.data(), static_cast<size_t>(size)).ok()) {
        out.clear();
    }
    return out;
}

void append_int32(std::string& out, int32_t v) {
    out.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

TEST(BinaryWriterTest, StringFormatIsLenPrefixed) {
    BufferWriter buf;
    ASSERT_TRUE(binary::encode(std::string("abc"), buf).ok());
    ASSERT_TRUE(binary::encode(std::string(), buf).ok());
    ASSERT_TRUE(binary::encode(std::string("\0\1\2", 3), buf).ok());

    BufferReader reader(buf.take());
    std::string value;
    ASSERT_TRUE(binary::decode(reader, value).ok());
    EXPECT_EQ(value, "abc");
    ASSERT_TRUE(binary::decode(reader, value).ok());
    EXPECT_TRUE(value.empty());
    ASSERT_TRUE(binary::decode(reader, value).ok());
    EXPECT_EQ(value, std::string("\0\1\2", 3));
    EXPECT_TRUE(reader.is_end());

    BufferReader poisoned(std::vector<uint8_t>{0xFF, 0xFF, 0xFF, 0xFF});
    EXPECT_TRUE(binary::decode(poisoned, value).fail());
}

TEST(BinaryWriterTest, FileWriterBufferedBytesEqualPlainStream) {
    const std::filesystem::path dir = make_tmp_dir("buffered");
    const std::filesystem::path file = dir / "data.bin";

    std::string expected;
    {
        File out;
        ASSERT_TRUE(File::open_for_write(file.string(), out).ok());
        FileWriter writer(out);

        for (int round = 0; round < 300; round++) {
            const std::string small = "small-" + std::to_string(round);
            ASSERT_TRUE(binary::encode(small, writer).ok());
            append_int32(expected, static_cast<int32_t>(small.size()));
            expected.append(small);

            if (round % 97 == 0) {
                const std::string big(FileWriter::K_BUFFER_BYTES + static_cast<size_t>(round),
                                      static_cast<char>('A' + round % 26));
                ASSERT_TRUE(writer.write_raw(big.data(), big.size()).ok());
                expected.append(big);
            }
        }
        ASSERT_TRUE(writer.flush().ok());
    }

    EXPECT_EQ(read_all_raw(file), expected);
    std::filesystem::remove_all(dir);
}

TEST(BinaryWriterTest, FileWriterDoesNotFlushOnDestruction) {
    const std::filesystem::path dir = make_tmp_dir("no_flush");
    const std::filesystem::path file = dir / "data.bin";
    {
        File out;
        ASSERT_TRUE(File::open_for_write(file.string(), out).ok());
        FileWriter writer(out);
        const char data[] = "pending";
        ASSERT_TRUE(writer.write_raw(data, sizeof(data)).ok());
    }
    EXPECT_EQ(std::filesystem::file_size(file), 0U);
    std::filesystem::remove_all(dir);
}

TEST(BinaryWriterTest, ChecksumWriterMatchesOneShotCrc) {
    const std::filesystem::path dir = make_tmp_dir("checksum");
    const std::filesystem::path file = dir / "data.bin";

    std::string payload;
    {
        File out;
        ASSERT_TRUE(File::open_for_write(file.string(), out).ok());
        FileWriter file_writer(out);
        ChecksumWriter checksum(file_writer);
        for (int i = 0; i < 50; i++) {
            const std::string piece = "piece-" + std::to_string(i);
            ASSERT_TRUE(binary::encode(piece, checksum).ok());
            append_int32(payload, static_cast<int32_t>(piece.size()));
            payload.append(piece);
        }
        EXPECT_EQ(checksum.bytes(), payload.size());
        EXPECT_EQ(checksum.result(),
                  compute_crc32(reinterpret_cast<const uint8_t*>(payload.data()), payload.size()));
        ASSERT_TRUE(file_writer.flush().ok());
    }

    EXPECT_EQ(read_all_raw(file), payload);
    std::filesystem::remove_all(dir);
}

TEST(BinaryWriterTest, FileReaderReportsEof) {
    const std::filesystem::path dir = make_tmp_dir("reader");
    const std::filesystem::path file = dir / "data.bin";
    {
        File out;
        ASSERT_TRUE(File::open_for_write(file.string(), out).ok());
        ASSERT_TRUE(out.write("ab", 2).ok());
    }
    File in;
    bool exists = false;
    ASSERT_TRUE(File::open_for_read(file.string(), in, exists).ok());
    ASSERT_TRUE(exists);
    FileReader reader(in);
    char buf[4] = {};
    ASSERT_TRUE(reader.read_raw(buf, 2).ok());
    EXPECT_EQ(std::string(buf, 2), "ab");
    EXPECT_EQ(reader.read_raw(buf, 2).code(), StatusCode::GET_EOF);
    std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace adviskv
