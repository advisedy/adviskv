#pragma once

// 全项目唯一的文件 IO 入口：一个拥有 fd 的 RAII File。
//
//   - 所有权：move-only，析构自动 close；在乎 close 错误的关键路径（tmp 文件
//     rename 之前）走显式 close()。
//   - 错误：统一 Status，文案自带 路径 + strerror。
//   - EOF 语义：read/pread 一开始就到 EOF 返回 GET_EOF，读到一半返回
//     PARTIAL_READ，WAL / framed record 的解码循环靠这两个码停。
//   - 不做缓冲（binary/base.h 的 FileWriter/FileReader 借 file 引用干这事），
//     不做 pwrite / ftruncate（全项目没有调用方，需要时照对称接口补）。
//   - 就是 fd 的值包装，不同步；谁持有谁负责串行化。

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <utility>

#include <fcntl.h>
#include <fmt/format.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/crash_injection.h"
#include "common/defer.h"
#include "common/define.h"
#include "common/status.h"

namespace adviskv {

// 默认权限
constexpr mode_t K_DEFAULT_FILE_MODE = 0644;

class File {
public:

    // 如果path 已经有文件，会清空
    static Status open_for_write(const std::string& path, File& out) {
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, K_DEFAULT_FILE_MODE);
        if (fd < 0) {
            const int saved = errno;
            return Status::ERROR(fmt::format("failed to open for write: {}: {}", path, strerror(saved)));
        }
        out = File(fd, path);
        return Status::OK();
    }

    static Status open_for_append(const std::string& path, File& out) {
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, K_DEFAULT_FILE_MODE);
        if (fd < 0) {
            const int saved = errno;
            return Status::ERROR(fmt::format("failed to open for append: {}: {}", path, strerror(saved)));
        }
        out = File(fd, path);
        return Status::OK();
    }

    // 文件如果没有，Status也是OK， 会更改exists
    static Status open_for_read(const std::string& path, File& out, bool& exists) {
        exists = false;
        out = File();
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            if (errno == ENOENT) {
                return Status::OK();
            }
            const int saved = errno;
            return Status::ERROR(fmt::format("failed to open for read: {}: {}", path, strerror(saved)));
        }
        out = File(fd, path);
        exists = true;
        return Status::OK();
    }

    File() = default;
    File(File&& other) noexcept : fd_(other.fd_), path_(std::move(other.path_)) { other.fd_ = -1; }

    File& operator=(File&& other) noexcept {
        if (this != &other) {
            reset();
            fd_ = other.fd_;
            path_ = std::move(other.path_);
            other.fd_ = -1;
        }
        return *this;
    }
    DISALLOW_COPY_AND_ASSIGN(File)

    ~File() { reset(); }

    // 显式关闭并检查 close 返回值；析构走 reset()，错误没法上报只能忽略。
    Status close() {
        if (fd_ < 0) {
            return Status::OK();
        }
        int fd = fd_;
        fd_ = -1;
        if (::close(fd) != 0) {
            const int saved = errno;
            return Status::ERROR(fmt::format("failed to close: {}: {}", path_, strerror(saved)));
        }
        return Status::OK();
    }

    bool is_open() const { return fd_ >= 0; }

    int fd() const { return fd_; }

    const std::string& path() const { return path_; }

    // 使用时，大部分情况忽略第三个参数，第三个参数是为了注入崩溃添加的
    Status write(const void* data, size_t len, const char* crash_point_name = nullptr) const {
        const char* b = static_cast<const char*>(data);
        size_t done = 0;
        while (done < len) {
            size_t once = len - done;
            if (testhook::crash_point_enabled(crash_point_name) && once > 1) {
                once = 1;
            }
            ssize_t n = ::write(fd_, b + done, once);
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n < 0) {
                return err("write");
            }
            if (n == 0) {
                return Status::ERROR(fmt::format("write returned 0: {}", path_));
            }
            done += static_cast<size_t>(n);
            testhook::crash_point(crash_point_name);
        }
        return Status::OK();
    }

    Status read(void* buf, size_t len) const {
        if (len == 0) {
            return Status::OK();
        }
        char* b = static_cast<char*>(buf);
        size_t done = 0;
        while (done < len) {
            ssize_t n = ::read(fd_, b + done, len - done);
            if (n == 0) {
                return read_eof("read", done, len);
            }
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return err("read");
            }
            done += static_cast<size_t>(n);
        }
        return Status::OK();
    }

    // 不扰动当前位置的read
    Status pread(uint64_t offset, void* buf, size_t len) const {
        if (len == 0) {
            return Status::OK();
        }
        char* b = static_cast<char*>(buf);
        size_t done = 0;
        off_t pos = static_cast<off_t>(offset);
        while (done < len) {
            ssize_t n = ::pread(fd_, b + done, len - done, pos);
            if (n == 0) {
                return read_eof("pread", done, len);
            }
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return err("pread");
            }
            done += static_cast<size_t>(n);
            pos += n;
        }
        return Status::OK();
    }

    Status seek(uint64_t offset) const {
        if (::lseek(fd_, static_cast<off_t>(offset), SEEK_SET) < 0) {
            return err("lseek");
        }
        return Status::OK();
    }

    // 从当前位置向前跳。
    Status skip(uint64_t bytes) const {
        if (bytes == 0) {
            return Status::OK();
        }
        if (::lseek(fd_, static_cast<off_t>(bytes), SEEK_CUR) < 0) {
            return err("lseek");
        }
        return Status::OK();
    }

    Status fsync() const {
        while (true) {
            if (::fsync(fd_) == 0) {
                return Status::OK();
            }
            if (errno == EINTR) {
                continue;
            }
            return err("fsync");
        }
    }

    Status fsync_parent_dir() const {
        std::filesystem::path parent = std::filesystem::path(path_).parent_path();
        if (parent.empty()) {
            return Status::ERROR(fmt::format("no parent dir to fsync: {}", path_));
        }
        return fsync_dir(parent.string());
    }

    // 拿文件大小。fstat 查的是这个 fd 打开那一刻的文件本身，之后路径被
    // rename/替换也影响不到它
    Status size(uint64_t& out) const {
        struct stat st {};
        if (::fstat(fd_, &st) != 0) {
            return err("fstat");
        }
        out = static_cast<uint64_t>(st.st_size);
        return Status::OK();
    }

    static Status fsync_dir(const std::string& dir_path) {
        int dir_fd = ::open(dir_path.c_str(), O_RDONLY | O_DIRECTORY);
        if (dir_fd < 0) {
            const int saved = errno;
            return Status::ERROR(fmt::format("failed to open dir for fsync: {}: {}", dir_path, strerror(saved)));
        }
        auto guard = Defer([&dir_fd]() {
            if (dir_fd != -1) {
                ::close(dir_fd);
            }
        });
        while (true) {
            if (::fsync(dir_fd) == 0) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            const int saved = errno;
            return Status::ERROR(fmt::format("failed to fsync dir: {}: {}", dir_path, strerror(saved)));
        }
        if (::close(dir_fd) != 0) {
            dir_fd = -1;
            const int saved = errno;
            return Status::ERROR(fmt::format("failed to close dir after fsync: {}: {}", dir_path, strerror(saved)));
        }
        dir_fd = -1;
        return Status::OK();
    }

    static Status rename(const std::string& from, const std::string& to) {
        if (::rename(from.c_str(), to.c_str()) != 0) {
            const int saved = errno;
            return Status::ERROR(fmt::format("failed to rename {} -> {}: {}", from, to, strerror(saved)));
        }
        return Status::OK();
    }

    // 整读进 string；超 max_bytes 直接报错。
    // 文件不存在时返回 OK 且 exists=false。
    static Status read_to_string(const std::string& path, size_t max_bytes, std::string& out, bool& exists) {
        out.clear();
        File file;
        RETURN_IF_INVALID_STATUS(open_for_read(path, file, exists))
        if (!exists) {
            return Status::OK();
        }
        uint64_t file_size{0};
        RETURN_IF_INVALID_STATUS(file.size(file_size))
        if (file_size > max_bytes) {
            return Status::ERROR(fmt::format("file too large: {} bytes, max {}: {}", file_size, max_bytes, path));
        }
        out.resize(static_cast<size_t>(file_size));
        if (file_size > 0) {
            RETURN_IF_INVALID_STATUS(file.read(out.data(), static_cast<size_t>(file_size)))
        }
        return Status::OK();
    }

    // 原子替换：写 path+".tmp"，fsync +  close 后 rename ，并且父目录 flush。
    // 读者看到的 path 永远要么旧完整版要么新完整版。失败时顺手把 tmp 清掉。
    template <typename Func>
    static Status atomic_replace(const std::string& path, Func write_to_file) {
        std::filesystem::path p(path);
        if (p.parent_path().empty()) {
            return Status::ERROR(fmt::format("atomic_replace needs a parent dir: {}", path));
        }
        try {
            std::filesystem::create_directories(p.parent_path());
        } catch (const std::exception& e) {
            return Status::ERROR(fmt::format("create dir {} failed: {}", p.parent_path().string(), e.what()));
        }

        const std::string tmp_path = path + ".tmp";
        File tmp;
        RETURN_IF_INVALID_STATUS(open_for_write(tmp_path, tmp))
        auto remove_tmp = Defer([&tmp_path]() {
            std::error_code ec;
            std::filesystem::remove(tmp_path, ec);
        });

        RETURN_IF_INVALID_STATUS(write_to_file(tmp))
        RETURN_IF_INVALID_STATUS(tmp.fsync())
        RETURN_IF_INVALID_STATUS(tmp.close())
        RETURN_IF_INVALID_STATUS(rename(tmp_path, path))
        RETURN_IF_INVALID_STATUS(fsync_dir(p.parent_path().string()))
        return Status::OK();
    }

private:
    File(int fd, std::string path) : fd_(fd), path_(std::move(path)) {}

    Status err(const char* op, int saved_errno) const {
        return Status::ERROR(fmt::format("failed to {}: {}: {}", op, path_, strerror(saved_errno)));
    }
    Status err(const char* op) const { return err(op, errno); }

    Status read_eof(const char* op, size_t done, size_t len) const {
        if (done == 0) {
            return Status::GET_EOF(fmt::format("{} hit eof: {}", op, path_));
        }
        return Status::PARTIAL_READ(fmt::format("{} {} of {} bytes before eof: {}", op, done, len, path_));
    }

    void reset() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    int fd_{-1};
    std::string path_;
};

}  // namespace adviskv
