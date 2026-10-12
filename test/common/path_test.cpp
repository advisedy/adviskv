#include "common/path.h"

#include <filesystem>

#include <gtest/gtest.h>

namespace adviskv {
namespace {

namespace fs = std::filesystem;

TEST(PathTest, EmptyPathUsesProjectRoot) {
    EXPECT_EQ(path_from_project_root(fs::path{}), project_root_dir());
}

TEST(PathTest, RelativePathUsesProjectRootAndNormalizes) {
    EXPECT_EQ(path_from_project_root("build/../conf/meta.yaml"), project_root_dir() / "conf/meta.yaml");
}

TEST(PathTest, AbsolutePathDoesNotUseProjectRoot) {
    EXPECT_EQ(path_from_project_root("/data/storage/../meta"), fs::path("/data/meta"));
}

TEST(PathTest, AtomicReplacementAppendsSuffixWithoutChangingDirectory) {
    EXPECT_EQ(atomic_replace_temp_path("/data/meta/meta.json"), fs::path("/data/meta/meta.json.tmp"));
    EXPECT_EQ(atomic_replace_temp_path("wal.log"), fs::path("wal.log.tmp"));
    EXPECT_EQ(atomic_replace_temp_path("/data/snapshot.12.tmp"), fs::path("/data/snapshot.12.tmp.tmp"));
}

TEST(PathTest, LogFileCombinesConfiguredDirectoryAndFilename) {
    EXPECT_EQ(log_file("/logs/storage-1", "storage-1.log"), fs::path("/logs/storage-1/storage-1.log"));
    EXPECT_EQ(log_file("build/demo/logs/meta/", "meta.log"), fs::path("build/demo/logs/meta/meta.log"));
}

}  // namespace
}  // namespace adviskv
