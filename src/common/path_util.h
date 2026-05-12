#pragma once

#include <filesystem>
#include <string>

#include "common/confmgr.h"

namespace adviskv {

inline std::filesystem::path project_root_dir() {
    return std::filesystem::path(ADVISKV_PROJECT_ROOT);
}

// 这个是直接从项目的根目录出发的路径， path填写是相对的
inline std::filesystem::path path_from_project_root(
    const std::filesystem::path& path) {
    if (path.empty()) {
        return project_root_dir();
    }
    if (path.is_absolute()) {
        return path.lexically_normal();
    }
    return (project_root_dir() / path).lexically_normal();
}

// 这个就是把conf里面的写的路径解析出来，直接转换成那种绝对的路径
inline std::filesystem::path path_from_config(const std::string& key) {
    return path_from_project_root(
        (ConfMgr::get_instance().Get<std::string>(key)));
}

}  // namespace adviskv
