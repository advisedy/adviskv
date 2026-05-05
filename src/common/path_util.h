#pragma once

#include <filesystem>
#include <string>

#include "common/confmgr.h"

namespace adviskv::common {

inline std::filesystem::path project_root_dir() {
    return std::filesystem::path(ADVISKV_PROJECT_ROOT);
}

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

inline std::filesystem::path path_from_config(const std::string& key) {
    return path_from_project_root(
        (ConfMgr::get_instance().Get<std::string>(key)));
}

}  // namespace adviskv::common
