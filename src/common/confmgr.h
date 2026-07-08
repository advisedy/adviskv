#pragma once

#include <string>

#include <yaml-cpp/node/node.h>
#include <yaml-cpp/yaml.h>

#include "common/define.h"
#include "common/log.h"

namespace adviskv {

class ConfMgr {
public:
    DISALLOW_COPY_AND_ASSIGN(ConfMgr)

    static ConfMgr& get_instance() {
        static ConfMgr instance;
        return instance;
    }

    void LoadFromFile(const std::string& filename) { root_node_ = YAML::LoadFile(filename); }

    // 这个是带上默认值的，不会抛出异常
    template <typename T>
    T Get(const std::string& key, const T& default_value) const {
        std::istringstream stream(key);
        std::string part;
        YAML::Node node = YAML::Clone(root_node_);
        while (std::getline(stream, part, '.')) {
            if (!node || !node.IsMap()) return default_value;
            if (!node[part]) return default_value;
            node = node[part];
        }

        try {
            return node.as<T>();
        } catch (const YAML::BadConversion& e) {
            return default_value;
        }
    }

    // 这个是没有默认值的，如果配置项不存在或者类型不匹配会抛出异常
    template <typename T>
    T Get(const std::string& key) const {
        std::istringstream stream(key);
        std::string part;
        YAML::Node node = YAML::Clone(root_node_);
        while (std::getline(stream, part, '.')) {
            if (!node || !node.IsMap()) throw std::runtime_error("conf mgr: config node '" + part + "' is not a map");
            if (!node[part]) throw std::runtime_error("conf mgr: config node '" + part + "' does not exist");
            node = node[part];
        }
        return node.as<T>();
    }

private:
    ConfMgr() = default;

    YAML::Node root_node_;
};

#define CONF_GET_INT(...) (adviskv::ConfMgr::get_instance().Get<int>(__VA_ARGS__))
#define CONF_GET_DOUBLE(...) (adviskv::ConfMgr::get_instance().Get<double>(__VA_ARGS__))
#define CONF_GET_STR(...) (adviskv::ConfMgr::get_instance().Get<std::string>(__VA_ARGS__))
#define CONF_GET_BOOL(...) (adviskv::ConfMgr::get_instance().Get<bool>(__VA_ARGS__))

}  // namespace adviskv