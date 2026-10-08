#pragma once

#include <string>

#include "common/status.h"
#include "storage/model/model.h"

namespace adviskv::storage {


class RaftMetaStore {
public:
    RaftMetaStore(std::string raft_meta_path, std::string dir_path);

    Status save(const RaftMeta& meta);
    Status load(RaftMeta& meta) const;

private:
    std::string raft_meta_path_;
    std::string dir_path_;
};

}  // namespace adviskv::storage