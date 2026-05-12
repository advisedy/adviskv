#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/model/param.h"

namespace adviskv::storage {

struct ReplicaMetaPayload {
    ReplicaInitParam init_param{};
};

class ReplicaMetaPersistEngine {
   public:
    static constexpr const char* kFileName = "replica_meta";

    explicit ReplicaMetaPersistEngine(std::string data_dir);

    Status save_replica_meta(const ReplicaMetaPayload& payload) const;
    Status load_replica_meta(const std::filesystem::path& meta_path,
                             ReplicaMetaPayload& payload) const;
    std::vector<std::filesystem::path> scan_replica_meta_files() const;

   private:
    std::filesystem::path replica_dir(const ReplicaID& replica_id) const;
    std::filesystem::path meta_path(const ReplicaID& replica_id) const;

    std::string data_dir_;
};

}  // namespace adviskv::storage
