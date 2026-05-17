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

// 这个是专门给replica_manager用的，存储replica_meta的信息，方面recover的时候读取
// 所以和persist_engine是不一样的。persist_engine是专门给replica去处理raft_node,
// 快照等相关持久化的事情的
// 声明一下，这个目录的结构式， 传进来的data_dir下面，会放置replica_id为名字的目录，里面会有如下内容
// "wal.log";
// "raft_meta";
// "snapshot";
// replica_meta，前几个是persist干的，最后一个是这个persist干的
class ReplicaMetaPersistEngine {
   public:
    static constexpr const char* kFileName = "replica_meta";

    explicit ReplicaMetaPersistEngine(std::string data_dir);

    Status save_replica_meta(const ReplicaMetaPayload& payload) const;
    Status load_replica_meta(const std::filesystem::path& meta_path,
                             ReplicaMetaPayload& payload) const;
    Status load_replica_meta(const ReplicaID& replica_id,
                             ReplicaMetaPayload& payload) const;
    Status delete_replica_meta(const ReplicaID& replica_id) const;
    std::vector<std::filesystem::path> scan_replica_meta_files() const;

   private:
    std::filesystem::path replica_dir(const ReplicaID& replica_id) const;
    std::filesystem::path meta_path(const ReplicaID& replica_id) const;

    std::string data_dir_;
};

}  // namespace adviskv::storage
