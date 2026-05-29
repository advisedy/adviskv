#include <cstdlib>
#include <filesystem>
#include <string>

#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/raft/state_machine/state_machine.h"

namespace {

void init_logger() {
    adviskv::LogConfig config;
    config.logger_name = "snapshot_crash_runner";
    config.log_level = "warn";
    config.log_to_console = true;
    config.log_to_file = false;
    adviskv::Logger::get_instance().init(config);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        return 2;
    }

    init_logger();

    const std::filesystem::path data_dir{argv[1]};
    adviskv::ReplicaID replica_id{101, 7, 2};
    adviskv::storage::PersistEngine persist{data_dir, replica_id};
    if (persist.init().fail()) {
        return 3;
    }
    adviskv::storage::KvStateMachine machine{adviskv::EngineType::MAP};
    const std::vector<adviskv::storage::LogEntry> entries = {
        adviskv::storage::LogEntry{1, 1, adviskv::storage::WriteOpType::PUT,
                                   "k1", "v1"},
        adviskv::storage::LogEntry{1, 2, adviskv::storage::WriteOpType::PUT,
                                   "k2", "v2"},
    };
    for (auto&& entry : entries) {
        if (machine.apply(entry).fail()) {
            return 4;
        }
    }
    persist.do_snapshot(machine);
    return 0;
}