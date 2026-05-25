#include <cstdlib>
#include <filesystem>
#include <string>

#include "common/log.h"
#include "common/status.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"

namespace {

void init_logger() {
    adviskv::LogConfig config;
    config.logger_name = "wal_crash_runner";
    config.log_level = "warn";
    config.log_to_console = true;
    config.log_to_file = false;
    adviskv::Logger::get_instance().init(config);
}

adviskv::storage::LogEntry make_entry(adviskv::Term term,
                                      adviskv::storage::LogIndex index,
                                      std::string key, std::string value) {
    return adviskv::storage::LogEntry{
        term, index, adviskv::storage::WriteOpType::PUT, std::move(key),
        std::move(value)};
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        return 2;
    }

    init_logger();

    const std::filesystem::path data_dir{argv[1]};
    const adviskv::ReplicaID replica_id{910, 1, 0};

    adviskv::storage::PersistEngine persist(data_dir.string(), replica_id);
    adviskv::Status status = persist.init();
    if (status.fail()) {
        return 3;
    }

    status = persist.append_wal(make_entry(2, 3, "crash-key", "crash-value"));
    if (status.fail()) {
        return 4;
    }

    return 0;
}