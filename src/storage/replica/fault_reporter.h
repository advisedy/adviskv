#pragma once

#include <functional>
#include <utility>

#include "common/status.h"

namespace adviskv::storage {

class FaultReporter {
public:
    using FailIfCallback = std::function<Status(Status)>;
    using ReportCallback = std::function<void(Status)>;

    FaultReporter() = default;
    FaultReporter(FailIfCallback fail_if, ReportCallback report)
            : fail_if_(std::move(fail_if)), report_(std::move(report)) {}

    Status fail_if(Status status) const {
        if (status.ok() || !fail_if_) {
            return status;
        }
        return fail_if_(std::move(status));
    }

    void report(Status status) const {
        if (report_) {
            report_(std::move(status));
        }
    }

private:
    FailIfCallback fail_if_;
    ReportCallback report_;
};

}
