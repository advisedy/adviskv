#pragma once

#include <memory>
#include <string>

#include "common/proto/proto.h"
#include "common/model/type.h"
#include "e2e_options.h"
#include "meta.grpc.pb.h"
#include "sdk/model.h"
#include "sdm.grpc.pb.h"

namespace adviskv::e2e {

class E2EContext {
public:
    explicit E2EContext(const Options& options);

    const Options& options() const { return options_; }

    meta_rpc::MetaService::Stub* meta() {
        return meta_stub_.get();
    }
    sdm_rpc::SdmService::Stub* sdm() { return sdm_stub_.get(); }

private:
    Options options_;
    std::unique_ptr<meta_rpc::MetaService::Stub> meta_stub_;
    std::unique_ptr<sdm_rpc::SdmService::Stub> sdm_stub_;
};

}  // namespace adviskv::e2e