#include "e2e_context.h"

#include <fmt/core.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <chrono>
#include <utility>

#include "e2e_assert.h"
#include "meta/catalog/meta_types.h"
#include "sdk/model.h"

namespace adviskv::e2e {

E2EContext::E2EContext(const Options& options) : options_(options) {
    const std::string meta_target =
        fmt::format("{}:{}", options_.meta_host, options_.meta_port);
    const std::string sdm_target =
        fmt::format("{}:{}", options_.sdm_host, options_.sdm_port);

    print_step(fmt::format("meta target: {}", meta_target));
    print_step(fmt::format("sdm target: {}", sdm_target));

    auto meta_channel =
        grpc::CreateChannel(meta_target, grpc::InsecureChannelCredentials());
    meta_stub_ = rpc::MetaService::NewStub(meta_channel);

    auto sdm_channel =
        grpc::CreateChannel(sdm_target, grpc::InsecureChannelCredentials());
    sdm_stub_ = rpc::ShardingManagerService::NewStub(sdm_channel);
}

}  // namespace adviskv::e2e