#include "storage/raft/raft_callback.h"

#include <grpcpp/create_channel.h>

namespace adviskv::storage {

RaftSender::RaftSender()
    : RaftSender(default_stub_factory) {}

RaftSender::RaftSender(StubFactory stub_factory)
    : stub_factory_(std::move(stub_factory)) {}

std::string RaftSender::target_of(const PeerMember& member) {
    return member.endpoint.ip + ":" + std::to_string(member.endpoint.port);
}

std::unique_ptr<rpc::StorageService::StubInterface>
RaftSender::default_stub_factory(const PeerMember& member) {
    auto channel = grpc::CreateChannel(target_of(member),
                                       grpc::InsecureChannelCredentials());
    return rpc::StorageService::NewStub(channel);
}

rpc::StorageService::StubInterface* RaftSender::stub_for(
    const PeerMember& member) const {
    const std::string target = target_of(member);
    std::scoped_lock lock(mutex_);
    auto it = stub_pool_.find(target);
    if (it != stub_pool_.end()) {
        return it->second.get();
    }

    auto stub = stub_factory_(member);
    auto* raw = stub.get();
    stub_pool_[target] = std::move(stub);
    return raw;
}

}  // namespace adviskv::storage