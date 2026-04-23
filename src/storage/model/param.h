#pragma once

#include <cstdint>
#include "common/type.h"
#include "common/status.h"
namespace adviskv::storage{


using Term = int64_t;
using LogIndex = int64_t;

enum class WriteOpType{
    PUT = 0,
    DEL = 1,
    NONE = 2
};

struct RequestVoteParam{
    ReplicaID from_replica_id;
    ReplicaID to_replica_id;
    Term term;
    LogIndex last_log_index;
    Term last_log_term;
};

struct RequestVoteResult{
    Term term;
    bool vote_granted{false};
};

struct PeerMember{
    NodeID node_id;
    ReplicaID replica_id;
    Endpoint endpoint;
};

struct ReplicaInitParam{
    ReplicaID replica_id;
    EngineType engine_type;
    Endpoint local_enopoint;
    std::vector<PeerMember> members;
};

struct LogEntry {
    Term term{0};
    LogIndex index{0};
};

struct PutParam{
    const Key& key;
    const Value& value;

    Status validate() const {
        return Status::OK();
    }
};

struct GetParam{
    const Key& key;
    Status validate() const {
        return Status::OK();
    }    
};



}