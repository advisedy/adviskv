#pragma once

#include "common/status.h"
#include "storage/model/param.h"
namespace adviskv::storage{

class RaftNode{

public:
    Status request_vote(const RequestVoteParam& param, RequestVoteResult& res);

};


}
