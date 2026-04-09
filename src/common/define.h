#pragma once

// #define VALIDATE_BEGIN(T) \
//     Status validate() const {

// #define VALIDATE_END() \
//     return Status::OK();}
    
// 这里cond要填写应该合法的情况，如果cond与预期不符合，就会return，跟assert的语义有点像
#define RETURN_IF_INVALID_CONDITION(cond, msg) \
    if(!(cond)){ return Status{StatusCode::INVALID_ARGUMENT, msg};}

#define RETURN_IF_INVALID_STATUS(status) \
    if(status.fail()){return status;}

// #define RETURN_IF_INVALID_STATUS(one, cond) \
// if(Status one = cond; status.fail()){return status;}

#define RETURN_IF_INVALID_OPTION(option) \
    if(Status status = option.validate(); status.fail()) { \
        return status;  \
    }
