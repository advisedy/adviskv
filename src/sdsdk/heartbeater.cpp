#pragma once

// class HeartBeater{

// };

#include "sdsdk/heartbeater.h"
namespace adviskv::sdsdk {

HeartBeater::HeartBeater(StorageCallbackPtr callback) : callback_(callback) {}

}  // namespace adviskv::sdsdk