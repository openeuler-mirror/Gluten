/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once

#include <google/protobuf/util/type_resolver_util.h>
#include "config.pb.h"

inline bool ParseProtobuf(const uint8_t *buf, int bufLen, google::protobuf::Message *msg)
{
    google::protobuf::io::CodedInputStream codedStream{buf, bufLen};
    // The default recursion limit is 100 which is too smaller for a deep
    // Substrait plan.
    codedStream.SetRecursionLimit(100000);
    return msg->ParseFromCodedStream(&codedStream);
}