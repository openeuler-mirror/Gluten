/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "util/config/ConfigBase.h"

namespace gluten {
// This kind string must be same with OmniBackend#name in java side.
inline static const std::string kOmniBackendKind{"omni"};

/// As a static instance in per executor, initialized at executor startup.
/// Should not put heavily work here.
class OmniBackend {
public:
    ~OmniBackend() {}

    static void create(const std::unordered_map<std::string, std::string> &conf);

private:
    explicit OmniBackend(const std::unordered_map<std::string, std::string> &conf)
    {
        init(conf);
    }

    void init(const std::unordered_map<std::string, std::string> &conf);

    void initUdf() const;

    static std::unique_ptr<OmniBackend> instance_;
    std::string cachePathPrefix_;
    std::string cacheFilePrefix_;

    std::shared_ptr<omniruntime::config::ConfigBase> backendConf_;
};
} // namespace gluten
