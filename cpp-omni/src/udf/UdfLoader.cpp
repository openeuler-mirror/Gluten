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

#include <dlfcn.h>
#include <google/protobuf/arena.h>
#include <filesystem>
#include <limits>
#include <vector>

#include "Udf.h"
#include "Udaf.h"
#include "UdfLoader.h"
#include "type/data_type.h"

#define GLUTEN_EXPAND(x) x
#define GLUTEN_STRINGIFY(x) #x
#define GLUTEN_TOSTRING(x) GLUTEN_STRINGIFY(x)
#define GLUTEN_CONCAT(x, y) x##y

namespace {
void *loadSymFromLibrary(void *handle, const std::string &libPath, const std::string &func, bool throwIfNotFound = true)
{
    if (handle == nullptr) {
        throw std::runtime_error("Library handle is null for " + libPath + ": " + dlerror());
    }
    void *sym = dlsym(handle, func.c_str());
    if (!sym && throwIfNotFound) {
        throw std::runtime_error(func + " not found in " + libPath);
    }
    return sym;
}

std::vector<std::string> splitByDelim(const std::string &s, const char delimiter)
{
    if (s.empty()) {
        return {};
    }
    std::vector<std::string> result;
    size_t start = 0;
    size_t end = s.find(delimiter);

    while (end != std::string::npos) {
        result.push_back(std::string(s.substr(start, end - start)));
        start = end + 1;
        end = s.find(delimiter, start);
    }

    result.push_back(std::string(s.substr(start)));
    return result;
}

std::vector<std::string> splitPaths(const std::string &s, bool checkExists)
{
    if (s.empty()) {
        return {};
    }
    auto splits = splitByDelim(s, ',');
    std::vector<std::string> paths;
    for (auto i = 0; i < splits.size(); ++i) {
        if (!splits[i].empty()) {
            std::filesystem::path path(splits[i]);
            if (checkExists && !std::filesystem::exists(path)) {
                throw std::runtime_error("File path not exists: " + splits[i]);
            }
            if (path.is_relative()) {
                path = std::filesystem::current_path() / path;
            }
            paths.push_back(path.lexically_normal().generic_string());
        }
    }
    return paths;
}
} // namespace

namespace gluten {
void UdfLoader::loadUdfLibraries(const std::string &libPaths)
{
    const auto &paths = splitPaths(libPaths, /*checkExists=*/true);
    loadUdfLibrariesInternal(paths);
}

void UdfLoader::loadUdfLibrariesInternal(const std::vector<std::string> &libPaths)
{
    for (const auto &libPath : libPaths) {
        if (handles_.find(libPath) == handles_.end()) {
            dlerror();
            void *handle = dlopen(libPath.c_str(), RTLD_LAZY);
            if (handle == nullptr) {
                throw std::runtime_error("Failed to load udf library: " + libPath + ", error: " + dlerror());
            }
            handles_[libPath] = handle;
        }
    }
}

std::string UdfLoader::toSubstraitTypeStr(const std::string &type)
{
    auto returnType = parser_.parse(type);
    auto substraitType = convertor_.toSubstraitType(arena_, returnType);

    std::string output;
    substraitType.SerializeToString(&output);
    return output;
}

std::string UdfLoader::toSubstraitTypeStr(int32_t numArgs, const char **args)
{
    std::vector<DataTypePtr> argTypes;
    argTypes.resize(numArgs);
    for (auto i = 0; i < numArgs; ++i) {
        argTypes[i] = parser_.parse(args[i]);
    }
    auto rowType = std::make_shared<RowType>(argTypes);
    auto substraitType = convertor_.toSubstraitType(arena_, rowType);

    std::string output;
    substraitType.SerializeToString(&output);
    return output;
}

std::unordered_set<std::shared_ptr<UdfLoader::UdfSignature>> UdfLoader::getRegisteredUdfSignatures()
{
    if (!signatures_.empty()) {
        return signatures_;
    }
    for (const auto &item : handles_) {
        const auto &libPath = item.first;
        const auto &handle = item.second;

        // Handle UDFs.
        void *getNumUdfSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_NUM_UDF), false);
        if (getNumUdfSym) {
            auto getNumUdf = reinterpret_cast<int (*)()>(getNumUdfSym);
            int numUdf = getNumUdf();
            // allocate
            auto *udfEntries = static_cast<UdfEntry *>(malloc(sizeof(UdfEntry) * numUdf));
            if (udfEntries == nullptr) {
                throw std::runtime_error("malloc failed");
            }

            void *getUdfEntriesSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_UDF_ENTRIES));
            auto getUdfEntries = reinterpret_cast<void (*)(UdfEntry *)>(getUdfEntriesSym);
            getUdfEntries(udfEntries);

            for (auto i = 0; i < numUdf; ++i) {
                const auto &entry = udfEntries[i];
                auto dataType = toSubstraitTypeStr(entry.dataType);
                auto argTypes = toSubstraitTypeStr(entry.numArgs, entry.argTypes);
                signatures_.insert(std::make_shared<UdfSignature>(entry.name, dataType, argTypes, entry.variableArity,
                    entry.allowTypeConversion));
            }
            free(udfEntries);
        }

        // Handle UDAFs.
        void *getNumUdafSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_NUM_UDAF), false);
        if (getNumUdafSym) {
            auto getNumUdaf = reinterpret_cast<int (*)()>(getNumUdafSym);
            int numUdaf = getNumUdaf();
            if (numUdaf < 0) {
                throw std::runtime_error("Invalid UDAF entry count " + std::to_string(numUdaf) + " in " + libPath);
            }
            if (numUdaf == 0) {
                continue;
            }
            auto entryCount = static_cast<size_t>(numUdaf);
            if (entryCount > std::numeric_limits<size_t>::max() / sizeof(UdafEntry)) {
                throw std::runtime_error("UDAF entry count is too large in " + libPath);
            }
            auto *udafEntries = static_cast<UdafEntry *>(malloc(sizeof(UdafEntry) * entryCount));
            if (udafEntries == nullptr) {
                throw std::runtime_error("malloc failed");
            }

            void *getUdafEntriesSym =
                loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_UDAF_ENTRIES));
            auto getUdafEntries = reinterpret_cast<void (*)(UdafEntry *)>(getUdafEntriesSym);
            getUdafEntries(udafEntries);

            for (auto i = 0; i < numUdaf; ++i) {
                const auto &entry = udafEntries[i];
                if (entry.name == nullptr || entry.dataType == nullptr || entry.argTypes == nullptr) {
                    throw std::runtime_error("Invalid UDAF entry at index " + std::to_string(i) + " in " + libPath);
                }
                auto dataType = toSubstraitTypeStr(entry.dataType);
                auto argTypes = toSubstraitTypeStr(entry.numArgs, entry.argTypes);
                std::string intermediateType;
                if (entry.intermediateType != nullptr && entry.intermediateType[0] != '\0') {
                    intermediateType = toSubstraitTypeStr(entry.intermediateType);
                }
                signatures_.insert(std::make_shared<UdfSignature>(entry.name, dataType, argTypes, intermediateType,
                    entry.variableArity, entry.allowTypeConversion));
            }
            free(udafEntries);
        }
    }
    return signatures_;
}

std::unordered_set<std::string> UdfLoader::getRegisteredUdafNames()
{
    if (!names_.empty()) {
        return names_;
    }
    if (signatures_.empty()) {
        getRegisteredUdfSignatures();
    }
    for (const auto &sig : signatures_) {
        if (!sig->intermediateType.empty()) {
            names_.insert(sig->name);
        }
    }
    return names_;
}

bool UdfLoader::isRegisteredUdaf(const std::string &name)
{
    return getInstance()->getRegisteredUdafNames().count(name) > 0;
}

std::string UdfLoader::getRegisteredUdafIntermediateType(const std::string &name)
{
    auto loader = getInstance();
    if (loader->signatures_.empty()) {
        loader->getRegisteredUdfSignatures();
    }
    for (const auto &sig : loader->signatures_) {
        if (sig->name == name && !sig->intermediateType.empty()) {
            return sig->intermediateType;
        }
    }
    return "";
}

void UdfLoader::registerUdf()
{
    for (const auto &item : handles_) {
        void *sym = loadSymFromLibrary(item.second, item.first, GLUTEN_TOSTRING(GLUTEN_REGISTER_UDF), false);
        if (sym) {
            auto registerUdf = reinterpret_cast<void (*)()>(sym);
            registerUdf();
        }
    }
}

std::shared_ptr<UdfLoader> UdfLoader::getInstance()
{
    static auto instance = std::make_shared<UdfLoader>();
    return instance;
}
} // namespace gluten
