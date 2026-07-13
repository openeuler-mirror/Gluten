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
#include <vector>
#include <filesystem>

#include "Udf.h"
#include "UdfLoader.h"
#include "type/data_type.h"

#define GLUTEN_EXPAND(x) x
#define GLUTEN_STRINGIFY(x) #x
#define GLUTEN_TOSTRING(x) GLUTEN_STRINGIFY(x)
#define GLUTEN_CONCAT(x, y) x##y

namespace {
void *loadSymFromLibrary(void *handle, const std::string &libPath, const std::string &func, bool throwIfNotFound = true)
{
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
            void *handle = dlopen(libPath.c_str(), RTLD_LAZY);
            handles_[libPath] = handle;
        }
        std::cout << "Successfully loaded udf library: " << libPath;
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
    std::vector<omniruntime::type::DataTypePtr> argTypes;
    argTypes.resize(numArgs);
    for (auto i = 0; i < numArgs; ++i) {
        argTypes[i] = parser_.parse(args[i]);
    }
    auto rowType = std::make_shared<omniruntime::type::RowType>(argTypes);
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
                if (entry.dataType == nullptr || (entry.numArgs > 0 && entry.argTypes == nullptr)) {
                    throw std::runtime_error("Invalid UDF entry: dataType is null, or argTypes is null with numArgs > 0");
                }
                auto dataType = toSubstraitTypeStr(entry.dataType);
                auto argTypes = toSubstraitTypeStr(entry.numArgs, entry.argTypes);
                signatures_.insert(std::make_shared<UdfSignature>(entry.name, dataType, argTypes, entry.variableArity,
                    entry.allowTypeConversion));
            }
            free(udfEntries);
        } else {
            std::cout << "No UDF found in " << libPath;
        }
    }
    return signatures_;
}

void UdfLoader::registerUdf() const
{
    for (const auto &item : handles_) {
        void *sym = loadSymFromLibrary(item.second, item.first, GLUTEN_TOSTRING(GLUTEN_REGISTER_UDF));
        auto registerUdf = reinterpret_cast<void (*)()>(sym);
        registerUdf();
    }
}

std::shared_ptr<UdfLoader> UdfLoader::getInstance()
{
    static auto instance = std::make_shared<UdfLoader>();
    return instance;
}
} // namespace gluten
