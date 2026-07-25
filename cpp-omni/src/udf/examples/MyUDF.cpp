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

#include <vectorization/VectorFunction.h>
#include <vectorization/registration/SimpleFunctionRegistry.h>
#include <type/data_type.h>
#include <iostream>
#include "udf/Udf.h"
#include "udf/examples/UdfCommon.h"

using namespace omniruntime::vectorization;
using namespace omniruntime::type;

namespace {
static const char *kInteger = "int";
static const char *kBigInt = "bigint";
static const char *kDate = "date";
static const char *kVarChar = "varchar";

namespace hivestringstring {
template <typename T>
struct HiveStringStringFunction {
    ALWAYS_INLINE omniruntime::vectorization::Status call(
        std::string &result, const std::string_view &a, const std::string_view &b)
    {
        result.append(a);
        result.append("---");
        result.append(b);
        return omniruntime::vectorization::Status::OK();
    }
};

// name: org.apache.spark.sql.hive.execution.UDFStringString
// signatures:
//    varchar, varchar -> varchar
// type: SimpleFunction
class HiveStringStringRegisterer final : public gluten::UdfRegisterer {
public:
    int getNumUdf() override
    {
        return 1;
    }

    void populateUdfEntries(int &index, gluten::UdfEntry *udfEntries) override
    {
        // Set `allowTypeConversion` for hive udf.
        udfEntries[index++] = {name_.c_str(), kVarChar, 1, arg_, false, true};
    }

    void registerSignatures() override
    {
        RegisterFunction<HiveStringStringFunction, std::string, std::string_view, std::string_view>(name_,
            {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR);
    }

private:
    const std::string name_ = "hive_string_string";
    const char *arg_[2] = {kVarChar};
};
} // namespace hivestringstring

std::vector<std::shared_ptr<gluten::UdfRegisterer>> &globalRegisters()
{
    static std::vector<std::shared_ptr<gluten::UdfRegisterer>> registerers;
    return registerers;
}

void setupRegisterers()
{
    static bool inited = false;
    if (inited) {
        return;
    }
    auto &registerers = globalRegisters();
    registerers.push_back(std::make_shared<hivestringstring::HiveStringStringRegisterer>());
    inited = true;
}
} // namespace

DEFINE_GET_NUM_UDF
{
    setupRegisterers();
    int numUdf = 0;
    for (const auto &registerer : globalRegisters()) {
        numUdf += registerer->getNumUdf();
    }
    return numUdf;
}

DEFINE_GET_UDF_ENTRIES
{
    setupRegisterers();
    int index = 0;
    for (const auto &registerer : globalRegisters()) {
        registerer->populateUdfEntries(index, udfEntries);
    }
}

DEFINE_REGISTER_UDF
{
    setupRegisterers();
    for (const auto &registerer : globalRegisters()) {
        registerer->registerSignatures();
    }
}
