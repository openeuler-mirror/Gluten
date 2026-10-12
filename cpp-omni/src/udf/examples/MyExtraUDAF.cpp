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

#include "operator/aggregation/aggregator/typed_aggregator.h"
#include "operator/aggregation/udaf/udaf_registry.h"
#include "type/data_type.h"
#include "udf/Udaf.h"
#include "udf/examples/UdfCommon.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace omniruntime::op;
using namespace omniruntime::type;

namespace {
static const char *kBigInt = "bigint";
static const char *kDouble = "double";
static const char *kFloat = "float";

static std::string describeTypeList(const DataTypes &types)
{
    std::string result = "[";
    for (int32_t i = 0; i < types.GetSize(); ++i) {
        if (i > 0) {
            result += ",";
        }
        result += std::to_string(static_cast<int32_t>(types.GetType(i)->GetId()));
    }
    result += "]";
    return result;
}

static std::string describeTypes(
    const DataTypes &input, const DataTypes &output, const bool inputRaw, const bool outputPartial)
{
    return "inputRaw=" + std::string(inputRaw ? "true" : "false") +
        ", outputPartial=" + std::string(outputPartial ? "true" : "false") +
        ", inputTypes=" + describeTypeList(input) + ", outputTypes=" + describeTypeList(output);
}

static double getDoubleValue(BaseVector *vector, DataTypeId typeId, int32_t rowIndex)
{
    if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
        if (typeId == OMNI_FLOAT) {
            return static_cast<double>(static_cast<ConstVector<float> *>(vector)->GetConstValue());
        }
        return static_cast<ConstVector<double> *>(vector)->GetConstValue();
    }
    if (vector->GetEncoding() == OMNI_DICTIONARY) {
        if (typeId == OMNI_FLOAT) {
            return static_cast<double>(static_cast<Vector<DictionaryContainer<float>> *>(vector)->GetValue(rowIndex));
        }
        return static_cast<Vector<DictionaryContainer<double>> *>(vector)->GetValue(rowIndex);
    }
    if (typeId == OMNI_FLOAT) {
        return static_cast<double>(static_cast<Vector<float> *>(vector)->GetValue(rowIndex));
    }
    return static_cast<Vector<double> *>(vector)->GetValue(rowIndex);
}

static int64_t getLongValue(BaseVector *vector, int32_t rowIndex)
{
    if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
        return static_cast<ConstVector<int64_t> *>(vector)->GetConstValue();
    }
    if (vector->GetEncoding() == OMNI_DICTIONARY) {
        return static_cast<Vector<DictionaryContainer<int64_t>> *>(vector)->GetValue(rowIndex);
    }
    return static_cast<Vector<int64_t> *>(vector)->GetValue(rowIndex);
}

static bool isOneColumnType(const DataTypes &types, const DataTypeId typeId)
{
    return types.GetSize() == 1 && types.GetType(0)->GetId() == typeId;
}

static std::string_view getBinaryValue(BaseVector *vector, int32_t rowIndex)
{
    if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
        return static_cast<ConstVector<std::string_view> *>(vector)->GetConstValue();
    }
    if (vector->GetEncoding() == OMNI_DICTIONARY) {
        return static_cast<Vector<DictionaryContainer<std::string_view>> *>(vector)->GetValue(rowIndex);
    }
    return static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->GetValue(rowIndex);
}

class MyDoubleSumAggregator final : public TypedAggregator {
public:
    MyDoubleSumAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_UDAF, inputTypes, outputTypes, channels, inputRaw, outputPartial,
              isOverflowAsNull)
    {
        if (inputRaw && inputTypes.GetSize() != 1) {
            throw OmniException(
                "Invalid Argument", "MyDoubleSum raw input expects one argument. " +
                    describeTypes(inputTypes, outputTypes, inputRaw, outputPartial));
        }
        if (!inputRaw && !isOneColumnType(inputTypes, OMNI_VARBINARY)) {
            throw OmniException(
                "Invalid Argument", "MyDoubleSum partial input expects one binary column. " +
                    describeTypes(inputTypes, outputTypes, inputRaw, outputPartial));
        }
        if (outputPartial && !isOneColumnType(outputTypes, OMNI_VARBINARY)) {
            throw OmniException(
                "Invalid Argument", "MyDoubleSum partial output expects one binary column. " +
                    describeTypes(inputTypes, outputTypes, inputRaw, outputPartial));
        }
        if (!outputPartial && !isOneColumnType(outputTypes, OMNI_DOUBLE)) {
            throw OmniException(
                "Invalid Argument", "MyDoubleSum final output expects one double column. " +
                    describeTypes(inputTypes, outputTypes, inputRaw, outputPartial));
        }
    }

    size_t GetStateSize() override
    {
        return sizeof(SumState);
    }

    void InitState(AggregateState *state) override
    {
        auto *sumState = CastState(state + aggStateOffset);
        sumState->sum = 0.0;
        sumState->hasValue = false;
    }

    void InitStates(std::vector<AggregateState *> &groupStates) override
    {
        for (auto groupState : groupStates) {
            InitState(groupState);
        }
    }

    std::vector<DataTypePtr> GetSpillType() override
    {
        return outputPartial ? std::vector<DataTypePtr>{VarBinaryType()} : std::vector<DataTypePtr>{DoubleType()};
    }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        if (vectors.empty()) {
            throw OmniException("Invalid Argument", "MyDoubleSum extract expects one output vector.");
        }
        const auto *sumState = ConstCastState(state + aggStateOffset);
        if (outputPartial) {
            auto *resultVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            if (!sumState->hasValue) {
                resultVector->SetNull(rowIndex);
                return;
            }
            SetBinaryValue(resultVector, rowIndex, *sumState);
            return;
        }
        auto *resultVector = static_cast<Vector<double> *>(vectors[0]);
        if (!sumState->hasValue) {
            resultVector->SetNull(rowIndex);
            return;
        }
        resultVector->SetValue(rowIndex, sumState->sum);
    }

    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override
    {
        for (int32_t i = 0; i < rowCount; ++i) {
            ExtractValues(groupStates[i], vectors, rowOffset + i);
        }
    }

    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override
    {
        for (int32_t i = 0; i < static_cast<int32_t>(groupStates.size()); ++i) {
            ExtractValues(groupStates[i], vectors, i);
        }
    }

protected:
    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override
    {
        auto *sumState = CastState(state);
        for (int32_t i = 0; i < rowCount; ++i) {
            const int32_t rowIndex = rowOffset + i;
            if (nullMap != nullptr && (*nullMap)[i]) {
                continue;
            }
            AddInput(sumState, vector, rowIndex);
        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override
    {
        for (int32_t i = 0; i < static_cast<int32_t>(rowStates.size()); ++i) {
            if (nullMap != nullptr && (*nullMap)[i]) {
                continue;
            }
            AddInput(CastState(rowStates[i] + aggStateOffset), vector, rowOffset + i);
        }
    }

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override
    {
        if (!outputPartial || originVector == nullptr) {
            return;
        }
        auto *sumVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(
            VectorHelper::CreateVector(OMNI_FLAT, OMNI_VARBINARY, originVector->GetSize()));
        for (int32_t i = 0; i < originVector->GetSize(); ++i) {
            if ((nullMap != nullptr && (*nullMap)[i]) || originVector->IsNull(i)) {
                sumVector->SetNull(i);
                continue;
            }
            SumState state{getDoubleValue(originVector, inputTypes.GetType(0)->GetId(), i), true};
            SetBinaryValue(sumVector, i, state);
        }
        result->Append(sumVector);
    }

private:
    struct SumState {
        double sum;
        bool hasValue;
    };

    static SumState *CastState(AggregateState *state)
    {
        return reinterpret_cast<SumState *>(state);
    }

    static const SumState *ConstCastState(const AggregateState *state)
    {
        return reinterpret_cast<const SumState *>(state);
    }

    static bool DecodeBinaryState(BaseVector *vector, int32_t rowIndex, SumState &state)
    {
        const auto value = getBinaryValue(vector, rowIndex);
        if (value.size() != kBinaryStateSize) {
            return false;
        }
        std::memcpy(&state.sum, value.data(), sizeof(state.sum));
        std::memcpy(&state.hasValue, value.data() + sizeof(state.sum), sizeof(state.hasValue));
        return true;
    }

    static void SetBinaryValue(Vector<LargeStringContainer<std::string_view>> *vector, int32_t rowIndex,
        const SumState &state)
    {
        char value[kBinaryStateSize];
        std::memcpy(value, &state.sum, sizeof(state.sum));
        std::memcpy(value + sizeof(state.sum), &state.hasValue, sizeof(state.hasValue));
        vector->SetValue(rowIndex, std::string_view(value, sizeof(value)));
    }

    void AddInput(SumState *state, BaseVector *vector, int32_t rowIndex)
    {
        if (vector->IsNull(rowIndex)) {
            return;
        }
        if (inputRaw) {
            state->sum += getDoubleValue(vector, inputTypes.GetType(0)->GetId(), rowIndex);
            state->hasValue = true;
            return;
        }
        SumState partialState{};
        if (!DecodeBinaryState(vector, rowIndex, partialState) || !partialState.hasValue) {
            return;
        }
        state->sum += partialState.sum;
        state->hasValue = true;
    }

    static constexpr size_t kBinaryStateSize = sizeof(double) + sizeof(bool);
};

class MyDoubleCountNonNullAggregator final : public TypedAggregator {
public:
    MyDoubleCountNonNullAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_UDAF, inputTypes, outputTypes, channels, inputRaw, outputPartial,
              isOverflowAsNull)
    {
        if (inputRaw && inputTypes.GetSize() != 1) {
            throw OmniException(
                "Invalid Argument", "MyDoubleCountNonNull raw input expects one argument. " +
                    describeTypes(inputTypes, outputTypes, inputRaw, outputPartial));
        }
        if (!inputRaw && !isOneColumnType(inputTypes, OMNI_VARBINARY)) {
            throw OmniException(
                "Invalid Argument", "MyDoubleCountNonNull partial input expects one binary column. " +
                    describeTypes(inputTypes, outputTypes, inputRaw, outputPartial));
        }
        if (outputPartial && !isOneColumnType(outputTypes, OMNI_VARBINARY)) {
            throw OmniException(
                "Invalid Argument", "MyDoubleCountNonNull partial output expects one binary column. " +
                    describeTypes(inputTypes, outputTypes, inputRaw, outputPartial));
        }
        if (!outputPartial && !isOneColumnType(outputTypes, OMNI_LONG)) {
            throw OmniException(
                "Invalid Argument", "MyDoubleCountNonNull final output expects one bigint column. " +
                    describeTypes(inputTypes, outputTypes, inputRaw, outputPartial));
        }
    }

    size_t GetStateSize() override
    {
        return sizeof(CountState);
    }

    void InitState(AggregateState *state) override
    {
        CastState(state + aggStateOffset)->count = 0;
    }

    void InitStates(std::vector<AggregateState *> &groupStates) override
    {
        for (auto groupState : groupStates) {
            InitState(groupState);
        }
    }

    std::vector<DataTypePtr> GetSpillType() override
    {
        return outputPartial ? std::vector<DataTypePtr>{VarBinaryType()} : std::vector<DataTypePtr>{LongType()};
    }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        if (vectors.empty()) {
            throw OmniException("Invalid Argument", "MyDoubleCountNonNull extract expects one output vector.");
        }
        const auto *countState = ConstCastState(state + aggStateOffset);
        if (outputPartial) {
            auto *resultVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            SetBinaryValue(resultVector, rowIndex, *countState);
            return;
        }
        static_cast<Vector<int64_t> *>(vectors[0])->SetValue(rowIndex, countState->count);
    }

    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override
    {
        for (int32_t i = 0; i < rowCount; ++i) {
            ExtractValues(groupStates[i], vectors, rowOffset + i);
        }
    }

    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override
    {
        for (int32_t i = 0; i < static_cast<int32_t>(groupStates.size()); ++i) {
            ExtractValues(groupStates[i], vectors, i);
        }
    }

protected:
    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override
    {
        auto *countState = CastState(state);
        for (int32_t i = 0; i < rowCount; ++i) {
            const int32_t rowIndex = rowOffset + i;
            if (nullMap != nullptr && (*nullMap)[i]) {
                continue;
            }
            AddInput(countState, vector, rowIndex);
        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override
    {
        for (int32_t i = 0; i < static_cast<int32_t>(rowStates.size()); ++i) {
            if (nullMap != nullptr && (*nullMap)[i]) {
                continue;
            }
            AddInput(CastState(rowStates[i] + aggStateOffset), vector, rowOffset + i);
        }
    }

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override
    {
        if (!outputPartial || originVector == nullptr) {
            return;
        }
        auto *countVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(
            VectorHelper::CreateVector(OMNI_FLAT, OMNI_VARBINARY, originVector->GetSize()));
        for (int32_t i = 0; i < originVector->GetSize(); ++i) {
            CountState state{0};
            if ((nullMap != nullptr && (*nullMap)[i]) || originVector->IsNull(i)) {
                SetBinaryValue(countVector, i, state);
                continue;
            }
            state.count = 1;
            SetBinaryValue(countVector, i, state);
        }
        result->Append(countVector);
    }

private:
    struct CountState {
        int64_t count;
    };

    static CountState *CastState(AggregateState *state)
    {
        return reinterpret_cast<CountState *>(state);
    }

    static const CountState *ConstCastState(const AggregateState *state)
    {
        return reinterpret_cast<const CountState *>(state);
    }

    static bool DecodeBinaryState(BaseVector *vector, int32_t rowIndex, CountState &state)
    {
        const auto value = getBinaryValue(vector, rowIndex);
        if (value.size() != kBinaryStateSize) {
            return false;
        }
        std::memcpy(&state.count, value.data(), sizeof(state.count));
        return true;
    }

    static void SetBinaryValue(Vector<LargeStringContainer<std::string_view>> *vector, int32_t rowIndex,
        const CountState &state)
    {
        char value[kBinaryStateSize];
        std::memcpy(value, &state.count, sizeof(state.count));
        vector->SetValue(rowIndex, std::string_view(value, sizeof(value)));
    }

    void AddInput(CountState *state, BaseVector *vector, int32_t rowIndex)
    {
        if (vector->IsNull(rowIndex)) {
            return;
        }
        if (inputRaw) {
            state->count++;
            return;
        }
        CountState partialState{};
        if (!DecodeBinaryState(vector, rowIndex, partialState)) {
            return;
        }
        state->count += partialState.count;
    }

    static constexpr size_t kBinaryStateSize = sizeof(int64_t);
};

template <typename AggregatorType> class OneArgUdafRegisterer final : public gluten::UdafRegisterer {
public:
    OneArgUdafRegisterer(const char *name, const char *returnType, const char *intermediateType)
        : name_(name), returnType_(returnType), intermediateType_(intermediateType)
    {
    }

    int getNumUdaf() override
    {
        return 2;
    }

    void populateUdafEntries(int &index, gluten::UdafEntry *udafEntries) override
    {
        fillUdafEntry(udafEntries[index++], argFloat_);
        fillUdafEntry(udafEntries[index++], argDouble_);
    }

    void registerSignatures() override
    {
        UdafRegistry::getInstance().registerUdaf(name_, [](const DataTypes &inputTypes, const DataTypes &outputTypes,
                                                          std::vector<int32_t> &channels, bool inputRaw,
                                                          bool outputPartial, bool isOverflowAsNull) {
            return std::make_unique<AggregatorType>(
                inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        });
    }

private:
    void fillUdafEntry(gluten::UdafEntry &entry, const char **argTypes) const
    {
        entry.name = name_.c_str();
        entry.dataType = returnType_;
        entry.numArgs = 1;
        entry.argTypes = argTypes;
        entry.intermediateType = intermediateType_;
        entry.variableArity = false;
        entry.allowTypeConversion = true;
    }

    const std::string name_;
    const char *returnType_;
    const char *intermediateType_;
    const char *argFloat_[1] = {kFloat};
    const char *argDouble_[1] = {kDouble};
};

std::vector<std::shared_ptr<gluten::UdafRegisterer>> &globalUdafRegisters()
{
    static std::vector<std::shared_ptr<gluten::UdafRegisterer>> registerers;
    return registerers;
}

void setupUdafRegisterers()
{
    static bool inited = false;
    if (inited) {
        return;
    }
    // Dynamic Hive UDAFs are backed by Spark's TypedImperativeAggregate, whose shuffle buffer
    // is BinaryType. Omni also expects UDAF partial types to be described as a struct, so wrap
    // each native state in a single binary field and encode/decode the state in the aggregator.
    globalUdafRegisters().push_back(
        std::make_shared<OneArgUdafRegisterer<MyDoubleSumAggregator>>(
            "com.example.hive.MyDoubleSum", kDouble, "struct<buf:binary>"));
    globalUdafRegisters().push_back(
        std::make_shared<OneArgUdafRegisterer<MyDoubleCountNonNullAggregator>>(
            "com.example.hive.MyDoubleCountNonNull", kBigInt, "struct<buf:binary>"));
    inited = true;
}

std::vector<std::shared_ptr<gluten::UdafRegisterer>> &globalRegisters()
{
    return globalUdafRegisters();
}

void setupRegisterers()
{
    setupUdafRegisterers();
}
} // namespace

DEFINE_GET_NUM_UDAF
{
    setupRegisterers();
    int numUdaf = 0;
    for (const auto &registerer : globalRegisters()) {
        numUdaf += registerer->getNumUdaf();
    }
    return numUdaf;
}

DEFINE_GET_UDAF_ENTRIES
{
    setupRegisterers();
    int index = 0;
    for (const auto &registerer : globalRegisters()) {
        registerer->populateUdafEntries(index, udafEntries);
    }
}

DEFINE_REGISTER_UDAF
{
    setupRegisterers();
    for (const auto &registerer : globalRegisters()) {
        registerer->registerSignatures();
    }
}
