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
#include "operator/aggregation/vector_getter.h"
#include "operator/aggregation/udaf/udaf_registry.h"
#include "type/data_type.h"
#include "udf/Udaf.h"
#include "udf/examples/UdfCommon.h"
#include "vector/row_vector.h"

#include <cstring>

using namespace omniruntime::op;
using namespace omniruntime::type;

namespace {
static const char *kDouble = "double";
static const char *kFloat = "float";

namespace myavg {
class MyDoubleAvgAggregator final : public TypedAggregator {
public:
    MyDoubleAvgAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_UDAF, inputTypes, outputTypes, channels, inputRaw, outputPartial,
              isOverflowAsNull)
    {
        if (inputRaw && inputTypes.GetSize() != 1) {
            throw OmniException("Invalid Argument",
                "MyDoubleAvg raw input expects one argument. " + DescribeTypes(inputTypes, outputTypes));
        }
        if (!inputRaw && !IsTwoColumnBuffer(inputTypes) && !IsRowBuffer(inputTypes) && !IsBinaryBuffer(inputTypes)) {
            throw OmniException("Invalid Argument",
                "MyDoubleAvg partial input expects binary, struct<sum,count> or sum,count. " +
                DescribeTypes(inputTypes, outputTypes));
        }
        if (outputPartial && !IsTwoColumnBuffer(outputTypes) && !IsRowBuffer(outputTypes) &&
            !IsBinaryBuffer(outputTypes)) {
            throw OmniException("Invalid Argument",
                "MyDoubleAvg partial output expects binary, struct<sum,count> or sum,count. " +
                DescribeTypes(inputTypes, outputTypes));
        }
        if (!outputPartial && outputTypes.GetSize() != 1) {
            throw OmniException("Invalid Argument",
                "MyDoubleAvg final output expects one double column. " + DescribeTypes(inputTypes, outputTypes));
        }
    }

    size_t GetStateSize() override
    {
        return sizeof(MyDoubleAvgState);
    }

    void InitState(AggregateState *state) override
    {
        auto *avgState = CastState(state + aggStateOffset);
        avgState->sum = 0.0;
        avgState->count = 0;
    }

    void InitStates(std::vector<AggregateState *> &groupStates) override
    {
        for (auto groupState : groupStates) {
            InitState(groupState);
        }
    }

    std::vector<DataTypePtr> GetSpillType() override
    {
        if (outputPartial && (IsRowBuffer(outputTypes) || IsBinaryBuffer(outputTypes))) {
            return {outputTypes.GetType(0)};
        }
        return outputPartial ? std::vector<DataTypePtr>{DoubleType(), LongType()} : std::vector<DataTypePtr>{DoubleType()};
    }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        const auto *avgState = ConstCastState(state + aggStateOffset);
        if (outputPartial) {
            ExtractPartial(avgState, vectors, rowIndex);
        } else {
            ExtractFinal(avgState, vectors, rowIndex);
        }
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
        auto *avgState = CastState(state);
        for (int32_t i = 0; i < rowCount; ++i) {
            const int32_t rowIndex = rowOffset + i;
            if (nullMap != nullptr && (*nullMap)[i]) {
                continue;
            }
            AddInput(avgState, vector, rowIndex);
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
        if (IsBinaryBuffer(outputTypes)) {
            auto *binaryVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(
                VectorHelper::CreateVector(OMNI_FLAT, OMNI_VARBINARY, originVector->GetSize()));
            for (int32_t i = 0; i < originVector->GetSize(); ++i) {
                if ((nullMap != nullptr && (*nullMap)[i]) || originVector->IsNull(i)) {
                    binaryVector->SetNull(i);
                    continue;
                }
                MyDoubleAvgState state{GetDoubleValue(originVector, inputTypes.GetType(0)->GetId(), i), 1};
                SetBinaryValue(binaryVector, i, state);
            }
            result->Append(binaryVector);
            return;
        }
        auto *sumVector = static_cast<Vector<double> *>(VectorHelper::CreateVector(OMNI_FLAT, OMNI_DOUBLE,
            originVector->GetSize()));
        auto *countVector = static_cast<Vector<int64_t> *>(VectorHelper::CreateVector(OMNI_FLAT, OMNI_LONG,
            originVector->GetSize()));
        for (int32_t i = 0; i < originVector->GetSize(); ++i) {
            if ((nullMap != nullptr && (*nullMap)[i]) || originVector->IsNull(i)) {
                sumVector->SetNull(i);
                countVector->SetNull(i);
                continue;
            }
            sumVector->SetValue(i, GetDoubleValue(originVector, inputTypes.GetType(0)->GetId(), i));
            countVector->SetValue(i, 1);
        }
        if (IsRowBuffer(outputTypes)) {
            std::vector<std::shared_ptr<BaseVector>> children;
            children.emplace_back(std::shared_ptr<BaseVector>(sumVector));
            children.emplace_back(std::shared_ptr<BaseVector>(countVector));
            result->Append(new RowVector(originVector->GetSize(), children));
            return;
        }
        result->Append(sumVector);
        result->Append(countVector);
    }

private:
    struct MyDoubleAvgState {
        double sum;
        int64_t count;
    };

    static MyDoubleAvgState *CastState(AggregateState *state)
    {
        return reinterpret_cast<MyDoubleAvgState *>(state);
    }

    static const MyDoubleAvgState *ConstCastState(const AggregateState *state)
    {
        return reinterpret_cast<const MyDoubleAvgState *>(state);
    }

    static bool IsTwoColumnBuffer(const DataTypes &types)
    {
        return types.GetSize() == 2 && types.GetType(0)->GetId() == OMNI_DOUBLE && types.GetType(1)->GetId() == OMNI_LONG;
    }

    static bool IsRowBuffer(const DataTypes &types)
    {
        return types.GetSize() == 1 && types.GetType(0)->GetId() == OMNI_ROW;
    }

    static bool IsBinaryBuffer(const DataTypes &types)
    {
        return types.GetSize() == 1 && types.GetType(0)->GetId() == OMNI_VARBINARY;
    }

    std::string DescribeTypes(const DataTypes &input, const DataTypes &output) const
    {
        return "inputRaw=" + std::string(inputRaw ? "true" : "false") +
            ", outputPartial=" + std::string(outputPartial ? "true" : "false") +
            ", inputTypes=" + DescribeTypeList(input) + ", outputTypes=" + DescribeTypeList(output);
    }

    static std::string DescribeTypeList(const DataTypes &types)
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

    static double GetDoubleValue(BaseVector *vector, DataTypeId typeId, int32_t rowIndex)
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

    static int64_t GetLongValue(BaseVector *vector, int32_t rowIndex)
    {
        if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
            return static_cast<ConstVector<int64_t> *>(vector)->GetConstValue();
        }
        if (vector->GetEncoding() == OMNI_DICTIONARY) {
            return static_cast<Vector<DictionaryContainer<int64_t>> *>(vector)->GetValue(rowIndex);
        }
        return static_cast<Vector<int64_t> *>(vector)->GetValue(rowIndex);
    }

    static std::string_view GetBinaryValue(BaseVector *vector, int32_t rowIndex)
    {
        if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
            return static_cast<ConstVector<std::string_view> *>(vector)->GetConstValue();
        }
        if (vector->GetEncoding() == OMNI_DICTIONARY) {
            return static_cast<Vector<DictionaryContainer<std::string_view>> *>(vector)->GetValue(rowIndex);
        }
        return static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->GetValue(rowIndex);
    }

    static bool DecodeBinaryState(BaseVector *vector, int32_t rowIndex, MyDoubleAvgState &state)
    {
        const auto value = GetBinaryValue(vector, rowIndex);
        if (value.size() != kBinaryStateSize) {
            return false;
        }
        std::memcpy(&state.sum, value.data(), sizeof(state.sum));
        std::memcpy(&state.count, value.data() + sizeof(state.sum), sizeof(state.count));
        return true;
    }

    static void SetBinaryValue(Vector<LargeStringContainer<std::string_view>> *vector, int32_t rowIndex,
        const MyDoubleAvgState &state)
    {
        char value[kBinaryStateSize];
        std::memcpy(value, &state.sum, sizeof(state.sum));
        std::memcpy(value + sizeof(state.sum), &state.count, sizeof(state.count));
        vector->SetValue(rowIndex, std::string_view(value, sizeof(value)));
    }

    void AddInput(MyDoubleAvgState *state, BaseVector *vector, int32_t rowIndex)
    {
        if (inputRaw) {
            if (!vector->IsNull(rowIndex)) {
                state->sum += GetDoubleValue(vector, inputTypes.GetType(0)->GetId(), rowIndex);
                state->count++;
            }
            return;
        }

        auto *sumVector = vector;
        if (IsBinaryBuffer(inputTypes)) {
            if (sumVector->IsNull(rowIndex)) {
                return;
            }
            MyDoubleAvgState partialState{};
            if (!DecodeBinaryState(sumVector, rowIndex, partialState)) {
                return;
            }
            state->sum += partialState.sum;
            state->count += partialState.count;
            return;
        }

        if (IsRowBuffer(inputTypes)) {
            if (sumVector->IsNull(rowIndex)) {
                return;
            }
            auto *rowVector = static_cast<RowVector *>(sumVector);
            if (rowVector->ChildSize() < 2) {
                return;
            }
            auto *sumChild = rowVector->ChildAt(0).get();
            auto *countChild = rowVector->ChildAt(1).get();
            if (sumChild->IsNull(rowIndex) || countChild->IsNull(rowIndex)) {
                return;
            }
            state->sum += GetDoubleValue(sumChild, OMNI_DOUBLE, rowIndex);
            state->count += GetLongValue(countChild, rowIndex);
            return;
        }

        auto *countVector = curVectorBatch->Get(channels[1]);
        if (!sumVector->IsNull(rowIndex) && !countVector->IsNull(rowIndex)) {
            state->sum += GetDoubleValue(sumVector, OMNI_DOUBLE, rowIndex);
            state->count += GetLongValue(countVector, rowIndex);
        }
    }

    void ExtractPartial(const MyDoubleAvgState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) const
    {
        if (IsBinaryBuffer(outputTypes)) {
            auto *binaryVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            if (state->count == 0) {
                binaryVector->SetNull(rowIndex);
                return;
            }
            SetBinaryValue(binaryVector, rowIndex, *state);
            return;
        }

        BaseVector *sumBaseVector = nullptr;
        BaseVector *countBaseVector = nullptr;
        if (IsRowBuffer(outputTypes)) {
            auto *rowVector = static_cast<RowVector *>(vectors[0]);
            sumBaseVector = rowVector->ChildAt(0).get();
            countBaseVector = rowVector->ChildAt(1).get();
        } else {
            sumBaseVector = vectors[0];
            countBaseVector = vectors[1];
        }

        auto *sumVector = static_cast<Vector<double> *>(sumBaseVector);
        auto *countVector = static_cast<Vector<int64_t> *>(countBaseVector);
        if (state->count == 0) {
            if (IsRowBuffer(outputTypes)) {
                vectors[0]->SetNull(rowIndex);
            }
            sumVector->SetNull(rowIndex);
            countVector->SetNull(rowIndex);
            return;
        }
        sumVector->SetValue(rowIndex, state->sum);
        countVector->SetValue(rowIndex, state->count);
    }

    static void ExtractFinal(const MyDoubleAvgState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex)
    {
        auto *resultVector = static_cast<Vector<double> *>(vectors[0]);
        if (state->count == 0) {
            resultVector->SetNull(rowIndex);
            return;
        }
        resultVector->SetValue(rowIndex, state->sum / static_cast<double>(state->count));
    }

    static constexpr size_t kBinaryStateSize = sizeof(double) + sizeof(int64_t);
};

class MyAvgRegisterer final : public gluten::UdafRegisterer {
    int getNumUdaf() override
    {
        return 2;
    }

    static void fillUdafEntry(
        gluten::UdafEntry &entry, const char *name, const char **argTypes, const char *intermediateType)
    {
        entry.name = name;
        entry.dataType = kDouble;
        entry.numArgs = 1;
        entry.argTypes = argTypes;
        entry.intermediateType = intermediateType;
        entry.variableArity = false;
        entry.allowTypeConversion = true;
    }

    void populateUdafEntries(int &index, gluten::UdafEntry *udafEntries) override
    {
        fillUdafEntry(udafEntries[index++], name_.c_str(), myAvgArgFloat_, myAvgIntermediateType_);
        fillUdafEntry(udafEntries[index++], name_.c_str(), myAvgArgDouble_, myAvgIntermediateType_);
    }

    void registerSignatures() override
    {
        UdafRegistry::getInstance().registerUdaf(name_, [](const DataTypes &inputTypes, const DataTypes &outputTypes,
                                                          std::vector<int32_t> &channels, bool inputRaw,
                                                          bool outputPartial, bool isOverflowAsNull) {
            return std::make_unique<MyDoubleAvgAggregator>(
                inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull);
        });
    }

private:
    const std::string name_ = "com.example.hive.MyDoubleAvg";
    const char *myAvgArgFloat_[1] = {kFloat};
    const char *myAvgArgDouble_[1] = {kDouble};
    // Dynamic Hive UDAFs are backed by Spark's TypedImperativeAggregate, whose shuffle buffer
    // is BinaryType. Omni also expects UDAF partial types to be described as a struct, so wrap
    // the binary state in a single-field struct and encode sum/count inside that binary field.
    const char *myAvgIntermediateType_ = "struct<buf:binary>";
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
    globalUdafRegisters().push_back(std::make_shared<MyAvgRegisterer>());
    inited = true;
}
} // namespace myavg

std::vector<std::shared_ptr<gluten::UdafRegisterer>> &globalRegisters()
{
    return myavg::globalUdafRegisters();
}

void setupRegisterers()
{
    myavg::setupUdafRegisterers();
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
