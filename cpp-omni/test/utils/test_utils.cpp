/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "test_utils.h"

using namespace omniruntime::vec;

VectorBatch *CreateVectorBatch(const DataTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    auto *vectorBatch = new VectorBatch(rowCount);
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        DataTypePtr type = types.GetType(i);
        vectorBatch->Append(CreateVector(*type, rowCount, args));
    }
    va_end(args);
    return vectorBatch;
}

BaseVector *CreateVector(DataType &dataType, int32_t rowCount, va_list &args)
{
    return DYNAMIC_TYPE_DISPATCH(CreateFlatVector, dataType.GetId(), rowCount, args);
}

BaseVector *CreateDictionaryVector(DataType &dataType, int32_t rowCount, int32_t *ids, int32_t idsCount,
    ...)
{
    va_list args;
    va_start(args, idsCount);
    BaseVector *dictionary = CreateVector(dataType, rowCount, args);
    va_end(args);
    BaseVector *dictVector = DYNAMIC_TYPE_DISPATCH(CreateDictionary, dataType.GetId(), dictionary, ids, idsCount);
    delete dictionary;
    return dictVector;
}

/**
 * create a VectorBatch with 1 col 1 row varchar value and it's partition id
 * 
 * @param {int} pid partition id for this row
 * @param {string} inputString varchar row value
 * @return {VectorBatch} a VectorBatch
 */
VectorBatch* CreateVectorBatch_1row_varchar_withPid(int pid, std::string inputString) {
    // gen vectorBatch
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), VarcharType() }));
    const int32_t numRows = 1;
    auto* col1 = new int32_t[numRows];
    col1[0] = pid;
    auto* col2 = new std::string[numRows];
    col2[0] = std::move(inputString);

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col1, col2);
    delete[] col1;
    delete[] col2;
    return in;
}

/**
 * create a VectorBatch with 1 col 1 row array<int> value and it's partition id
 *
 * @param {int} pid partition id for this row
 * @param {int[]} elements of array
 * @return {VectorBatch} a VectorBatch
 */
VectorBatch* CreateVectorBatch_1row_array_int_withPid(int pid, int* elements, int element_count) {
    // gen vectorBatch
    auto arrayColType = std::make_shared<omniruntime::type::ArrayType>(IntType());
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), arrayColType }));
    const int32_t numRows = 1;

    auto *vecBatch = new VectorBatch(numRows);
    auto* col1 = new int32_t[numRows];
    col1[0] = pid;
    vecBatch->Append(CreateVector(numRows, col1));

    auto elementVector = std::make_shared<Vector<int32_t>>(element_count, OMNI_INT);
    for (int i = 0; i < element_count; i++) {
        elementVector->SetValue(i, elements[i]);
    }
    auto arrayVec = new ArrayVector(numRows, elementVector);
    arrayVec->SetOffset(0, 0);
    arrayVec->SetOffset(1, element_count);
    vecBatch->Append(arrayVec);

    return vecBatch;
}

/**
 * Create a VectorBatch with 1 row of MAP<INT,INT> + partition id col.
 * The map has 2 entries: {pid=>pid*10, pid+1=>(pid+1)*10}
 */
VectorBatch* CreateVectorBatch_1row_map_int_int_withPid(int pid) {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;
    
    auto keyType = IntType();
    auto valueType = IntType();
    auto mapColType = std::make_shared<MapType>(keyType, valueType);
    
    const int32_t numRows = 1;
    auto *vecBatch = new VectorBatch(numRows);
    
    // pid column
    auto* col1 = new int32_t[numRows];
    col1[0] = pid;
    vecBatch->Append(CreateVector(numRows, col1));
    
    // MAP<INT,INT> column: 2 key-value pairs
    int32_t numEntries = 2;
    auto keyVector = std::make_shared<Vector<int32_t>>(numEntries);
    auto valueVector = std::make_shared<Vector<int32_t>>(numEntries);
    keyVector->SetValue(0, pid);
    keyVector->SetValue(1, pid + 1);
    valueVector->SetValue(0, pid * 10);
    valueVector->SetValue(1, (pid + 1) * 10);
    
    auto mapVec = new MapVector(numRows, keyVector, valueVector);
    mapVec->SetOffset(0, 0);
    mapVec->SetOffset(1, numEntries);
    vecBatch->Append(mapVec);
    
    return vecBatch;
}

/**
 * Create a VectorBatch with 1 row of ROW<INT,VARCHAR> + partition id col.
 * The struct has 2 fields: int_value=pid*100, str_value="row_"+pid
 */
VectorBatch* CreateVectorBatch_1row_row_int_varchar_withPid(int pid) {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;
    
    std::vector<std::shared_ptr<DataType>> fieldTypes;
    fieldTypes.push_back(IntType());
    fieldTypes.push_back(VarcharType());
    auto rowColType = std::make_shared<RowType>(fieldTypes);
    
    const int32_t numRows = 1;
    auto *vecBatch = new VectorBatch(numRows);
    
    // pid column
    auto* col1 = new int32_t[numRows];
    col1[0] = pid;
    vecBatch->Append(CreateVector(numRows, col1));
    
    // ROW<INT,VARCHAR> column
    auto intField = std::make_shared<Vector<int32_t>>(numRows);
    intField->SetValue(0, pid * 100);
    
    auto strField = std::make_shared<Vector<LargeStringContainer<std::string_view>>>(numRows);
    std::string strVal = "row_" + std::to_string(pid);
    strField->SetValue(0, std::string_view(strVal));
    
    std::vector<std::shared_ptr<BaseVector>> children;
    children.push_back(intField);
    children.push_back(strField);
    auto rowVec = new RowVector(numRows, children);
    vecBatch->Append(rowVec);
    
    return vecBatch;
}

/**
 * create a VectorBatch with 4col OMNI_INT OMNI_LONG OMNI_DOUBLE OMNI_VARCHAR and it's partition id
 * 
 * @param {int} parNum partition number
 * @param {int} rowNum row number
 * @return {VectorBatch} a VectorBatch
 */
VectorBatch* CreateVectorBatch_4col_withPid(int parNum, int rowNum) {
    int partitionNum = parNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), LongType(), DoubleType(), VarcharType() }));

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new int32_t[numRows];
    auto* col2 = new int64_t[numRows];
    auto* col3 = new double[numRows];
    auto* col4 = new std::string[numRows];
    std::string startStr = "_START_";
    std::string endStr = "_END_";

    std::vector<std::string*> string_cache_test_;
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = i + 1;
        col2[i] = i + 1;
        col3[i] = i + 1;
        std::string strTmp = std::string(startStr + to_string(i + 1) + endStr);
        col4[i] = std::move(strTmp);
    }

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3, col4);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    return in;
}

VectorBatch* CreateVectorBatch_1FixCol_withPid(int parNum, int rowNum, DataTypePtr fixColType) {
    int partitionNum = parNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), std::move(fixColType) }));

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new int64_t[numRows];
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = i + 1;
    }

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1);
    delete[] col0;
    delete[] col1;
    return in; 
}

VectorBatch* CreateVectorBatch_2column_1row_withPid(int pid, std::string strVar, int intVar) {
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), VarcharType(), IntType() }));

    const int32_t numRows = 1;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new std::string[numRows];
    auto* col2 = new int32_t[numRows];

    col0[0] = pid;
    col1[0] = std::move(strVar);
    col2[0] = intVar;

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    return in;
}

VectorBatch* CreateVectorBatch_4varcharCols_withPid(int parNum, int rowNum) {
    int partitionNum = parNum;
    DataTypes inputTypes(
        std::vector<DataTypePtr>({ IntType(), VarcharType(), VarcharType(), VarcharType(), VarcharType() }));

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new std::string[numRows];
    auto* col2 = new std::string[numRows];
    auto* col3 = new std::string[numRows];
    auto* col4 = new std::string[numRows];

    for (int i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        std::string strTmp1 = std::string("Col1_START_" + to_string(i + 1) + "_END_");
        col1[i] = std::move(strTmp1);
        std::string strTmp2 = std::string("Col2_START_" + to_string(i + 1) + "_END_");
        col2[i] = std::move(strTmp2);
        std::string strTmp3 = std::string("Col3_START_" + to_string(i + 1) + "_END_");
        col3[i] = std::move(strTmp3);
        std::string strTmp4 = std::string("Col4_START_" + to_string(i + 1) + "_END_");
        col4[i] = std::move(strTmp4);
    }

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3, col4);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    return in;
}

VectorBatch* CreateVectorBatch_4charCols_withPid(int parNum, int rowNum) {
    int partitionNum = parNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), CharType(), CharType(), CharType(), CharType() }));

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new std::string[numRows];
    auto* col2 = new std::string[numRows];
    auto* col3 = new std::string[numRows];
    auto* col4 = new std::string[numRows];

    std::vector<std::string*> string_cache_test_;
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        std::string strTmp1 = std::string("Col1_CHAR_" + to_string(i + 1) + "_END_");
        col1[i] = std::move(strTmp1);
        std::string strTmp2 = std::string("Col2_CHAR_" + to_string(i + 1) + "_END_");
        col2[i] = std::move(strTmp2);
        std::string strTmp3 = std::string("Col3_CHAR_" + to_string(i + 1) + "_END_");
        col3[i] = std::move(strTmp3);
        std::string strTmp4 = std::string("Col4_CHAR_" + to_string(i + 1) + "_END_");
        col4[i] = std::move(strTmp4);
    }

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3, col4);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    return in;
}

VectorBatch* CreateVectorBatch_5fixedCols_withPid(int parNum, int rowNum) {
    int partitionNum = parNum;
    // gen vectorBatch
    DataTypes inputTypes(
        std::vector<DataTypePtr>({ IntType(), BooleanType(), ShortType(), IntType(), LongType(), DoubleType() }));

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new bool[numRows];
    auto* col2 = new int16_t[numRows];
    auto* col3 = new int32_t[numRows];
    auto* col4 = new int64_t[numRows];
    auto* col5 = new double[numRows];
    for (int i = 0; i < numRows; i++) {
        col0[i] = i % partitionNum;
        col1[i] = (i % 2) == 0 ? true : false;
        col2[i] = i + 1;
        col3[i] = i + 1;
        col4[i] = i + 1;
        col5[i] = i + 1;
    }

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3, col4, col5);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete[] col5;
    return in; 
}

VectorBatch* CreateVectorBatch_2dictionaryCols_withPid(int partitionNum) {
    // dictionary test
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    auto *col0 = new int32_t[dataSize];
    for (int32_t i = 0; i< dataSize; i++) {
        col0[i] = (i + 1) % partitionNum;
    }
    int32_t col1[dataSize] = {111, 112, 113, 114, 115, 116};
    int64_t col2[dataSize] = {221, 222, 223, 224, 225, 226};
    void *datas[2] = {col1, col2};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};

    VectorBatch *vectorBatch = new VectorBatch(dataSize);
    auto Vec0 = CreateVector<int32_t>(dataSize, col0);
    vectorBatch->Append(Vec0);
    auto dicVec0 = CreateDictionaryVector(*sourceTypes.GetType(0), dataSize, ids, dataSize, datas[0]);
    auto dicVec1 = CreateDictionaryVector(*sourceTypes.GetType(1), dataSize, ids, dataSize, datas[1]);
    vectorBatch->Append(dicVec0);
    vectorBatch->Append(dicVec1);

    delete[] col0;
    return vectorBatch;
}

VectorBatch* CreateVectorBatch_1decimal128Col_withPid(int partitionNum, int rowNum) {
    const int32_t numRows = rowNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), Decimal128Type(38, 2) }));
    
    auto *col0 = new int32_t[numRows];
    auto *col1 = new Decimal128[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = Decimal128(0, 1);
    }
    
    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1);
    delete[] col0;
    delete[] col1;
    return in;
}

VectorBatch* CreateVectorBatch_1decimal64Col_withPid(int partitionNum, int rowNum) {
    const int32_t numRows = rowNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), Decimal64Type(7, 2) }));
    
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = 1;
    }
    
    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1);
    delete[] col0;
    delete[] col1;
    return in;
}

VectorBatch* CreateVectorBatch_2decimalCol_withPid(int partitionNum, int rowNum) {
    const int32_t numRows = rowNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), Decimal64Type(7, 2), Decimal128Type(38, 2) }));
    
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int64_t[numRows];
    auto *col2 = new Decimal128[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = 1;
        col2[i] = Decimal128(0, 1);
    }
    
    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    return in;
}

VectorBatch* CreateVectorBatch_someNullRow_vectorBatch() {
    const int32_t numRows = 6;
    const int32_t numCols = 6;
    bool data0[numRows] = {true, false, true, false, true, false};
    int16_t data1[numRows] = {0, 1, 2, 3, 4, 6};
    int32_t data2[numRows] = {0, 1, 2, 0, 1, 2};
    int64_t data3[numRows] = {0, 1, 2, 3, 4, 5};
    double data4[numRows] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    std::string data5[numRows] = {"abcde", "fghij", "klmno", "pqrst", "", ""};

    DataTypes inputTypes(
        std::vector<DataTypePtr>({ BooleanType(), ShortType(), IntType(), LongType(), DoubleType(), VarcharType(5) }));
    VectorBatch* vecBatch = CreateVectorBatch(inputTypes, numRows, data0, data1, data2, data3, data4, data5);
    for (int32_t i = 0; i < numCols; i++) {
        for (int32_t j = 0; j < numRows; j = j + 2) {
            vecBatch->Get(i)->SetNull(j);
        }
    }
    return vecBatch;
}

VectorBatch* CreateVectorBatch_someNullCol_vectorBatch() {
    const int32_t numRows = 6;
    const int32_t numCols = 4;
    int32_t data1[numRows] = {0, 1, 2, 0, 1, 2};
    int64_t data2[numRows] = {0, 1, 2, 3, 4, 5};
    double data3[numRows] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    std::string data4[numRows] = {"abcde", "fghij", "klmno", "pqrst", "", ""};

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), VarcharType(5) }));
    VectorBatch* vecBatch = CreateVectorBatch(inputTypes, numRows, data1, data2, data3, data4);
    for (int32_t i = 0; i < numCols; i = i + 2) {
        for (int32_t j = 0; j < numRows; j++) {
            vecBatch->Get(i)->SetNull(j);
        }
    }
    return vecBatch;
}

void Test_Shuffle_Compression(std::string compStr, int32_t numPartition, int32_t numVb, int32_t numRow) {
    std::string shuffleTestsDir = s_shuffle_tests_dir;
    std::string tmpDataFilePath = shuffleTestsDir + "/shuffle_" + compStr;
    if (!IsFileExist(shuffleTestsDir)) {
        mkdir(shuffleTestsDir.c_str(), S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH);
    }
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = numPartition;
    long splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              compStr.c_str(),
                                              tmpDataFilePath,
                                              0,
                                              shuffleTestsDir);
    for (int64_t j = 0; j < numVb; j++) {
        VectorBatch* vb = CreateVectorBatch_4col_withPid(partitionNum, numRow);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
    if (IsFileExist(tmpDataFilePath)) {
        remove(tmpDataFilePath.c_str());
    }
}

std::vector<DataTypePtr> BuildDataTypesFromInput(const InputDataTypes& types, int32_t numCols)
{
    std::vector<DataTypePtr> out;
    out.reserve(numCols);
    for (int32_t i = 0; i < numCols; ++i) {
        auto id = static_cast<omniruntime::type::DataTypeId>(types.inputVecTypeIds[i]);
        switch (id) {
            case OMNI_BYTE:       out.push_back(ByteDataType::Instance()); break;
            case OMNI_BOOLEAN:    out.push_back(BooleanDataType::Instance()); break;
            case OMNI_SHORT:      out.push_back(ShortDataType::Instance()); break;
            case OMNI_INT:        out.push_back(IntDataType::Instance()); break;
            case OMNI_LONG:       out.push_back(LongDataType::Instance()); break;
            case OMNI_FLOAT:      out.push_back(FloatDataType::Instance()); break;
            case OMNI_DOUBLE:     out.push_back(DoubleDataType::Instance()); break;
            case OMNI_DATE32:     out.push_back(Date32DataType::Instance()); break;
            case OMNI_DATE64:     out.push_back(Date64DataType::Instance()); break;
            case OMNI_TIMESTAMP:  out.push_back(TimestampDataType::Instance()); break;
            case OMNI_VARCHAR:    out.push_back(VarcharDataType::Instance()); break;
            case OMNI_CHAR:       out.push_back(CharDataType::Instance()); break;
            case OMNI_VARBINARY:  out.push_back(VarBinaryDataType::Instance()); break;
            case OMNI_DECIMAL64:  out.push_back(std::make_shared<Decimal64DataType>(
                                     types.inputDataPrecisions[i], types.inputDataScales[i])); break;
            case OMNI_DECIMAL128: out.push_back(std::make_shared<Decimal128DataType>(
                                     types.inputDataPrecisions[i], types.inputDataScales[i])); break;
            default:
                // ARRAY/MAP/ROW 或未知：无法从扁平结构重建递归类型，返回空 → 调用方自行 SetInputDataTypes
                return {};
        }
    }
    return out;
}

long Test_splitter_nativeMake(std::string partitioning_name,
                              int num_partitions,
                              InputDataTypes inputDataTypes,
                              int numCols,
                              int buffer_size,
                              const char* compression_type_jstr,
                              std::string data_file_jstr,
                              int num_sub_dirs,
                              std::string local_dirs_jstr,
                              uint64_t task_spill_mem_threshold,
                              uint64_t executor_spill_mem_threshold) {
    auto splitOptions = SplitOptions::Defaults();
    if (buffer_size > 0) {
        splitOptions.buffer_size = buffer_size;
    }
    if (num_sub_dirs > 0) {
        splitOptions.num_sub_dirs = num_sub_dirs;
    }
    setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs_jstr.c_str(), 1);
    auto compression_type_result = GetCompressionType(compression_type_jstr);
    splitOptions.compression_type = compression_type_result;
    splitOptions.data_file = data_file_jstr;
    splitOptions.task_spill_mem_threshold = task_spill_mem_threshold;
    splitOptions.executor_spill_mem_threshold = executor_spill_mem_threshold;
    auto splitter = Splitter::Make(partitioning_name, inputDataTypes, numCols, num_partitions, std::move(splitOptions));
    // 模拟生产路径：nativeMake 通过 SetInputDataTypes 传入完整递归 DataType（写 Arrow 文件头 schema 用）。
    // 标量类型由扁平 InputDataTypes 重建；复杂类型返回空时由测试自行 SetInputDataTypes（现有复杂测试已如此）。
    auto dataTypes = BuildDataTypesFromInput(inputDataTypes, numCols);
    if (!dataTypes.empty()) {
        splitter->SetInputDataTypes(dataTypes);
    }
    return reinterpret_cast<intptr_t>(static_cast<void *>(splitter));
}

void Test_splitter_split(long splitter_addr, VectorBatch* vb) {
    auto splitter = reinterpret_cast<Splitter *>(splitter_addr);
    // Initialize split global variables
    splitter->Split(*vb);
}

void Test_splitter_stop(long splitter_addr) {
    auto splitter = reinterpret_cast<Splitter *>(splitter_addr);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_addr);
        throw std::runtime_error("Test no splitter.");
    }
    splitter->Stop();
}

void Test_splitter_splitbyrow(long splitter_addr, VectorBatch* vb) {
    auto splitter = reinterpret_cast<Splitter *>(splitter_addr);
    splitter->SplitByRow(vb);
}

void Test_splitter_stopbyrow(long splitter_addr) {
    auto splitter = reinterpret_cast<Splitter *>(splitter_addr);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_addr);
        throw std::runtime_error("Test no splitter.");
    }
    splitter->StopByRow();
}

void Test_splitter_close(long splitter_addr) {
    auto splitter = reinterpret_cast<Splitter *>(splitter_addr);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_addr);
        throw std::runtime_error("Test no splitter.");
    }
    delete splitter;
}

void GetFilePath(const char *path, const char *filename, char *filepath, const uint64_t filepathLen) {
    strcpy(filepath, path);
    if(filepath[strlen(path) - 1] != '/') {
        strcat(filepath, "/");
    }
    strcat(filepath, filename);
}

void DeletePathAll(const char* path) {
    DIR *dir;
    struct dirent *dirInfo;
    struct stat statBuf;
    static constexpr uint32_t FILE_PATH_LEN = 256;
    char filepath[FILE_PATH_LEN] = {0};
    lstat(path, &statBuf);
    if (S_ISREG(statBuf.st_mode)) {
        remove(path);
    } else if (S_ISDIR(statBuf.st_mode)) {
        if ((dir = opendir(path)) != NULL) {
            while ((dirInfo = readdir(dir)) != NULL) {
                GetFilePath(path, dirInfo->d_name, filepath, FILE_PATH_LEN);
                if (strcmp(dirInfo->d_name, ".") == 0 || strcmp(dirInfo->d_name, "..") == 0) {
                    continue;
                }
                DeletePathAll(filepath);
                rmdir(filepath);
            }
            closedir(dir);
            rmdir(path);
        }
    }
}