//
// Created by root on 4/29/25.
//

#include "OmniPlanConverter.h"

namespace omniruntime
{
namespace
{
std::unordered_map<std::string, std::string> ParseTextOptions(
    const substrait::ReadRel_LocalFiles_FileOrFiles_TextReadOptions& options)
{
    auto sourceKind = static_cast<int>(options.source_kind());
    auto codecKind = static_cast<int>(options.codec_kind());
    const bool rawLine = sourceKind == 1 && codecKind == 1;
    const bool lazySimple = sourceKind == 2 && codecKind == 2;
    if (!rawLine && !lazySimple) {
        throw std::runtime_error(
            "Unsupported Text source/codec combination.");
    }
    if (options.whole_text()) {
        throw std::runtime_error("Unsupported Text option: whole_text must be false.");
    }
    if (!options.line_separator().empty()) {
        throw std::runtime_error("Unsupported Text option: custom line separator.");
    }
    if (options.charset() != "UTF-8") {
        throw std::runtime_error("Unsupported Text option: charset must be UTF-8.");
    }
    if (options.compression_codec() != "NONE") {
        throw std::runtime_error("Unsupported Text option: compression is not available.");
    }
    std::unordered_map<std::string, std::string> result = {
        {"text.source_kind", rawLine ? "SPARK_TEXT" : "HIVE_TEXT"},
        {"text.codec_kind", rawLine ? "RAW_LINE" : "LAZY_SIMPLE"},
        {"text.charset", options.charset()},
        {"text.line_separator", options.line_separator()},
        {"text.compression_codec", options.compression_codec()},
        {"text.session_timezone", options.session_timezone()},
        {"text.whole_text", options.whole_text() ? "true" : "false"}};
    if (lazySimple) {
        if (options.field_delimiter().size() != 1) {
            throw std::runtime_error("LazySimple field delimiter must be exactly one byte.");
        }
        if (options.escape().size() > 1) {
            throw std::runtime_error("LazySimple escape delimiter must be empty or one byte.");
        }
        if (options.header() > 1) {
            throw std::runtime_error("LazySimple skip header count must be 0 or 1.");
        }
        result["text.field_delimiter"] = options.field_delimiter();
        result["text.null_literal"] = options.null_value();
        result["text.escape_enabled"] = options.escape().empty() ? "false" : "true";
        result["text.escape_char"] = options.escape();
        result["text.skip_input_lines"] = std::to_string(options.header());
        result["text.emit_header"] = "false";
        result["text.last_column_takes_rest"] = "false";
    }
    return result;
}
}

OmniPlanConverter::OmniPlanConverter(const std::vector<std::shared_ptr<ResultIterator>> &inputIters,
    mem::MemoryPool *OmniPool, const std::unordered_map<std::string, std::string> &confMap,
    const std::optional<std::string> writeFilesTempPath, bool validationMode)
    : validationMode_(validationMode),
      substraitOmniPlanConverter_(confMap, writeFilesTempPath, validationMode)
{
    substraitOmniPlanConverter_.setInputIters(std::move(inputIters));
}


std::shared_ptr<SplitInfo> parseScanSplitInfo(
    const google::protobuf::RepeatedPtrField<substrait::ReadRel_LocalFiles_FileOrFiles>& fileList)
{
    using SubstraitFileFormatCase = ::substrait::ReadRel_LocalFiles_FileOrFiles::FileFormatCase;
    auto splitInfo = std::make_shared<SplitInfo>();
    splitInfo->paths.reserve(fileList.size());
    splitInfo->starts.reserve(fileList.size());
    splitInfo->lengths.reserve(fileList.size());
    splitInfo->partitionColumns.reserve(fileList.size());
    splitInfo->metadataColumns.reserve(fileList.size());
    std::string serializedFileSchema;
    for (const auto& file : fileList) {
        // Expect all Partitions share the same index.
        splitInfo->partitionIndex = file.partition_index();
        std::unordered_map<std::string, std::string> partitionColumnMap;
        for (const auto& partitionColumn : file.partition_columns()) {
            partitionColumnMap[partitionColumn.key()] = partitionColumn.value();
        }
        splitInfo->partitionColumns.emplace_back(partitionColumnMap);
        std::unordered_map<std::string, std::string> metadataColumnMap;
        for (const auto& metadataColumn : file.metadata_columns()) {
            metadataColumnMap[metadataColumn.key()] = metadataColumn.value();
        }
        splitInfo->metadataColumns.emplace_back(metadataColumnMap);
        splitInfo->paths.emplace_back(file.uri_file());
        splitInfo->starts.emplace_back(file.start());
        splitInfo->lengths.emplace_back(file.length());
        FileProperties fileProps;
        if (file.has_properties()) {
            fileProps.fileSize = file.properties().filesize();
            fileProps.modificationTime = file.properties().modificationtime();
        }
        splitInfo->properties.emplace_back(fileProps);
        switch (file.file_format_case()) {
            case SubstraitFileFormatCase::kOrc:
                splitInfo->format = FileFormat::ORC;
                break;
            case SubstraitFileFormatCase::kParquet:
                splitInfo->format = FileFormat::PARQUET;
                break;
            case SubstraitFileFormatCase::kText: {
                splitInfo->format = FileFormat::TEXT;
                auto textOptions = ParseTextOptions(file.text());
                if (!splitInfo->customSplitInfo.empty() &&
                    splitInfo->customSplitInfo != textOptions) {
                    throw std::runtime_error("Text options must be identical within one LocalFiles split.");
                }
                splitInfo->customSplitInfo = std::move(textOptions);
                if (splitInfo->customSplitInfo.at("text.codec_kind") == "LAZY_SIMPLE") {
                    if (!file.has_schema() || file.schema().names_size() == 0) {
                        throw std::runtime_error("LazySimple Text split requires the full file schema.");
                    }
                    const auto currentSchema = file.schema().SerializeAsString();
                    if (!serializedFileSchema.empty() && serializedFileSchema != currentSchema) {
                        throw std::runtime_error("Text file schema must be identical within one LocalFiles split.");
                    }
                    if (serializedFileSchema.empty()) {
                        serializedFileSchema = currentSchema;
                        std::vector<std::string> names;
                        names.reserve(file.schema().names_size());
                        for (const auto& name : file.schema().names()) {
                            names.emplace_back(name);
                        }
                        auto types = SubstraitParser::ParseNamedStruct(file.schema());
                        splitInfo->fileSchema = ROW(std::move(names), std::move(types));
                    }
                }
                break;
            }
            default:
                splitInfo->format = FileFormat::UNKNOWN;
                break;
        }
    }
    return splitInfo;
}


void parseLocalFileNodes(
    SubstraitToOmniPlanConverter* planConverter,
    std::vector<::substrait::ReadRel_LocalFiles>& localFiles)
{
    std::vector<std::shared_ptr<SplitInfo>> splitInfos;
    splitInfos.reserve(localFiles.size());
    for (int32_t i = 0; i < localFiles.size(); i++) {
        const auto& localFile = localFiles[i];
        const auto& fileList = localFile.items();
        splitInfos.push_back(parseScanSplitInfo(fileList));
    }
    planConverter->setSplitInfos(std::move(splitInfos));
}


std::shared_ptr<const PlanNode> OmniPlanConverter::ToOmniPlan(const ::substrait::Plan &substraitPlan,
    std::vector<::substrait::ReadRel_LocalFiles> localFiles)
{
    if (!validationMode_) {
        parseLocalFileNodes(&substraitOmniPlanConverter_, localFiles);
    }
    auto OmniPlan = substraitOmniPlanConverter_.ToOmniPlan(substraitPlan);
    return OmniPlan;
}
}
