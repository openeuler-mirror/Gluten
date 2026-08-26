/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution;

import com.huawei.boostkit.spark.jni.OrcColumnarBatchWriter;
import com.huawei.boostkit.spark.jni.ParquetColumnarBatchWriter;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.gluten.connector.write.PaimonFileInfoJson;
import org.apache.gluten.metrics.BatchWriteMetrics;
import org.apache.gluten.runtime.OmniRuntime;
import org.apache.gluten.runtime.RuntimeAware;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Task-local bridge from Spark columnar batches to Paimon data files using Omni native ORC/Parquet
 * writers. Files are first written under a staging directory; the Scala commit builder moves them
 * into the final Paimon partition/bucket directory and builds Paimon commit messages.
 *
 * @since 2026
 */
public class PaimonWriteJniWrapper implements RuntimeAware {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonWriteJniWrapper.class);
    private static final int FORMAT_ORC = 0;
    private static final int FORMAT_PARQUET = 1;

    // Must match Paimon table option partition.default-name (default: __DEFAULT_PARTITION__).
    private static final String DEFAULT_PARTITION_PATH = "__DEFAULT_PARTITION__";
    private static final String HDFS_SCHEME_WITH_SLASH = "hdfs:/";
    private static final String HDFS_SCHEME_WITH_AUTHORITY = "hdfs://";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OmniRuntime runtime;
    private WriterState state;

    public PaimonWriteJniWrapper(OmniRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public long rtHandle() {
        return runtime.getHandle();
    }

    /**
     * Initializes the native writer for this task.
     *
     * @param schema Spark row schema
     * @param omniTypes Omni type ids per column
     * @param params writer initialization parameters
     */
    public void init(StructType schema, int[] omniTypes, PaimonWriterInitParams params) {
        if (state != null) {
            throw new IllegalStateException("Already initialized");
        }
        state = new WriterState(schema, omniTypes, params);
    }

    /**
     * Writes one columnar batch into staging files.
     *
     * @param batch input columnar batch
     */
    public void write(ColumnarBatch batch) {
        if (state == null) {
            throw new IllegalStateException("Not initialized");
        }
        state.write(batch);
    }

    /**
     * Closes writers and returns serialized file metadata JSON strings.
     *
     * @return serialized Paimon file info JSON strings
     */
    public String[] commit() {
        if (state == null) {
            throw new IllegalStateException("Not initialized");
        }
        WriterState s = state;
        state = null;
        try {
            return s.commit().toArray(new String[0]);
        } finally {
            s.close();
        }
    }

    /**
     * Returns write metrics for the current task.
     *
     * @return batch write metrics snapshot
     */
    public BatchWriteMetrics metrics() {
        if (state == null) {
            return new BatchWriteMetrics(0L, 0, 0L, 0L);
        }
        return state.metrics();
    }

    /** Holder for writer initialization parameters. */
    public static final class PaimonWriterInitParams {
        final int format;
        final String stagingDirectory;
        final int partitionId;
        final long taskId;
        final String operationId;
        final boolean isLegacyDatetimeRebase;
        final String[] partitionColumns;
        final String[] bucketColumns;
        final int numBuckets;
        final int bucketColumnIndex;
        final int dataColumnCount;
        final String bucketFunctionType;

        public PaimonWriterInitParams(int format, String stagingDirectory, int partitionId,
                long taskId, String operationId, boolean isLegacyDatetimeRebase,
                String[] partitionColumns, String[] bucketColumns, int numBuckets,
                int bucketColumnIndex, int dataColumnCount, String bucketFunctionType) {
            this.format = format;
            this.stagingDirectory = stagingDirectory;
            this.partitionId = partitionId;
            this.taskId = taskId;
            this.operationId = operationId;
            this.isLegacyDatetimeRebase = isLegacyDatetimeRebase;
            this.partitionColumns = partitionColumns;
            this.bucketColumns = bucketColumns;
            this.numBuckets = numBuckets;
            this.bucketColumnIndex = bucketColumnIndex;
            this.dataColumnCount = dataColumnCount;
            this.bucketFunctionType = bucketFunctionType;
        }
    }

    private static final class WriterState {
        private final StructType schema;
        private final StructType fileSchema;
        private final int[] omniTypes;
        private final int format;
        private final String stagingDirectory;
        private final int partitionId;
        private final long taskId;
        private final String operationId;
        private final boolean isLegacyDatetimeRebase;
        private final String[] partitionColumns;
        private final String[] bucketColumns;
        private final int numBuckets;
        private final int bucketColumnIndex;
        private final int dataColumnCount;
        private final String bucketFunctionType;
        private final boolean[] dataColumnIds;
        private final long startTimeNs = System.nanoTime();
        private final Map<String, FileWriterInfo> writers = new LinkedHashMap<>();
        private final List<String> fileInfoJson = new ArrayList<>();
        private int fileIndex;
        private long totalBytesWritten;
        private int numFiles;

        WriterState(StructType schema, int[] omniTypes, PaimonWriterInitParams params) {
            this.schema = schema;
            this.omniTypes = omniTypes;
            this.format = params.format;
            this.stagingDirectory = normalizeNativePath(params.stagingDirectory);
            this.partitionId = params.partitionId;
            this.taskId = params.taskId;
            this.operationId = params.operationId;
            this.isLegacyDatetimeRebase = params.isLegacyDatetimeRebase;
            this.partitionColumns = params.partitionColumns == null ? new String[0] : params.partitionColumns;
            this.bucketColumns = params.bucketColumns == null ? new String[0] : params.bucketColumns;
            this.numBuckets = params.numBuckets;
            this.bucketColumnIndex = params.bucketColumnIndex;
            this.dataColumnCount = params.dataColumnCount;
            this.bucketFunctionType =
                    params.bucketFunctionType == null ? "default" : params.bucketFunctionType.toLowerCase(Locale.ROOT);
            this.dataColumnIds = buildDataColumnIds(schema, this.partitionColumns, this.dataColumnCount);
            this.fileSchema = buildFileSchema(schema, this.dataColumnIds);
        }

        void write(ColumnarBatch batch) {
            if (batch.numRows() == 0) {
                return;
            }
            Map<String, List<Integer>> rowsByPartitionBucket = new LinkedHashMap<>();
            for (int row = 0; row < batch.numRows(); row++) {
                String key = writerKey(partitionKey(batch, row), bucket(batch, row));
                rowsByPartitionBucket.computeIfAbsent(key, ignored -> new ArrayList<>()).add(row);
            }
            for (Map.Entry<String, List<Integer>> entry : rowsByPartitionBucket.entrySet()) {
                FileWriterInfo writer = getOrCreateWriter(entry.getKey());
                for (int[] range : contiguousRanges(entry.getValue())) {
                    writeRange(writer, batch, range[0], range[1]);
                }
            }
        }

        private void writeRange(FileWriterInfo info, ColumnarBatch batch, int start, int end) {
            if (info.writer instanceof ParquetColumnarBatchWriter) {
                ((ParquetColumnarBatchWriter) info.writer).splitWrite(
                        omniTypes, omniTypes, dataColumnIds, batch, start, end);
            } else if (info.writer instanceof OrcColumnarBatchWriter) {
                ((OrcColumnarBatchWriter) info.writer).splitWrite(
                        omniTypes, omniTypes, dataColumnIds, batch, start, end);
            } else {
                throw new IllegalStateException("Unexpected writer type: " + info.writer.getClass().getName());
            }
            info.recordCount += end - start;
        }

        private FileWriterInfo getOrCreateWriter(String partitionKey) {
            FileWriterInfo existing = writers.get(partitionKey);
            if (existing != null) {
                return existing;
            }
            fileIndex++;
            String ext = format == FORMAT_PARQUET ? "parquet" : "orc";
            String fileName = String.format(Locale.ROOT, "data-%d-%s-%d-%05d.%s",
                    partitionId, operationId, taskId, fileIndex, ext);
            String path = String.format(Locale.ROOT, "%s/%s", stagingDirectory, fileName);
            try {
                Path output = new Path(path);
                FileSystem fs = output.getFileSystem(new Configuration());
                fs.mkdirs(output.getParent());
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to create Paimon staging directory for " + path, ex);
            }
            Object writer = createNativeWriter(path);
            FileWriterInfo info = new FileWriterInfo(
                    writer,
                    path,
                    partitionValues(partitionPart(partitionKey)),
                    bucketPart(partitionKey));
            writers.put(partitionKey, info);
            return info;
        }

        private Object createNativeWriter(String path) {
            if (format == FORMAT_PARQUET) {
                ParquetColumnarBatchWriter writer = new ParquetColumnarBatchWriter(isLegacyDatetimeRebase);
                writer.initializeSchemaJava(fileSchema);
                try {
                    writer.initializeWriterJava(new Path(path));
                } catch (IOException ex) {
                    throw new IllegalStateException("Failed to open " + path, ex);
                }
                return writer;
            }
            if (format != FORMAT_ORC) {
                throw new UnsupportedOperationException("Unsupported Paimon write format: " + format);
            }
            OrcColumnarBatchWriter writer = new OrcColumnarBatchWriter(true, false);
            Path output = new Path(path);
            Configuration conf = new Configuration();
            writer.initializeOutputStreamJava(output.toUri());
            writer.initializeSchemaTypeJava(fileSchema);
            try {
                OrcFile.WriterOptions opts = OrcFile.writerOptions(conf).fileSystem(output.getFileSystem(conf));
                writer.initializeWriterJava(output.toUri(), fileSchema, opts);
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to open " + path, ex);
            }
            return writer;
        }

        List<String> commit() {
            for (FileWriterInfo info : writers.values()) {
                closeWriter(info.writer);
                long fileSize = fileSize(info.path);
                totalBytesWritten += fileSize;
                numFiles++;
                PaimonFileInfoJson json = new PaimonFileInfoJson();
                json.setPath(info.path);
                json.setPartitionValues(info.partitionValues);
                json.setBucket(info.bucket);
                json.setRecordCount(info.recordCount);
                json.setFileSizeInBytes(fileSize);
                try {
                    fileInfoJson.add(MAPPER.writeValueAsString(json));
                } catch (IOException ex) {
                    throw new IllegalStateException("Serialize PaimonFileInfoJson failed", ex);
                }
            }
            writers.clear();
            LOG.warn("[Gluten][PaimonWrite] Native {} writer committed files={} bytes={}",
                    formatName(), numFiles, totalBytesWritten);
            return new ArrayList<>(fileInfoJson);
        }

        BatchWriteMetrics metrics() {
            return new BatchWriteMetrics(totalBytesWritten, numFiles, 0L, System.nanoTime() - startTimeNs);
        }

        void close() {
            for (FileWriterInfo info : writers.values()) {
                closeWriter(info.writer);
            }
            writers.clear();
        }

        private static void closeWriter(Object writer) {
            if (writer instanceof ParquetColumnarBatchWriter) {
                ((ParquetColumnarBatchWriter) writer).close();
            } else if (writer instanceof OrcColumnarBatchWriter) {
                ((OrcColumnarBatchWriter) writer).close();
            } else {
                throw new IllegalStateException(
                        "Unexpected Paimon native writer type: " + writer.getClass().getName());
            }
        }

        private static long fileSize(String path) {
            try {
                Path output = new Path(path);
                FileSystem fs = output.getFileSystem(new Configuration());
                return fs.exists(output) ? fs.getFileStatus(output).getLen() : 0L;
            } catch (IOException ignored) {
                return 0L;
            }
        }

        private String formatName() {
            return format == FORMAT_PARQUET ? "parquet" : "orc";
        }

        private static String normalizeNativePath(String path) {
            if (path == null || !path.startsWith(HDFS_SCHEME_WITH_SLASH)
                    || path.startsWith(HDFS_SCHEME_WITH_AUTHORITY)) {
                return path;
            }
            Configuration conf = new Configuration();
            String defaultFs = conf.get("fs.defaultFS");
            if (defaultFs == null || !defaultFs.startsWith(HDFS_SCHEME_WITH_AUTHORITY)) {
                LOG.warn("[Gluten][PaimonWrite] Cannot normalize HDFS path without fs.defaultFS, path={}",
                        path);
                return path;
            }
            String normalizedDefaultFs = defaultFs.endsWith("/")
                    ? defaultFs.substring(0, defaultFs.length() - 1)
                    : defaultFs;
            return normalizedDefaultFs + path.substring("hdfs:".length());
        }

        private String partitionKey(ColumnarBatch batch, int row) {
            List<String> values = new ArrayList<>(partitionColumns.length);
            for (String colName : partitionColumns) {
                int colIdx = schema.fieldIndex(colName);
                values.add(partitionValue(batch.column(colIdx), schema.fields()[colIdx].dataType(), row));
            }
            return String.join("\u0001", values);
        }

        private int bucket(ColumnarBatch batch, int row) {
            if (numBuckets <= 0) {
                return 0;
            }
            if (bucketColumnIndex >= 0 && bucketColumnIndex < batch.numCols()) {
                return Math.floorMod(batch.column(bucketColumnIndex).getInt(row), numBuckets);
            }
            BinaryRow bucketKey = bucketKeyRow(batch, row);
            if ("mod".equals(bucketFunctionType)) {
                return modBucket(bucketKey, batch, row);
            }
            if (!"default".equals(bucketFunctionType)) {
                throw new UnsupportedOperationException(
                        "Unsupported Paimon bucket function for native write: " + bucketFunctionType);
            }
            return defaultBucket(bucketKey);
        }

        private int defaultBucket(BinaryRow bucketKey) {
            return Math.abs(bucketKey.hashCode() % numBuckets);
        }

        private int modBucket(BinaryRow bucketKey, ColumnarBatch batch, int row) {
            if (bucketColumns.length != 1) {
                throw new UnsupportedOperationException("Paimon mod bucket function requires exactly one bucket key");
            }
            int colIdx = schema.fieldIndex(bucketColumns[0]);
            DataType dataType = schema.fields()[colIdx].dataType();
            if (bucketKey.isNullAt(0)) {
                return 0;
            }
            if (dataType instanceof IntegerType) {
                return Math.floorMod(bucketKey.getInt(0), numBuckets);
            }
            if (dataType instanceof LongType) {
                return (int) Math.floorMod(bucketKey.getLong(0), (long) numBuckets);
            }
            throw new UnsupportedOperationException(
                    "Paimon mod bucket function only supports INT or BIGINT bucket key: " + dataType);
        }

        private static String writerKey(String partitionKey, int bucket) {
            return partitionKey + "\u0002" + bucket;
        }

        private static String partitionPart(String writerKey) {
            int index = writerKey.lastIndexOf('\u0002');
            return index < 0 ? writerKey : writerKey.substring(0, index);
        }

        private static int bucketPart(String writerKey) {
            int index = writerKey.lastIndexOf('\u0002');
            return index < 0 ? 0 : Integer.parseInt(writerKey.substring(index + 1));
        }

        private List<String> partitionValues(String partitionKey) {
            if (partitionColumns.length == 0) {
                return new ArrayList<>();
            }
            return new ArrayList<>(Arrays.asList(partitionKey.split("\u0001", -1)));
        }

        private static String partitionValue(ColumnVector col, DataType dataType, int row) {
            if (col.isNullAt(row)) {
                return DEFAULT_PARTITION_PATH;
            }
            if (dataType instanceof BooleanType) {
                return String.valueOf(col.getBoolean(row));
            }
            if (dataType instanceof IntegerType || dataType instanceof DateType) {
                return String.valueOf(col.getInt(row));
            }
            if (dataType instanceof LongType || dataType instanceof TimestampType) {
                return String.valueOf(col.getLong(row));
            }
            if (dataType instanceof ShortType) {
                return String.valueOf(col.getShort(row));
            }
            if (dataType instanceof ByteType) {
                return String.valueOf(col.getByte(row));
            }
            if (dataType instanceof FloatType) {
                return String.valueOf(col.getFloat(row));
            }
            if (dataType instanceof DoubleType) {
                return String.valueOf(col.getDouble(row));
            }
            if (dataType instanceof StringType || dataType instanceof CharType || dataType instanceof VarcharType) {
                return col.getUTF8String(row).toString();
            }
            if (dataType instanceof BinaryType) {
                return new String(col.getBinary(row), java.nio.charset.StandardCharsets.ISO_8859_1);
            }
            if (dataType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) dataType;
                return col.getDecimal(row, decimalType.precision(), decimalType.scale())
                        .toJavaBigDecimal()
                        .toPlainString();
            }
            throw new UnsupportedOperationException("Unsupported Paimon partition column type: " + dataType);
        }

        private static boolean[] buildDataColumnIds(
                StructType schema, String[] partitionColumns, int dataColumnCount) {
            boolean[] ids = new boolean[schema.fields().length];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = i < dataColumnCount;
            }
            for (String partitionColumn : partitionColumns) {
                ids[schema.fieldIndex(partitionColumn)] = false;
            }
            return ids;
        }

        private static StructType buildFileSchema(StructType schema, boolean[] dataColumnIds) {
            List<StructField> fields = new ArrayList<>();
            for (int i = 0; i < dataColumnIds.length; i++) {
                if (dataColumnIds[i]) {
                    fields.add(schema.fields()[i]);
                }
            }
            return new StructType(fields.toArray(new StructField[0]));
        }

        private BinaryRow bucketKeyRow(ColumnarBatch batch, int row) {
            BinaryRow binaryRow = new BinaryRow(bucketColumns.length);
            BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
            writer.reset();
            writer.writeRowKind(RowKind.INSERT);
            for (int i = 0; i < bucketColumns.length; i++) {
                int colIdx = schema.fieldIndex(bucketColumns[i]);
                writePaimonField(writer, i, batch.column(colIdx), schema.fields()[colIdx].dataType(), row);
            }
            writer.complete();
            return binaryRow;
        }

        private static void writePaimonField(
                BinaryRowWriter writer, int pos, ColumnVector col, DataType dataType, int row) {
            if (col.isNullAt(row)) {
                writer.setNullAt(pos);
                return;
            }
            writeNonNullPaimonField(writer, pos, col, dataType, row);
        }

        private static void writeNonNullPaimonField(
                BinaryRowWriter writer, int pos, ColumnVector col, DataType dataType, int row) {
            if (writePaimonNumericField(writer, pos, col, dataType, row)) {
                return;
            }
            writePaimonStringBinaryDecimalField(writer, pos, col, dataType, row);
        }

        private static boolean writePaimonNumericField(
                BinaryRowWriter writer, int pos, ColumnVector col, DataType dataType, int row) {
            if (dataType instanceof BooleanType) {
                writer.writeBoolean(pos, col.getBoolean(row));
                return true;
            }
            if (dataType instanceof IntegerType || dataType instanceof DateType) {
                writer.writeInt(pos, col.getInt(row));
                return true;
            }
            if (dataType instanceof LongType) {
                writer.writeLong(pos, col.getLong(row));
                return true;
            }
            if (dataType instanceof TimestampType) {
                writer.writeTimestamp(pos, Timestamp.fromMicros(col.getLong(row)), 6);
                return true;
            }
            if (dataType instanceof ShortType) {
                writer.writeShort(pos, col.getShort(row));
                return true;
            }
            if (dataType instanceof ByteType) {
                writer.writeByte(pos, col.getByte(row));
                return true;
            }
            if (dataType instanceof FloatType) {
                writer.writeFloat(pos, col.getFloat(row));
                return true;
            }
            if (dataType instanceof DoubleType) {
                writer.writeDouble(pos, col.getDouble(row));
                return true;
            }
            return false;
        }

        private static void writePaimonStringBinaryDecimalField(
                BinaryRowWriter writer, int pos, ColumnVector col, DataType dataType, int row) {
            if (dataType instanceof StringType || dataType instanceof CharType || dataType instanceof VarcharType) {
                writer.writeString(pos, BinaryString.fromString(col.getUTF8String(row).toString()));
                return;
            }
            if (dataType instanceof BinaryType) {
                byte[] bytes = col.getBinary(row);
                writer.writeBinary(pos, bytes, 0, bytes.length);
                return;
            }
            if (dataType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) dataType;
                Decimal decimal = Decimal.fromBigDecimal(
                        col.getDecimal(row, decimalType.precision(), decimalType.scale()).toJavaBigDecimal(),
                        decimalType.precision(),
                        decimalType.scale());
                writer.writeDecimal(pos, decimal, decimalType.precision());
                return;
            }
            throw new UnsupportedOperationException("Unsupported Paimon bucket column type: " + dataType);
        }

        private static List<int[]> contiguousRanges(List<Integer> rows) {
            rows.sort(Integer::compareTo);
            List<int[]> ranges = new ArrayList<>();
            int start = rows.get(0);
            int previous = start;
            for (int i = 1; i < rows.size(); i++) {
                int current = rows.get(i);
                if (current != previous + 1) {
                    ranges.add(new int[] {start, previous + 1});
                    start = current;
                }
                previous = current;
            }
            ranges.add(new int[] {start, previous + 1});
            return ranges;
        }

        private static final class FileWriterInfo {
            final Object writer;
            final String path;
            final List<String> partitionValues;
            final int bucket;
            long recordCount;

            FileWriterInfo(Object writer, String path, List<String> partitionValues, int bucket) {
                this.writer = writer;
                this.path = path;
                this.partitionValues = partitionValues;
                this.bucket = bucket;
            }
        }
    }
}
