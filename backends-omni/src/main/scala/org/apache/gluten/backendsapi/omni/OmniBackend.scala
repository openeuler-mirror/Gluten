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
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.GlutenBuildInfo._
import org.apache.gluten.backendsapi._
import org.apache.gluten.columnarbatch.OmniBatch
import org.apache.gluten.component.Component.BuildInfo
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.datasources.orc.OmniOrcFileFormat
import org.apache.gluten.datasources.parquet.OmniParquetFileFormat
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.substrait.rel.LocalFilesNode
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.validate.NativePlanValidationInfo
import org.apache.gluten.vectorized.OmniNativePlanEvaluator
import org.apache.spark.shuffle.OmniShuffleUtil
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, First, Last, Max, Min, StddevSamp, StddevPop, VarianceSamp, VariancePop, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentRow, CumeDist, DenseRank, Lag, Lead, Literal, NamedExpression, NthValue, NTile, Rank, PercentRank, RowNumber, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, WindowExpression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.hive.execution.HiveFileFormat
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

class OmniBackend extends SubstraitBackend {
//  import OmniBackend._

  override def name(): String = OmniBackend.BACKEND_NAME
  override def buildInfo(): BuildInfo =
    BuildInfo("Omni", OMNI_BRANCH, OMNI_REVISION, OMNI_REVISION_TIME)
  override def iteratorApi(): IteratorApi = new OmniIteratorApiImpl
  override def sparkPlanExecApi(): SparkPlanExecApi = new OmniSparkPlanExecApi
  override def transformerApi(): TransformerApi = new OmniTransformerApi
  override def validatorApi(): ValidatorApi = new OmniValidatorApi
  override def metricsApi(): MetricsApi = new OmniMetricsApiImpl
  override def listenerApi(): ListenerApi = new OmniListenerApi
  override def ruleApi(): RuleApi = new OmniRuleApi
  override def settings(): BackendSettingsApi = OmniBackendSettings
}

object OmniBackend {
  val BACKEND_NAME: String = "omni"
  val CONF_PREFIX: String = GlutenConfig.prefixOf(BACKEND_NAME)
}

object DataTypeUtils {
  def isPrimitiveType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | StringType |
           _: DecimalType | DateType | TimestampType | NullType | BinaryType =>
        true
      case _ => false
    }
  }
}

object OmniBackendSettings extends BackendSettingsApi {
  val SHUFFLE_SUPPORTED_CODEC = Set("lz4", "zstd", "snappy", "zlib")
  val GLUTEN_OMNI_UDF_LIB_PATHS = OmniBackend.CONF_PREFIX + ".udfLibraryPaths"
  val GLUTEN_OMNI_DRIVER_UDF_LIB_PATHS = OmniBackend.CONF_PREFIX + ".driver.udfLibraryPaths"
  val GLUTEN_OMNI_INTERNAL_UDF_LIB_PATHS = OmniBackend.CONF_PREFIX + ".internal.udfLibraryPaths"
  val GLUTEN_OMNI_UDF_ALLOW_TYPE_CONVERSION = OmniBackend.CONF_PREFIX + ".udfAllowTypeConversion"

  /**
   * When true, the BHJ hash table is built only once per executor and shared across all tasks
   * that probe the same broadcast relation. This matches the ClickHouse/Velox optimization and
   * reduces both CPU (no redundant builds) and peak memory (only one copy per executor).
   * Corresponds to the native config key "spark.gluten.omni.buildHashTableOncePerExecutor.enabled".
   */
  val GLUTEN_OMNI_BUILD_HASH_TABLE_ONCE_PER_EXECUTOR =
    "spark.gluten.omni.buildHashTableOncePerExecutor.enabled"

  /** The columnar-batch type this backend is by default using. */
  override def primaryBatchType: Convention.BatchType = OmniBatch

  override def enableJoinKeysRewrite(): Boolean = false

  override def validateScanExec(
      format: ReadFileFormat,
      fields: Array[StructField],
      rootPaths: Seq[String],
      properties: Map[String, String],
      serializableHadoopConf: Option[SerializableConfiguration] = None): ValidationResult = {
    def checkUnsupportedDataTypes: ValidationResult = {
      //Collect unsupported types
      val unsupportedDataTypes = fields.map(_.dataType).collect {
        case m: MapType
          if (!DataTypeUtils.isPrimitiveType(m.keyType) || !DataTypeUtils.isPrimitiveType(m.valueType)) => "nested MapType"
        case a: ArrayType
          if (!DataTypeUtils.isPrimitiveType(a.elementType)) => "nested ArrayType"
      }
      for (unsupportedDataType <- unsupportedDataTypes) {
        return ValidationResult.failed(s"Validation failed for ${this.getClass.toString}"
          + s"deu to: data type $unsupportedDataType in file schema.")
      }
      ValidationResult.succeeded
    }
    format match {
      case ReadFileFormat.ParquetReadFormat => checkUnsupportedDataTypes
      case ReadFileFormat.OrcReadFormat => checkUnsupportedDataTypes
      case _ => ValidationResult.failed(s"Unsupported file format $format")
    }
  }

  override def needOutputSchemaForPlan(): Boolean = true

  override def getSubstraitReadFileFormatV1(
      fileFormat: FileFormat): LocalFilesNode.ReadFileFormat = {
    fileFormat.getClass.getSimpleName match {
      case "OrcFileFormat" => ReadFileFormat.OrcReadFormat
      case "ParquetFileFormat" => ReadFileFormat.ParquetReadFormat
      case "OmniOrcFileFormat" => ReadFileFormat.OrcReadFormat
      case "OmniParquetFileFormat" => ReadFileFormat.ParquetReadFormat
      // Delta Lake and other workflows scan JSON (e.g. _delta_log); Omni has no native JSON scan —
      // map to a concrete ReadFileFormat so validation fails cleanly and the scan falls back to JVM.
      case "JsonFileFormat" | "JSONFileFormat" => ReadFileFormat.JsonReadFormat
      case "CSVFileFormat" => ReadFileFormat.TextReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  override def getSubstraitReadFileFormatV2(scan: Scan): LocalFilesNode.ReadFileFormat = {
    scan.getClass.getSimpleName match {
      case "OrcScan" => ReadFileFormat.OrcReadFormat
      case "ParquetScan" => ReadFileFormat.ParquetReadFormat
      case "DwrfScan" => ReadFileFormat.DwrfReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  override def supportNativeWrite(fields: Array[StructField]): Boolean = {
    fields.map {
      field =>
        field.dataType match {
          case _: YearMonthIntervalType => return false
          case _ =>
        }
    }
    true
  }

  override def supportWriteFilesExec(
                                      format: FileFormat,
                                      fields: Array[StructField],
                                      bucketSpec: Option[BucketSpec],
                                      options: Map[String, String]): ValidationResult = {

    def validateHiveFileFormat(hiveFileFormat: HiveFileFormat): Option[String] = {
      val fileSinkConfField = format.getClass.getDeclaredField("fileSinkConf")
      fileSinkConfField.setAccessible(true)
      val fileSinkConf = fileSinkConfField.get(hiveFileFormat)
      val tableInfoField = fileSinkConf.getClass.getDeclaredField("tableInfo")
      tableInfoField.setAccessible(true)
      val tableInfo = tableInfoField.get(fileSinkConf)
      val getOutputFileFormatClassNameMethod = tableInfo.getClass
        .getDeclaredMethod("getOutputFileFormatClassName")
      val outputFileFormatClassName = getOutputFileFormatClassNameMethod.invoke(tableInfo)

      outputFileFormatClassName match {
        case "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat" =>
          None
        case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" =>
          None
        case _ =>
          Some(
            "HiveFileFormat is supported only with orc/parquet as the output file type"
          ) // Unsupported format
      }
    }

    // Validate if all types are supported.
    def validateDataTypes(): Option[String] = {
      val unsupportedTypes: Seq[String] = format match {
        case _: OrcFileFormat | _: OmniOrcFileFormat =>
          fields.flatMap {
            case StructField(_, _: YearMonthIntervalType, _, _) =>
              Some("YearMonthIntervalType")
            case _ => None
          }
        case _ => Seq.empty
      }
      if (unsupportedTypes.nonEmpty) {
        Some(unsupportedTypes.mkString("Found unsupported type:", ",", ""))
      } else {
        None
      }
    }

    def validateFileFormat(): Option[String] = {
      format match {
        case _: OrcFileFormat => None // Orc is directly supported
        case _: ParquetFileFormat => None // Parquet is directly supported
        case _: OmniOrcFileFormat => None // Omni native Orc writer
        case _: OmniParquetFileFormat => None // Omni native Parquet writer
        case h: HiveFileFormat if GlutenConfig.get.enableHiveFileFormatWriter =>
          validateHiveFileFormat(h) // Orc via Hive SerDe
        case _ =>
          Some(
            "Only OrcFileFormat, ParquetFileFormat, OmniOrcFileFormat, " +
              "OmniParquetFileFormat and HiveFileFormat are supported."
          ) // Unsupported format
      }
    }
    validateFileFormat().orElse(validateDataTypes()) match {
      case Some(reason) => ValidationResult.failed(reason)
      case _ => ValidationResult.succeeded
    }
  }

  override def shuffleSupportedCodec(): Set[String] = SHUFFLE_SUPPORTED_CODEC

  override def enableNativeWriteFiles(): Boolean = {
    GlutenConfig.get.enableNativeWriter.getOrElse(
      SparkShimLoader.getSparkShims.enableNativeWriteFilesByDefault()
    )
  }

  /** Omni supports AppendDataExec (Iceberg append write uses columnar offload). */
  override def supportAppendDataExec(): Boolean = true

  override def supportSortExec(): Boolean = true

  override def supportExpandExec(): Boolean = true
  
  override def supportCartesianProductExec(): Boolean = true

  override def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    var isSupport: Boolean  = true
    windowFunctions.foreach {
      windowExpr =>
        val aliasExpr = windowExpr.asInstanceOf[Alias]
        aliasExpr.child match {
          case wExpression: WindowExpression =>
            val isOffsetFunc = wExpression.windowFunction match {
              case RowNumber() | Rank(_) | PercentRank(_) | CumeDist() | DenseRank(_) =>
                false
              case _: NTile =>
                false
              case _: NthValue =>
                false
              case _: Lead | _: Lag =>
                true
              case AggregateExpression(aggFunction, _, false, _, _) =>
                aggFunction match {
                  case _: Sum =>
                  case _: Max =>
                  case _: Average =>
                  case _: Min =>
                  case _: StddevSamp =>
                  case _: StddevPop =>
                  case _: VarianceSamp =>
                  case _: VariancePop =>
                  case Count(Literal(1, IntegerType) :: Nil) | Count(ArrayBuffer(Literal(1, IntegerType))) =>
                  case Count(_) if aggFunction.children.size == 1 =>
                  case _: First =>
                  case _: Last =>
                  case _ => isSupport = false
                }
                false
              case _ =>
                isSupport = false
                false
            }
            if (!isOffsetFunc) {
              wExpression.windowSpec.frameSpecification match {
                case swf: SpecifiedWindowFrame =>
                  if (swf.lower != UnboundedPreceding && swf.lower != CurrentRow) {
                    isSupport = false
                  }
                  if (swf.upper != UnboundedFollowing && swf.upper != CurrentRow) {
                    isSupport = false
                  }
                case _ =>
              }
            }
          case _ =>
            isSupport = false
        }
    }
    isSupport
  }

  override def supportColumnarShuffleExec(): Boolean = {
    val conf = GlutenConfig.get
    conf.enableColumnarShuffle && (conf.isUseGlutenShuffleManager
      || conf.isUseColumnarShuffleManager
      || conf.isUseCelebornShuffleManager
      || conf.isUseUniffleShuffleManager)
  }

  override def transformCheckOverflow: Boolean = false
}

class OmniValidatorApi extends ValidatorApi {

  /** Validate against Substrait plan node in native backend. */
  override def doNativeValidateWithFailureReason(plan: PlanNode): ValidationResult = {
    TaskResources.runUnsafe {
      val validator = OmniNativePlanEvaluator.create(BackendsApiManager.getBackendName)
      asValidationResult(validator.doNativeValidateWithFailureReason(plan.toProtobuf.toByteArray))
    }
  }

  private def asValidationResult(info: NativePlanValidationInfo): ValidationResult = {
    if (info.isSupported == 1) {
      return ValidationResult.succeeded
    }
    ValidationResult.failed(
      String.format(
        "Native validation failed: %n%s",
        info.fallbackInfo.asScala.reduce[String] { case (l, r) => l + "\n" + r }))
  }

  /** Validate against ColumnarShuffleExchangeExec. */
  override def doColumnarShuffleExchangeExecValidate(
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      child: SparkPlan): Option[String] = {
    if (outputAttributes.isEmpty) {
      // See: https://github.com/apache/incubator-gluten/issues/7600.
      return Some("Shuffle with empty output schema is not supported")
    }
    if (child.output.isEmpty) {
      // See: https://github.com/apache/incubator-gluten/issues/7600.
      return Some("Shuffle with empty input schema is not supported")
    }
    val res = doSchemaValidateForShuffle(child.schema)
    if (res.isDefined){
      return res
    }
    OmniShuffleUtil.doShuffleValidate(outputAttributes, outputPartitioning, child)
  }

  def doSchemaValidateForShuffle(schema: DataType): Option[String] = {
    if (DataTypeUtils.isPrimitiveType(schema)) {
      return None
    }
    schema match {
      case map: MapType =>
        doSchemaValidate(map.keyType).orElse(doSchemaValidate(map.valueType))
      case struct: StructType =>
        struct.fields.foreach {
          f =>
            val reason = doSchemaValidateForShuffle(f.dataType)
            if (reason.isDefined) {
              return reason
            }
        }
        None
      case array: ArrayType =>
        doSchemaValidate(array.elementType)
      case _ =>
        Some(s"Schema / data type not supported: $schema")
    }
  }

  override def doSchemaValidate(schema: DataType): Option[String] = {
    if (DataTypeUtils.isPrimitiveType(schema)) {
      return None
    }
    schema match {
      case map: MapType =>
        doSchemaValidate(map.keyType).orElse(doSchemaValidate(map.valueType))
      case struct: StructType =>
        struct.fields.foreach {
          f =>
            val reason = doSchemaValidate(f.dataType)
            if (reason.isDefined) {
              return reason
            }
        }
        None
      case array: ArrayType =>
        doSchemaValidate(array.elementType)
      case _: BinaryType =>
        None
      case _ =>
        Some(s"Schema / data type not supported: $schema")
    }
  }
}
