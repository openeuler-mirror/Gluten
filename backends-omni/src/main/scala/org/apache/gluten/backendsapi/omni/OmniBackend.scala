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
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.substrait.rel.LocalFilesNode
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.validate.NativePlanValidationInfo
import org.apache.gluten.vectorized.OmniNativePlanEvaluator
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters.asScalaBufferConverter

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

object OmniBackendSettings extends BackendSettingsApi {
  val SHUFFLE_SUPPORTED_CODEC = Set("lz4", "zstd")

  /** The columnar-batch type this backend is by default using. */
  override def primaryBatchType: Convention.BatchType = OmniBatch

  override def validateScanExec(
      format: ReadFileFormat,
      fields: Array[StructField],
      rootPaths: Seq[String],
      properties: Map[String, String],
      serializableHadoopConf: Option[SerializableConfiguration] = None): ValidationResult = {
    // todo: add some check
    ValidationResult.succeeded
  }

  override def needOutputSchemaForPlan(): Boolean = true

  override def getSubstraitReadFileFormatV1(
      fileFormat: FileFormat): LocalFilesNode.ReadFileFormat = {
    fileFormat.getClass.getSimpleName match {
      case "OrcFileFormat" => ReadFileFormat.OrcReadFormat
      case "ParquetFileFormat" => ReadFileFormat.ParquetReadFormat
      case "DwrfFileFormat" => ReadFileFormat.DwrfReadFormat
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

  override def shuffleSupportedCodec(): Set[String] = SHUFFLE_SUPPORTED_CODEC

  override def enableNativeWriteFiles(): Boolean = true

  override def supportSortExec(): Boolean = true

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
    doSchemaValidate(child.schema)
  }

  private def isPrimitiveType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | ShortType | IntegerType | LongType | DoubleType | StringType |
          _: DecimalType | DateType | TimestampType =>
        true
      case _ => false
    }
  }

  override def doSchemaValidate(schema: DataType): Option[String] = {
    if (isPrimitiveType(schema)) {
      return None
    }
    // unsupported any complex type
    schema match {
      case struct: StructType =>
        struct.fields.foreach {
          f =>
            val reason = doSchemaValidate(f.dataType)
            if (reason.isDefined) {
              return reason
            }
        }
        None
      case _ =>
        Some(s"Schema / data type not supported: $schema")
    }
  }
}
