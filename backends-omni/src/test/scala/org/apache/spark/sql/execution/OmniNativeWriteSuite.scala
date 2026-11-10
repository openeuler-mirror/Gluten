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
package org.apache.spark.sql.execution

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.{OmniCreateDataSourceTableAsSelectCommand, OmniInsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.execution.{GlutenHiveCtas, OmniInsertIntoHiveDirCommand, OmniInsertIntoHiveTable}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

/**
 * Parameterized coverage for the Q33 native file-writer matrix.
 *
 * Each case is executed twice to check whether the write is routed through Gluten/Omni.
 * An Omni command can still use a row writer for unsupported formats.  The
 * SQL is kept as one statement per case so adding a new format or write mode
 * does not require another true/false SQL directory.
 */
class OmniNativeWriteSuite extends GlutenQueryTest with SQLTestUtils {

  private val nativeWriterKey = "spark.gluten.sql.native.writer.enabled"
  private val database = "omni_native_write_suite"
  private var _spark: SparkSession = _

  private case class WriteCase(
      name: String,
      table: Option[String],
      createTable: Option[String],
      statement: String,
      expectedRows: Int = 8,
      directoryFormat: Option[String] = None)

  override protected def spark: SparkSession = _spark

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")
    spark.sql(s"CREATE DATABASE $database")
    spark.sql(s"USE $database")
    spark.sql("CREATE TABLE src_kv (id BIGINT, value STRING, p STRING) STORED AS PARQUET")
    spark.sql(
      "INSERT INTO src_kv " +
        "SELECT id, concat('v', cast(id AS STRING)), " +
        "CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END FROM range(8)")
  }

  override protected def afterAll(): Unit = {
    try {
      if (_spark != null) {
        _spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")
        _spark.stop()
        _spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
      .set("spark.master", "local[2]")
      .set("spark.sql.test", "")
      .set("spark.sql.hive.convertMetastoreParquet", "false")
      .set("spark.sql.hive.convertMetastoreOrc", "false")
      .set("spark.sql.hive.convertMetastoreCtas", "false")
      .set(HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
        "org.apache.spark.sql.hive.execution.PairSerDe")
      .set(UI_ENABLED, false)
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
      .set(StaticSQLConf.WAREHOUSE_PATH, getClass.getResource("/").getPath + "/omni-native-write")

    val omniLibPath = sys.props.get("omni.lib.path").orElse(
      sys.env.get("OMNI_HOME").map(_ + "/lib/libspark_columnar_plugin.so"))
    omniLibPath.foreach(path => conf.set("spark.gluten.sql.columnar.libpath", path))

    conf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.default.parallelism", "2")
  }

  private val cases: Seq[WriteCase] = Seq(
    WriteCase("01 hive non-partitioned parquet insert", Some("t01_hive_np"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) STORED AS PARQUET"),
      "INSERT INTO TABLE %s SELECT id, value FROM src_kv"),
    WriteCase("02 hive dynamic partition parquet insert", Some("t02_hive_dp"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) PARTITIONED BY (p STRING) STORED AS PARQUET"),
      "INSERT INTO TABLE %s PARTITION (p) SELECT id, value, p FROM src_kv"),
    WriteCase("03 hive parquet CTAS", Some("t03_hive_ctas_np"), None,
      "CREATE TABLE %s STORED AS PARQUET AS SELECT id, value FROM src_kv"),
    WriteCase("04 hive text CTAS", Some("t04_hive_ctas_text"), None,
      "CREATE TABLE %s STORED AS TEXTFILE AS SELECT id, value FROM src_kv"),
    WriteCase("05 hive dynamic partition CTAS", Some("t05_hive_ctas_dp"), None,
      "CREATE TABLE %s STORED AS PARQUET PARTITIONED BY (p) AS SELECT id, value, p FROM src_kv"),
    WriteCase("06 parquet directory insert", None, None,
      "INSERT OVERWRITE DIRECTORY '%s' STORED AS PARQUET SELECT id, value FROM src_kv",
      directoryFormat = Some("parquet")),
    WriteCase("07 text directory insert", None, None,
      "INSERT OVERWRITE DIRECTORY '%s' SELECT id, value FROM src_kv",
      directoryFormat = Some("text")),
    WriteCase("08 datasource non-partitioned parquet insert", Some("t08_ds_np"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING parquet"),
      "INSERT INTO TABLE %s SELECT id, value FROM src_kv"),
    WriteCase("09 datasource static partition parquet insert", Some("t09_ds_sp"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING parquet PARTITIONED BY (p STRING)"),
      "INSERT INTO TABLE %s PARTITION (p='even') SELECT id, value FROM src_kv WHERE p='even'",
      expectedRows = 4),
    WriteCase("10 datasource dynamic partition parquet insert", Some("t10_ds_dp"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING parquet PARTITIONED BY (p STRING)"),
      "INSERT INTO TABLE %s SELECT id, value, p FROM src_kv"),
    WriteCase("11 datasource parquet CTAS", Some("t11_ds_ctas"), None,
      "CREATE TABLE %s USING parquet AS SELECT id, value FROM src_kv"),
    WriteCase("12 datasource partitioned parquet CTAS", Some("t12_ds_ctas_p"), None,
      "CREATE TABLE %s USING parquet PARTITIONED BY (p) AS SELECT id, value, p FROM src_kv"),
    WriteCase("13 datasource dynamic partition overwrite", Some("t13_ds_dp2"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING parquet PARTITIONED BY (p STRING)"),
      "INSERT OVERWRITE TABLE %s SELECT id, value, p FROM src_kv"),
    WriteCase("14 datasource parquet bucket", Some("t14_ds_bucket"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING parquet CLUSTERED BY (id) INTO 2 BUCKETS"),
      "INSERT INTO TABLE %s SELECT id, value FROM src_kv"),
    WriteCase("15 datasource sorted parquet bucket", Some("t15_ds_bucket_sorted"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING parquet CLUSTERED BY (id) SORTED BY (id) INTO 2 BUCKETS"),
      "INSERT INTO TABLE %s SELECT id, value FROM src_kv"),
    WriteCase("16 datasource ORC bucket", Some("t16_ds_orc_bucket"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING orc CLUSTERED BY (id) INTO 2 BUCKETS"),
      "INSERT INTO TABLE %s SELECT id, value FROM src_kv"),
    WriteCase("17 datasource partitioned parquet bucket", Some("t17_ds_part_bucket"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING parquet " +
        "PARTITIONED BY (p STRING) CLUSTERED BY (id) INTO 2 BUCKETS"),
      "INSERT INTO TABLE %s SELECT id, value, p FROM src_kv"),
    WriteCase("18 datasource ORC non-partitioned insert", Some("t18_ds_orc_np"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING orc"),
      "INSERT INTO TABLE %s SELECT id, value FROM src_kv"),
    WriteCase("19 datasource ORC static partition insert", Some("t19_ds_orc_sp"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING orc PARTITIONED BY (p STRING)"),
      "INSERT INTO TABLE %s PARTITION (p='even') SELECT id, value FROM src_kv WHERE p='even'",
      expectedRows = 4),
    WriteCase("20 datasource ORC dynamic partition insert", Some("t20_ds_orc_dp"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING orc PARTITIONED BY (p STRING)"),
      "INSERT INTO TABLE %s SELECT id, value, p FROM src_kv"),
    WriteCase("21 datasource ORC CTAS", Some("t21_ds_orc_ctas"), None,
      "CREATE TABLE %s USING orc AS SELECT id, value FROM src_kv"),
    WriteCase("22 datasource partitioned ORC CTAS", Some("t22_ds_orc_ctas_p"), None,
      "CREATE TABLE %s USING orc PARTITIONED BY (p) AS SELECT id, value, p FROM src_kv"),
    WriteCase("23 datasource partitioned ORC bucket", Some("t23_ds_orc_part_bucket"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING orc " +
        "PARTITIONED BY (p STRING) CLUSTERED BY (id) INTO 2 BUCKETS"),
      "INSERT INTO TABLE %s SELECT id, value, p FROM src_kv"),
    WriteCase("24 datasource ORC bucket overwrite", Some("t24_ds_orc_bucket_ow"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING orc CLUSTERED BY (id) INTO 2 BUCKETS"),
      "INSERT OVERWRITE TABLE %s SELECT id, value FROM src_kv"),
    WriteCase("25 datasource multi-column ORC bucket", Some("t25_ds_orc_bucket_multi"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) USING orc CLUSTERED BY (id, value) INTO 2 BUCKETS"),
      "INSERT INTO TABLE %s SELECT id, value FROM src_kv"),
    WriteCase("26 hive ORC non-partitioned insert", Some("t26_hive_orc_np"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) STORED AS ORC"),
      "INSERT INTO TABLE %s SELECT id, value FROM src_kv"),
    WriteCase("27 hive ORC dynamic partition insert", Some("t27_hive_orc_dp"),
      Some("CREATE TABLE %s (id BIGINT, value STRING) PARTITIONED BY (p STRING) STORED AS ORC"),
      "INSERT INTO TABLE %s PARTITION (p) SELECT id, value, p FROM src_kv"),
    WriteCase("28 hive ORC CTAS", Some("t28_hive_orc_ctas"), None,
      "CREATE TABLE %s STORED AS ORC AS SELECT id, value FROM src_kv"),
    WriteCase("29 ORC directory insert", None, None,
      "INSERT OVERWRITE DIRECTORY '%s' STORED AS ORC SELECT id, value FROM src_kv",
      directoryFormat = Some("orc"))
  )

  cases.foreach { testCase =>
    Seq(true, false).foreach { enableNativeWrite =>
      test(s"${testCase.name}, native=$enableNativeWrite") {
        runCase(testCase, enableNativeWrite)
      }
    }
  }

  private def runCase(testCase: WriteCase, enableNativeWrite: Boolean): Unit = {
    val tableName = testCase.table.map(t => s"$database.$t")
    val directory = testCase.directoryFormat.map(_ =>
      java.nio.file.Files.createTempDirectory("omni-native-write-").toFile)

    try {
      tableName.foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))
      testCase.createTable.foreach(sql => spark.sql(sql.format(tableName.get)))
      val statement = testCase.statement.format(
        tableName.orElse(directory.map(_.getCanonicalPath.replace("\\", "/"))).get)

      withSQLConf(
        nativeWriterKey -> enableNativeWrite.toString,
        "hive.exec.dynamic.partition.mode" -> "nonstrict",
        "spark.sql.hive.convertMetastoreParquet" -> "false",
        "spark.sql.hive.convertMetastoreOrc" -> "false",
        "spark.sql.hive.convertMetastoreCtas" -> "false") {
        val result = spark.sql(statement)
        result.collect()
        val plan = result.queryExecution.executedPlan
        assert(
          usesOmniWriteRoute(plan) == enableNativeWrite,
          s"Unexpected Omni write routing for ${testCase.name}, native=$enableNativeWrite:\n${plan.treeString}")
      }

      tableName.foreach { t =>
        checkAnswer(spark.sql(s"SELECT count(*) FROM $t"), Row(testCase.expectedRows.toLong))
      }
      directory.foreach { dir =>
        val reader = testCase.directoryFormat.get match {
          case "parquet" => spark.read.parquet(dir.getCanonicalPath)
          case "orc" => spark.read.orc(dir.getCanonicalPath)
          case "text" => spark.read.text(dir.getCanonicalPath)
        }
        assert(reader.count() == testCase.expectedRows)
      }
    } finally {
      directory.foreach(Utils.deleteRecursively)
      tableName.foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))
    }
  }

  private def usesOmniWriteRoute(plan: SparkPlan): Boolean = plan match {
    // Eager SQL commands keep the executed write plan outside SparkPlan.children.
    case result: CommandResultExec => usesOmniWriteRoute(result.commandPhysicalPlan)
    case adaptive: AdaptiveSparkPlanExec => usesOmniWriteRoute(adaptive.executedPlan)
    case _: ColumnarWriteFilesExec => true
    case DataWritingCommandExec(command, child) =>
      command match {
        case _: GlutenHiveCtas | _: OmniInsertIntoHiveTable | _: OmniInsertIntoHiveDirCommand |
            _: OmniInsertIntoHadoopFsRelationCommand | _: OmniCreateDataSourceTableAsSelectCommand =>
          true
        case _ => usesOmniWriteRoute(child)
      }
    case ExecutedCommandExec(_: GlutenHiveCtas) => true
    case other => other.children.exists(usesOmniWriteRoute)
  }
}
