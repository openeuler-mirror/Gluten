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
package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.internal.SQLConf
import java.util.UUID
import java.io.{File, FileNotFoundException, InputStream}
import java.nio.file.{Files, StandardCopyOption}

class GlutenCSVSuite extends CSVSuite with GlutenSQLTestsBaseTrait {

  /** Returns full path to the given file in the resource folder */
  override protected def testFile(fileName: String): String = {
    val in: InputStream = getClass.getClassLoader.getResourceAsStream(fileName)
    if (in == null) throw new FileNotFoundException(fileName)

    try {
      val tempDir = System.getProperty("java.io.tmpdir")
      val tempFile = new File(tempDir, s"spark-test-${UUID.randomUUID()}-${new File(fileName).getName}")

      Files.copy(in, tempFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      tempFile.deleteOnExit()
      tempFile.getAbsolutePath
    } finally {
      in.close()
    }
  }
}

class GlutenCSVv1Suite extends GlutenCSVSuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "csv")
}

class GlutenCSVv2Suite extends GlutenCSVSuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}

class GlutenCSVLegacyTimeParserSuite extends GlutenCSVSuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.LEGACY_TIME_PARSER_POLICY, "legacy")
}
