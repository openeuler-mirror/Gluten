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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf

import org.scalatest.funsuite.AnyFunSuite

class OmniSpillMemoryFractionSuite extends AnyFunSuite {
  private val entries = Seq(
    GlutenConfig.COLUMNAR_OMNI_SPILL_MEMORY_FRACTION,
    GlutenConfig.COLUMNAR_SPILL_MEMORY_FRACTION)

  test("spill memory fractions default to 0.9") {
    val conf = new GlutenConfig(new SQLConf)
    assert(conf.omniColumnarSpillMemoryFraction == 0.9)
    assert(conf.columnarSpillMemoryFraction == 0.9)
  }

  test("spill memory fractions preserve fractional values") {
    entries.foreach { entry =>
      Seq(0.123456789, 1e-12, 1.0).foreach { fraction =>
        val conf = new SQLConf
        conf.setConfString(entry.key, fraction.toString)
        assert(conf.getConf(entry) == fraction)
      }
    }
  }

  test("spill memory fractions reject percentages and invalid ratios") {
    entries.foreach { entry =>
      Seq("90", "0", "-0.1", "NaN", "Infinity", "0.9 junk").foreach { value =>
        val conf = new SQLConf
        intercept[IllegalArgumentException] {
          conf.setConfString(entry.key, value)
          conf.getConf(entry)
        }
      }
    }
  }
}
