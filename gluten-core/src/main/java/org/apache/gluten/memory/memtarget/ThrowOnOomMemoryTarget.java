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
package org.apache.gluten.memory.memtarget;

import org.apache.gluten.config.GlutenConfig$;

import org.apache.spark.memory.SparkMemoryUtil;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.task.TaskResources;
import org.apache.spark.util.Utils;

import java.util.function.Function;

public class ThrowOnOomMemoryTarget implements MemoryTarget {
  private final MemoryTarget target;
  private final boolean advisory;

  public ThrowOnOomMemoryTarget(MemoryTarget target) {
    this(target, false);
  }

  public ThrowOnOomMemoryTarget(MemoryTarget target, boolean advisory) {
    this.target = target;
    this.advisory = advisory;
  }

  @Override
  public long borrow(long size) {
    long granted = target.borrow(size);
    if (granted >= size) {
      return granted;
    }
    // OOM happens.
    // Note if the target is a Spark memory consumer, spilling should already be requested but
    // failed to reclaim enough memory.
    if (granted != 0L) {
      target.repay(granted);
    }
    if (advisory) {
      throw new OutOfMemoryException("Spark reservation denied", true);
    }
    // Log memory usage
    if (TaskResources.inSparkTask()) {
      TaskResources.getLocalTaskContext().taskMemoryManager().showMemoryUsage();
    }
    // Build error message, then throw
    StringBuilder errorBuilder = new StringBuilder();
    errorBuilder
        .append(
            String.format(
                "Not enough spark off-heap execution memory. Acquired: %s, granted: %s. "
                    + "Try tweaking config option spark.memory.offHeap.size to get larger "
                    + "space to run this application "
                    + "(if spark.gluten.memory.dynamic.offHeap.sizing.enabled "
                    + "is not enabled). %n",
                Utils.bytesToString(size), Utils.bytesToString(granted)))
        .append("Current config settings: ")
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenConfig$.MODULE$.GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY(),
                getSqlConfStringOrNa(
                    GlutenConfig$.MODULE$.GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY(),
                    ThrowOnOomMemoryTarget::reformatBytes)))
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenConfig$.MODULE$.GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY(),
                getSqlConfStringOrNa(
                    GlutenConfig$.MODULE$.GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY(),
                    ThrowOnOomMemoryTarget::reformatBytes)))
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenConfig$.MODULE$.GLUTEN_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES_KEY(),
                getSqlConfStringOrNa(
                    GlutenConfig$.MODULE$.GLUTEN_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES_KEY(),
                    ThrowOnOomMemoryTarget::reformatBytes)))
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenConfig$.MODULE$.SPARK_OFFHEAP_ENABLED(),
                getSqlConfStringOrNa(
                    GlutenConfig$.MODULE$.SPARK_OFFHEAP_ENABLED(), value -> value)))
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenConfig$.MODULE$.GLUTEN_DYNAMIC_OFFHEAP_SIZING_ENABLED(),
                getSqlConfStringOrNa(
                    GlutenConfig$.MODULE$.GLUTEN_DYNAMIC_OFFHEAP_SIZING_ENABLED(), value -> value)))
        .append(System.lineSeparator());
    // Dump all consumer usages to exception body
    errorBuilder.append(SparkMemoryUtil.dumpMemoryTargetStats(target));
    errorBuilder.append(System.lineSeparator());
    throw new OutOfMemoryException(errorBuilder.toString());
  }

  private static String getSqlConfStringOrNa(
      String confKey, Function<String, String> ifPresent) {
    final SQLConf sqlConf = SQLConf.get();
    if (!sqlConf.contains(confKey)) {
      return "N/A";
    }
    return ifPresent.apply(sqlConf.getConfString(confKey));
  }

  private static String reformatBytes(String in) {
    return Utils.bytesToString(Utils.byteStringAsBytes(in));
  }

  @Override
  public long repay(long size) {
    return target.repay(size);
  }

  @Override
  public long usedBytes() {
    return target.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public static class OutOfMemoryException extends RuntimeException {
    private final boolean advisory;

    public OutOfMemoryException(String message) {
      super(message);
      this.advisory = false;
    }

    private OutOfMemoryException(String message, boolean advisory) {
      super(message, null, true, !advisory);
      this.advisory = advisory;
    }

    public boolean isAdvisory() {
      return advisory;
    }
  }

  public MemoryTarget target() {
    return target;
  }
}
