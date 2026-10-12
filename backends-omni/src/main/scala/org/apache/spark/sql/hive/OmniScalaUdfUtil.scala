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
package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.expression.UDFResolver

object OmniScalaUdfUtil {
  private val javaUdfPrefix = "org.apache.spark.sql.api.java.UDF"

  def scalaUdfSqlName(expr: Expression): Option[String] = expr match {
    case s: ScalaUDF => s.udfName.filter(_.nonEmpty)
    case _ => None
  }

  def scalaUdfJavaClassName(expr: Expression): Option[String] = expr match {
    case s: ScalaUDF => javaUdfClassName(s.function)
    case _ => None
  }

  def registeredNativeName(expr: Expression): Option[String] = {
    val candidates = Seq(scalaUdfJavaClassName(expr), scalaUdfSqlName(expr)).flatten.distinct
    candidates.find(UDFResolver.UDFNames.contains)
  }

  def isRegisteredNativeScalaUdf(expr: Expression): Boolean =
    registeredNativeName(expr).isDefined

  private def javaUdfClassName(function: AnyRef): Option[String] = {
    findJavaUdfClass(function, 0)
  }

  private def findJavaUdfClass(obj: Any, depth: Int): Option[String] = {
    if (obj == null || depth > 4) {
      return None
    }
    if (isSparkJavaUdf(obj)) {
      return Some(obj.getClass.getName)
    }
    val seen = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
    val queue = new java.util.ArrayDeque[AnyRef]()
    queue.add(obj.asInstanceOf[AnyRef])
    var hops = 0
    while (!queue.isEmpty && hops < 32) {
      val cur = queue.poll()
      hops += 1
      if (cur != null && !seen.containsKey(cur)) {
        seen.put(cur, java.lang.Boolean.TRUE)
        if (isSparkJavaUdf(cur)) {
          return Some(cur.getClass.getName)
        }
        var cls: Class[_] = cur.getClass
        while (cls != null && cls != classOf[Object]) {
          cls.getDeclaredFields.foreach { field =>
            field.setAccessible(true)
            val captured =
              try field.get(cur)
              catch { case _: Throwable => null }
            captured match {
              case ref: AnyRef if ref != cur && !seen.containsKey(ref) =>
                queue.add(ref)
              case _ =>
            }
          }
          cls = cls.getSuperclass
        }
      }
    }
    None
  }

  private def isSparkJavaUdf(obj: Any): Boolean = {
    obj.getClass.getInterfaces.exists { iface =>
      val name = iface.getName
      name.startsWith(javaUdfPrefix) &&
      name.length > javaUdfPrefix.length &&
      name.substring(javaUdfPrefix.length).forall(_.isDigit)
    }
  }
}
