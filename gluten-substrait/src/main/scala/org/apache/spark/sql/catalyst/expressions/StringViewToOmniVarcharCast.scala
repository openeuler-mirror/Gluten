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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{DataType, Metadata, MetadataBuilder, StringType}

/**
 * A physical-planning marker expression that materializes an Omni StringView column back to Omni
 * VARCHAR. It deliberately does not reuse Spark's generic Cast(StringType -> StringType), because
 * the Omni StringView switch maps ordinary Spark StringType to StringView globally.
 */
case class StringViewToOmniVarcharCast(child: Expression) extends UnaryExpression with Unevaluable {
  override def dataType: DataType = StringType

  override def nullable: Boolean = child.nullable

  override def prettyName: String = "string_view_to_omni_varchar_cast"

  override protected def withNewChildInternal(newChild: Expression): StringViewToOmniVarcharCast =
    copy(child = newChild)
}

/**
 * A physical-planning marker wrapping a string [[Literal]] that must be emitted as an Omni StringView
 * literal (substrait variation 21) instead of the default VARCHAR. Inserted by StringViewFallbackRule
 * for a literal compared (EQ/NEQ) with a StringView column, so native resolves the {SV,SV} comparison
 * (native has no {SV,VARCHAR} comparison overload). The Catalyst dataType stays StringType.
 */
case class StringViewLiteral(literal: Literal) extends UnaryExpression with Unevaluable {
  override def child: Expression = literal
  override def dataType: DataType = StringType
  override def nullable: Boolean = literal.nullable
  override def prettyName: String = "string_view_literal"
  override protected def withNewChildInternal(newChild: Expression): StringViewLiteral =
    copy(literal = newChild.asInstanceOf[Literal])
}

object StringViewToOmniVarcharCast {
  // Attribute-metadata key marking a StringType column whose physical Omni type is StringView.
  // Absent or false means standard VARCHAR — the closed default. ConverterUtils.getTypeNode(attr)
  // reads this to emit substrait variation 21 (StringView) vs 0 (VARCHAR); the native parser
  // decodes the variation literally, with no config dependency. StringView is opt-in: only a data
  // source (switch on) or a whitelisted expression marks a column as StringView. The Catalyst type
  // stays StringType in both cases.
  val PHYSICAL_STRING_VIEW_METADATA_KEY = "org.apache.gluten.omni.physicalStringView"

  /** Mark `base` metadata as physical StringView (emits variation 21). */
  def stringViewMetadata(base: Metadata): Metadata =
    new MetadataBuilder().withMetadata(base).putBoolean(PHYSICAL_STRING_VIEW_METADATA_KEY, true).build()

  /** Force `base` metadata to physical VARCHAR (emits variation 0), clearing any StringView mark. */
  def varcharMetadata(base: Metadata): Metadata =
    new MetadataBuilder().withMetadata(base).putBoolean(PHYSICAL_STRING_VIEW_METADATA_KEY, false).build()

  /** True if `metadata` carries the physical-StringView marker (caller ensures StringType context). */
  def isPhysicalStringView(metadata: Metadata): Boolean =
    metadata.contains(PHYSICAL_STRING_VIEW_METADATA_KEY) &&
      metadata.getBoolean(PHYSICAL_STRING_VIEW_METADATA_KEY)

  /** True if `attr` is a StringType column whose physical Omni type is StringView. */
  def isPhysicalStringView(attr: Attribute): Boolean =
    attr.dataType == StringType && isPhysicalStringView(attr.metadata)
}
