package org.apache.gluten.utils.omni

import org.apache.gluten.utils.SQLQueryTestSettings

object OmniSQLQueryTestSettings extends SQLQueryTestSettings {
  override def getSupportedSQLQueryTests: Set[String] = Set.empty
  override def getOverwriteSQLQueryTests: Set[String] = Set.empty
}
