package org.apache.gluten.utils.omni

import org.apache.gluten.utils.{BackendTestSettings, SQLQueryTestSettings}

class OmniTestSettings extends BackendTestSettings {
  override def shouldRun(suiteName: String, testName: String): Boolean = {
    // Spark 3.2 upstream suites do not install the Omni native libpath fixture.
    // Keep them discovered but ignored; backend-specific Gluten suites run in
    // gluten-ut/test and are covered separately by this build profile.
    false
  }

  override def getSQLQueryTestSettings: SQLQueryTestSettings = OmniSQLQueryTestSettings
}
