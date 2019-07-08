package io.blue.config


object Constants {
  var MAX_PARALLEL = 64

  object ConnectorType {
    val ORACLE_ROWID = "ORACLE_ROWID"
    val JDBC = "JDBC"
    val FILE = "FILE"
  }
}