package io.blue.core.message

import io.blue.connector._

case class JdbcProducerParams(index: Int, table: String, filter: String, connector: JdbcConnector)