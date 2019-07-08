package io.blue.core.message

import io.blue.connector._
import io.blue.core.metadata._

case class JdbcSinkParams(
  index: Int,
  metadata: Metadata,
  connector: JdbcConnector
)