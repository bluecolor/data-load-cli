package io.blue.core.message

import io.blue.connector._
import io.blue.core.metadata._

case class OracleHashProducerParams(
  index: Int,
  parallel: Int,
  table: String,
  hashId: Int,
  columns: List[Column],
  connector: OracleHashConnector
)