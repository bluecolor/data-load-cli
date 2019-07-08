package io.blue.core.message

import io.blue.connector._
import io.blue.core.metadata._

case class OracleRowidProducerParams(
  index: Int,
  table: String,
  ranges: (String, String),
  columns: List[Column],
  connector: OracleRowidConnector
)