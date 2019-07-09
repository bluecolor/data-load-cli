package io.blue.connector

trait Connector {
  var name: String = _
  var connectorType: String = _
  var parallel: Integer = _
  var batchSize: Int = _

  def count(name: String, filter: String): Long
}