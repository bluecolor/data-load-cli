package io.blue.connector

trait Connector {
  var name: String = _
  var connectorType: String = _
  var parallel: Integer = _
}