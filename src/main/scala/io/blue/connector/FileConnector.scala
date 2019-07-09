package io.blue.connector

class FileConnector extends Connector {
  var path: String = _
  var fieldDelimiter: String = _
  var recordSeperator: String = _

  def count(store: String, filter: String): Long = {
    0
  }
}