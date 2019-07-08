package io.blue.connector

import java.sql._
import com.typesafe.scalalogging._

import io.blue.core.metadata._
import io.blue.config.Constants.ConnectorType

class JdbcConnector extends Connector with LazyLogging {
  var url: String = _
  var username: String = _
  var password: String = _
  var driverClass: String = _

  def connect = {
    Class.forName(driverClass)
    DriverManager.getConnection(url, username, password)
  }

  def getColumns(table: String): List[Column] = {
    val connection = connect
    var query = s"select * from ${table} where 1=2"
    logger.trace(s"Finding all columns of the table ${table} ...")
    logger.trace(query)

    val rs = connection.createStatement.executeQuery(query)
    val md = rs.getMetaData

    val columns  = (1 to md.getColumnCount).map{ i =>
      var column = new Column
      column.name = md.getColumnName(i)
      column.columnType = md.getColumnType(i)
      column.columnTypeName = md.getColumnTypeName(i)
      column
    }.toList
    logger.trace("Done finding columns")
    connection.close
    columns
  }

  def getMetadata(table: String): Metadata = {
    var metadata = new Metadata
    metadata.table = table
    metadata.columns = getColumns(table)
    metadata
  }

}