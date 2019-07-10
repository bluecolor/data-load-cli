package io.blue.connector

import com.typesafe.scalalogging._
import io.blue.core.metadata._

class OracleHashConnector extends JdbcConnector with LazyLogging {


  def findHashes(table: String, parallel: Int): List[Int] = {
    val query = s"""
      select distinct ora_hash(rowid, ${parallel}) from ${table}
    """
    logger.debug(query)
    val connection = connect
    val rs = connection.createStatement.executeQuery(query)
    var hashes: List[Int] = List()
    while(rs.next) {
      println(rs.getInt(1))
      hashes ::= rs.getInt(1)
    }
    connection.close
    hashes
  }


  def getMetadata(table: String, parallel: Int) : OracleHashMetadata = {
    var metadata = new OracleHashMetadata
    metadata.hashes = findHashes(table, parallel)
    metadata.table = table
    metadata.columns = getColumns(table)
    metadata
  }

}