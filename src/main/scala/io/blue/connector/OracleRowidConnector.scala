package io.blue.connector

import com.typesafe.scalalogging._
import io.blue.core.metadata._

class OracleRowidConnector extends JdbcConnector with LazyLogging {

  def getSplitQuery(ownerName: String, tableName: String, parallel: Int) = {
    s"""
      SELECT
        dbms_rowid.rowid_create (1,data_object_id,lo_fno,lo_block,0) ROWID_FROM,
        --------------------------------------------------------------------------
        dbms_rowid.rowid_create (1,data_object_id,hi_fno,hi_block, 10000) ROWID_TO
        --------------------------------------------------------------------------
      FROM (WITH c1 AS
        ( SELECT   *
          FROM dba_extents
          WHERE segment_name = UPPER ('${tableName}') AND owner = UPPER ('${ownerName}')
          ORDER BY block_id
        )
        SELECT DISTINCT grp,
            first_value(relative_fno) OVER (partition by grp order by relative_fno,
            block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lo_fno,
            first_value (block_id) OVER (partition by grp order by relative_fno,
            block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lo_block,
            last_value (relative_fno) OVER (partition by grp order by relative_fno,
            block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hi_fno,
            last_value (block_id + blocks - 1) OVER (partition by grp order by relative_fno,
            block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hi_block,
            sum (blocks) OVER (PARTITION BY grp) AS sum_blocks
        FROM (SELECT relative_fno, block_id, blocks,
                TRUNC(  (  SUM (blocks) OVER (ORDER BY relative_fno,block_id)- 0.01)
                  / (SUM (blocks) OVER () / ${parallel}) ) grp
                  FROM c1
                  WHERE segment_name = UPPER ('${tableName}')
                  AND owner = UPPER ('${ownerName}')
              ORDER BY block_id)),
      (SELECT data_object_id
          FROM all_objects
        WHERE object_name = UPPER ('${tableName}') AND owner = UPPER ('${ownerName}'))
    """
  }

  def findRanges(table: String, parallel: Int): List[(String, String)] = {
    var ownerName = table.split("\\.")(0)
    var tableName = table.split("\\.")(1)
    val query = getSplitQuery(ownerName, tableName, parallel)
    logger.debug(query)
    val connection = connect
    val rs = connection.createStatement.executeQuery(query)
    var ranges: List[(String, String)] = List()
    while(rs.next) {
      ranges ::= (rs.getString(1), rs.getString(2))
    }
    ranges
  }

  def getMetadata(table: String, parallel: Int) : OracleRowidSourceMetadata = {
    var metadata = new OracleRowidSourceMetadata
    metadata.ranges = findRanges(table, parallel)
    metadata.table = table
    metadata.columns = getColumns(table)
    metadata
  }

}