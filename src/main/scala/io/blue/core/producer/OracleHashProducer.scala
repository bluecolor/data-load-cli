package io.blue.core.producer

import akka.actor.{ActorRef, Actor, Props, ActorSystem}

import io.blue.core.message._
import io.blue.Options
import io.blue.connector._
import io.blue.core.metadata._


class OracleHashProducer extends Producer with Actor {

  var connector: OracleHashConnector = _
  var table: String = _
  var index: Int = -1
  var hashId: Int = _
  var parallel: Int = _
  var filter: String = _
  var columns: List[Column] = List()

  def receive = {
    case message: OracleHashProducerParams => setParams(message)
    case message: RegisterSink => registerSink(message)
    case message: StartProducer => start
    case _ => logger.warn("huh?")
  }

  def setParams(message: OracleHashProducerParams) {
    this.index = message.index
    this.connector = message.connector.asInstanceOf[OracleHashConnector]
    this.table = message.table
    this.hashId= message.hashId
    this.parallel = message.parallel
    this.columns = message.columns
  }

  def getQuery: String = {
    s"""
      select ${columns.map(_.name).mkString(",")} from ${table}
      where ora_hash(rowid, ${parallel}) = ${hashId}
      ${if (filter == null) "" else "and " + filter }
    """
  }

  def start {
    run(getQuery)
  }

  def run(query: String) {
    val connection = connector.connect
    logger.debug(query)
    try{
      val rs = connection.createStatement.executeQuery(query)
      var count = 0
      while (rs.next) {
        nextSink ! Record((1 to rs.getMetaData.getColumnCount).map(rs.getObject(_)).toList)
        count += 1
      }
    } catch {
        case e: Exception =>
          e.printStackTrace
          System.exit(1)
      }
    connection.close
    context.parent ! ProducerDone(index)
  }

}