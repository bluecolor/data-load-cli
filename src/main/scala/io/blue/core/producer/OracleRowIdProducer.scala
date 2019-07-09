package io.blue.core.producer

import akka.actor.{ActorRef, Actor, Props, ActorSystem}

import io.blue.core.message._
import io.blue.Options
import io.blue.connector.OracleRowidConnector
import io.blue.core.metadata._


class OracleRowidProducer extends Producer with Actor {

  var connector: OracleRowidConnector = _
  var table: String = _
  var index: Int = -1
  var ranges: (String, String) = _
  var filter: String = _
  var columns: List[Column] = List()

  def receive = {
    case message: OracleRowidProducerParams => setParams(message)
    case message: RegisterSink => registerSink(message)
    case message: StartProducer => start
    case _ => logger.warn("huh?")
  }

  def setParams(message: OracleRowidProducerParams) {
    this.index = message.index
    this.connector = message.connector.asInstanceOf[OracleRowidConnector]
    this.table = message.table
    this.ranges= message.ranges
    this.columns = message.columns
  }

  def getQuery: String = {
    s"""
      select ${columns.map(_.name).mkString(",")} from ${table}
      where rowid between '${ranges._1}' and '${ranges._2}'
      ${if (filter == null) "" else "and " + filter }
    """
  }

  def start {
    run(getQuery)
  }

  def run(query: String) {
    val connection = connector.connect
    logger.trace(query)
    val rs = connection.createStatement.executeQuery(query)
    var count = 0
    while (rs.next) {
      nextSink ! Record((1 to rs.getMetaData.getColumnCount).map(rs.getObject(_)).toList)
      count += 1
    }
    context.parent ! ProducerDone(index)
  }

}