package io.blue.core.producer

import akka.actor.{ActorRef, Actor, Props, ActorSystem}

import io.blue.core.message._
import io.blue.Options
import io.blue.connector.JdbcConnector
import io.blue.core.metadata._


class JdbcProducer extends Producer with Actor {

  var connector: JdbcConnector = _
  var table: String = _
  var index: Int = -1
  var ranges: (String, String) = _
  var filter: String = _
  var columns: List[Column] = List()

  def receive = {
    case message: JdbcProducerParams => setParams(message)
    case message: RegisterSink => registerSink(message)
    case message: StartProducer => start
    case _ => logger.warn("huh?")
  }

  def setParams(message: JdbcProducerParams) {
    this.index = message.index
    this.table = message.table
    this.filter= message.filter
    this.connector = message.connector.asInstanceOf[JdbcConnector]
    this.columns = connector.getColumns(this.table)
    this.table = message.table
  }

  def getQuery: String = {
    s"""
      select ${columns.map(_.name).mkString(",")} from ${table}
      where 1=1
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
      nextSink ! Record((1 to columns.length).map(rs.getObject(_)).toList)
      count += 1
    }
    context.parent ! ProducerDone(index)
  }

}