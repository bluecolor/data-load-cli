package io.blue.core.sink

import java.sql.Types._
import java.sql._
import java.io._
import akka.actor.{ActorRef, Actor, Props, ActorSystem}

import io.blue.core.metadata._
import io.blue.connector.JdbcConnector
import io.blue.core.message._


class JdbcSink extends Actor {

  var index: Int = _
  var connector: JdbcConnector = _
  var metadata: Metadata = _
  var connection: java.sql.Connection = _
  var statement: PreparedStatement = _
  var batchIndex: Long = 0

  def receive = {
    case message: ProducersDone => onProducersDone
    case params: JdbcSinkParams => setParams(params)
    case Record(data: List[Object]) => write(data)
    case _  => println("JdbcSink: huh?")
  }

  def setParams(params: JdbcSinkParams) {
    this.index = params.index
    this.metadata = params.metadata
    this.connector = params.connector
    this.connection = this.connector.connect
    val columns = metadata.columns.map(_.name).mkString(",")
    val binds = metadata.columns.map(_ => "?").mkString(",")
    val query = s"""
      insert into ${metadata.table} (${columns}) values (${binds})
    """
    this.statement = this.connection.prepareStatement(query)
  }

  def onProducersDone {
    if (batchIndex > 0) {
      context.parent ! SinkProgress(batchIndex)
      statement.executeBatch
    }
    statement.close
    connection.close
    context.parent ! SinkDone(index)
  }

  def write(data: List[Object]) {
    for ((value, i) <- data.zipWithIndex) {
      this.statement.setObject(i+1, value, metadata.columns(i).columnType)
    }
    statement.addBatch
    batchIndex += 1
    if (batchIndex == this.connector.batchSize) {
      context.parent ! SinkProgress(batchIndex)
      statement.executeBatch
      batchIndex = 0
    }
  }
}
