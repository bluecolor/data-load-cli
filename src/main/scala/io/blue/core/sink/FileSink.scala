package io.blue.core.sink

import java.io._
import akka.actor.{ActorRef, Actor, Props, ActorSystem}

import io.blue.core.metadata._
import io.blue.connector.FileConnector
import io.blue.core.message._

class FileSink extends Actor {

  var index: Int = _
  var connector: FileConnector = _
  var metadata: Metadata = _
  var writer: BufferedWriter = _

  def receive = {
    case message: ProducerDone => onProducerDone
    case connector: FileConnector => setConnector(connector)
    case params: FileSinkParams => setParams(params)
    case metadata: Metadata => registerMetadata(metadata)
    case Record(data: List[Object]) => write(data)
    case _  => println("FileSink: huh?")
  }

  def setConnector(connector: FileConnector) {
    this.connector = connector
  }

  def setParams(params: FileSinkParams) {
    this.connector = params.connector
    this.index = params.index
    var fw = new FileWriter(s"${connector.path}/${index}.txt")
		this.writer = new BufferedWriter(fw)
  }

  def registerMetadata(metadata: Metadata) {
    this.metadata = metadata
  }

  def onProducerDone {
    this.writer.flush
    this.writer.close
    context.parent ! SinkDone(index)
  }

  def write(data: List[Object]) {
    val record = s"${data.map(_.toString).mkString(connector.fieldDelimiter)}${connector.recordSeperator}"
    this.writer.write(record)
  }
}
