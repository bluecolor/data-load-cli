package io.blue.core.sink

import java.io._
import akka.actor.{ActorRef, Actor, Props, ActorSystem}

import io.blue.core.metadata._
import io.blue.connector.FileConnector
import io.blue.core.message._

class FileSink extends Actor {

  var index: Int = _
  var connector: FileConnector = _
  var writer: BufferedWriter = _
  var batchIndex: Long = 0

  def receive = {
    case message: ProducersDone => onProducersDone
    case connector: FileConnector => setConnector(connector)
    case params: FileSinkParams => setParams(params)
    case Record(data: List[Object]) => write(data)
    case _  => println("FileSink: huh?")
  }

  def setConnector(connector: FileConnector) {
    this.connector = connector
  }

  def setParams(params: FileSinkParams) {
    this.connector = params.connector
    this.index = params.index
    val file = s"${connector.path}/${index}.txt"
	  this.writer = new BufferedWriter(new FileWriter(new File(file)), this.connector.batchSize)
  }

  def onProducersDone {
    this.writer.flush
    this.writer.close
    context.parent ! SinkDone(index)
  }

  def write(data: List[Object]) {
    val record = s"${data.map(_.toString).mkString(connector.fieldDelimiter)}${connector.recordSeperator}"
    this.writer.write(record)
    batchIndex += 1
    if (batchIndex == this.connector.batchSize) {
      context.parent ! SinkProgress(batchIndex)
      batchIndex = 0
    }
  }
}
