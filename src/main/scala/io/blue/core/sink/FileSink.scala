package io.blue.core.sink

import java.io._
import akka.actor.{ActorRef, Actor, Props, ActorSystem}

import io.blue.core.metadata._
import io.blue.connector.FileConnector
import io.blue.core.message._

class FileSink extends Sink with Actor {

  var connector: FileConnector = _
  var writer: BufferedWriter = _

  def receive = {
    case message: ProducersDone => onProducersDone
    case params: FileSinkParams => setParams(params)
    case Record(data: List[Object]) => write(data)
    case _  => println("FileSink: huh?")
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
    context.parent ! SinkProgress(batchIndex)
    context.parent ! SinkDone(index)
  }

  def write(data: List[Object]) {

    try {
      val r = data.map{col: Object =>
        if (col == null) { null } else { col.toString }
      }
      val record = s"${r.mkString(connector.fieldDelimiter)}${connector.recordSeperator}"
      this.writer.write(record)
      batchIndex += 1
      if (batchIndex == this.connector.batchSize) {
        context.parent ! SinkProgress(batchIndex)
        batchIndex = 0
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
  }
}
