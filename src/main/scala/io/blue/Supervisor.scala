package io.blue

import io.blue.core.producer._
import io.blue.core.sink._
import io.blue.core._
import io.blue.core.message._
import io.blue.core.metadata._
import io.blue.config.Config
import io.blue.connector._
import io.blue.core.producer._

import akka.actor.{Actor, ActorSystem, Props, ActorRef}

class Supervisor extends Actor {

  var options: Options = _
  var sourceMetadata: Metadata = _
  var sourceConnector: Connector = _
  var targetConnector: Connector = _
  var producers: Map[Int, (ActorRef, Status.Value)] = Map()
  var sinks: Map[Int, (ActorRef, Status.Value)] = Map()

  def receive = {
    case options: Options => init(options)
    case ProducerDone(index: Int) => onProducerDone(index)
    case SinkDone(index: Int) => onSinkDone(index)
    case message: CheckProgress => onCheckProgress
    case start: Start => run
    case _ => println("Supervisor: huh?")
  }

  def init(options: Options) {
    this.options = options
    sourceConnector = options.config.getSourceConnector(options.cli.source)
    targetConnector = options.config.getTargetConnector(options.cli.target)
    sourceMetadata = sourceConnector match {
      case c: OracleRowidConnector =>
        c.getMetadata(options.cli.sourceTable, options.cli.parallel)
      case c: JdbcConnector =>
        c.getMetadata(options.cli.sourceTable)
      case _ => new Metadata()
    }
    if (sourceConnector.isInstanceOf[OracleRowidConnector]) {
      options.parallel = sourceMetadata.asInstanceOf[OracleRowidSourceMetadata].ranges.length
    }
  }

  def onProducerDone(index: Int) {
    val (producer, _) =  producers(index)
    producers += (index -> (producer, Status.Done))
    self ! CheckProgress()
  }

  def onSinkDone(index: Int) {
    val (sink, _) =  sinks(index)
    sinks += (index -> (sink, Status.Done))
    self ! CheckProgress()
  }

  def isProducersDone: Boolean = {
    val s = Array(Status.Ready, Status.Running)
    producers.values.map(_._2).filter(s contains _).size == 0
  }

  def isSinksDone: Boolean = {
    val s = Array(Status.Ready, Status.Running)
    sinks.values.map(_._2).filter(s contains _).size == 0
  }

  def onCheckProgress = {
    if (isSinksDone) {
      context.system.terminate
    }
  }

  def run {
    for (i <- 1 to options.parallel) {
      var producer: ActorRef = null
      var sink: ActorRef = null

      sourceConnector match {
        case c: OracleRowidConnector =>
          producer = context.actorOf(Props[OracleRowidProducer], name = s"OracleRowidProducer_${i}")
          producers += (i -> (producer, Status.Ready))
          val rowidRange = sourceMetadata.asInstanceOf[OracleRowidSourceMetadata].ranges(i-1)
          val params = OracleRowidProducerParams(i, options.cli.sourceTable, rowidRange, sourceMetadata.columns, c)
          producer ! params
      }

      targetConnector match {
        case c: FileConnector =>
          sink = context.actorOf(Props[FileSink], name = s"FileSink_${i}")
          sinks += (i -> (sink, Status.Ready))
          val params = FileSinkParams(i, c)
          sink ! params
      }

      producer ! RegisterSink(sink)
      producer ! StartProducer()
      producers += (i -> (producer, Status.Running))
      sinks += (i -> (sink, Status.Running))
    }
  }
}