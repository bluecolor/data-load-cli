package io.blue

import io.blue.core.producer._
import io.blue.core.sink._
import io.blue.core._
import io.blue.core.message._
import io.blue.core.metadata._
import io.blue.config.Config
import io.blue.connector._
import io.blue.core.producer._

import me.tongfei.progressbar._
import com.typesafe.scalalogging._
import akka.actor.{Actor, ActorSystem, Props, ActorRef}

class Supervisor extends Actor with LazyLogging {

  var options: Options = _
  var sourceMetadata: Metadata = _
  var targetMetadata: Metadata = _
  var sourceConnector: Connector = _
  var targetConnector: Connector = _
  var producers: Map[Int, (ActorRef, Status.Value)] = Map()
  var sinks: Map[Int, (ActorRef, Status.Value)] = Map()

  var progressBar: ProgressBar = _

  def receive = {
    case options: Options => init(options)
    case ProducerDone(index: Int) => onProducerDone(index)
    case SinkProgress(progress: Long) => onSinkProgress(progress)
    case SinkDone(index: Int) => onSinkDone(index)
    case message: CheckProgress => onCheckProgress
    case start: Start => run
    case _ => logger.error("Unknown message!")
  }

  def onSinkProgress(progress: Long) {
    progressBar.stepBy(progress)
  }

  def initProgressBar {
    progressBar = new ProgressBar("Lauda", sourceConnector.count(options.cli.sourceTable, options.cli.filter))
    progressBar.start
  }

  def getMetadata(table: String, connector: Connector): Metadata = {
    connector match {
      case c: OracleRowidConnector =>
        c.getMetadata(table, options.cli.parallel)
      case c: JdbcConnector =>
        c.getMetadata(table)
      case _ => new Metadata
    }
  }

  def init(options: Options) {
    logger.debug("Initializing options...")
    this.options = options
    sourceConnector = options.config.getSourceConnector(options.cli.source)
    targetConnector = options.config.getTargetConnector(options.cli.target)
    sourceMetadata = getMetadata(options.cli.sourceTable, sourceConnector)
    targetMetadata = getMetadata(options.cli.targetTable, targetConnector)
    if (sourceConnector.isInstanceOf[OracleRowidConnector]) {
      options.sourceParallel = sourceMetadata.asInstanceOf[OracleRowidSourceMetadata].ranges.length
    }
    logger.debug("Options initialized")
  }

  def broadcastProducersDone {
    logger.debug("Broadcasting done message to all sinks...")
    sinks.values.map(_._1).foreach(_ ! ProducersDone())
  }

  def onProducerDone(index: Int) {
    logger.debug(s"Producer ${index} is done")
    val (producer, _) =  producers(index)
    producers += (index -> (producer, Status.Done))
    if (isProducersDone) {
      broadcastProducersDone
    }
    self ! CheckProgress()
  }

  def onSinkDone(index: Int) {
    logger.debug(s"Sink ${index} is done")
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
      progressBar.stop
      context.system.terminate
    }
  }

  def initProducers {
    logger.debug(s"Initializing producers")
    // can not parallize all producers
    sourceConnector match {
      case c: OracleRowidConnector =>
        for (i <- 1 to options.sourceParallel) {
          val producer = context.actorOf(Props[OracleRowidProducer], name = s"OracleRowidProducer_${i}")
          producers += (i -> (producer, Status.Ready))
          val rowidRange = sourceMetadata.asInstanceOf[OracleRowidSourceMetadata].ranges(i-1)
          val params = OracleRowidProducerParams(i, options.cli.sourceTable, rowidRange, sourceMetadata.columns, c)
          producer ! params
        }
      case c: JdbcConnector =>
        options.sourceParallel = 1
        val producer = context.actorOf(Props[JdbcProducer], name = s"JdbcProducer_1")
        producers += (1 -> (producer, Status.Ready))
        val params = JdbcProducerParams(1, options.cli.sourceTable, options.cli.filter, c)
        producer ! params
    }
  }

  def initSinks {
    logger.debug(s"Initializing sinks")

    targetConnector match {
      case c: JdbcConnector =>
        if(options.cli.truncate) {
          c.truncate(options.cli.targetTable)
        }
      case _ =>
    }

    for (i <- 1 to options.targetParallel) {
      targetConnector match {
        case c: FileConnector =>
          val sink = context.actorOf(Props[FileSink], name = s"FileSink_${i}")
          sinks += (i -> (sink, Status.Ready))
          val params = FileSinkParams(i, c)
          sink ! params
        case c: JdbcConnector =>
          val sink = context.actorOf(Props[JdbcSink], name = s"JdbcSink_${i}")
          sinks += (i -> (sink, Status.Ready))
          val params = JdbcSinkParams(i, targetMetadata, c)
          sink ! params
      }
    }
  }

  def registerSinks {
    producers.values.map(_._1).foreach { producer =>
      sinks.values.map(_._1).foreach { sink =>
        producer ! RegisterSink(sink)
      }
    }
  }

  def startProducers {
    for ((producer, i) <- producers.values.map(_._1).zipWithIndex) {
      producer ! StartProducer()
      producers += (i+1 -> (producer, Status.Running))
      sinks += (i+1 -> (sinks(i+1)._1, Status.Running))
    }
  }

  def run {
    initProducers
    initSinks
    registerSinks
    initProgressBar
    startProducers
  }

}