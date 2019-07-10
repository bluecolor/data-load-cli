package io.blue


import java.io.File
import akka.actor.{Actor, ActorSystem, Props, ActorRef}
import com.typesafe.scalalogging._

import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import io.blue.config.Config
import io.blue.cli.Cli
import io.blue.core.message._

object Application extends App with LazyLogging {

  logger.debug("Starting application")

  val cli = Cli.parse(args)
  if (cli.helpRequested) {
    cli.printHelp
    System.exit(0)
  }

  var configFile: File = null

  if (cli.config != null) {
    configFile = new File(cli.config)
    if (!configFile.exists) {
      logger.error("Config file does not exist!")
      System.exit(1)
    }
  } else {
    Config.searchConfigFile match {
      case Some(f) => configFile = f
      case None =>
        logger.error("Cannot find a valid configuration file")
        System.exit(1)
    }
  }

  val options = new Options(cli, Config.parse(configFile))

  var system: ActorSystem = ActorSystem("LaudaSystem")
  val supervisor = system.actorOf(Props[Supervisor], name = "supervisor")

  implicit val timeout = Timeout(500 seconds)
  supervisor ! options

  supervisor ! Start()
  Await.ready(system.whenTerminated, 1.days)
}

