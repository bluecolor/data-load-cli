package io.blue.core.sink

import java.io._
import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import com.typesafe.scalalogging._

import io.blue.core.metadata._
import io.blue.connector.FileConnector
import io.blue.core.message._

abstract class Sink extends LazyLogging {

  var index: Int = _
  var batchIndex: Long = 0

}
