package io.blue.core.producer

import com.typesafe.scalalogging._
import akka.actor.{ActorRef, Actor, Props, ActorSystem}

import io.blue.core.message._


abstract class Producer extends LazyLogging {

  var sinks: List[ActorRef] = List()
  var sinkIndex: Int = -1

  def nextSink: ActorRef = {
    sinkIndex += 1
    if (sinkIndex == sinks.length) {
      sinkIndex = 0
    }
    sinks(sinkIndex)
  }

  def registerSink(message: RegisterSink) {
    sinks ::= message.sink
  }

  def start
}