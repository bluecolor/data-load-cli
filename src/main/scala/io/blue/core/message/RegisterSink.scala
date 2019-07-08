package io.blue.core.message

import akka.actor.ActorRef

case class RegisterSink(sink: ActorRef)