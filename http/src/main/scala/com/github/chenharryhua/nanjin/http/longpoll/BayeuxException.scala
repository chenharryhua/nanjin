package com.github.chenharryhua.nanjin.http.longpoll

import org.cometd.bayeux.Message

sealed abstract class BayeuxException(message: Message, msg: String) extends Exception(msg)
object BayeuxException {
  final case class HandshakeException(message: Message) extends BayeuxException(message, "handshake failed")
  final case class SubscribeException(message: Message) extends BayeuxException(message, "subscribe failed")
  final case class ListenerException(message: Message) extends BayeuxException(message, "listener failed")

}
