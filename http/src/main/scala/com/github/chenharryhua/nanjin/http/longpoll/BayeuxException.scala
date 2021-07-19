package com.github.chenharryhua.nanjin.http.longpoll

import org.cometd.bayeux.Message

sealed abstract class BayeuxException(msg: String) extends Exception(msg)
object BayeuxException {
  case class HandshakeException(message: Message) extends BayeuxException("handshake failed")

}
