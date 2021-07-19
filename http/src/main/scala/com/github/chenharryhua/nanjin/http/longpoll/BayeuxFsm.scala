package com.github.chenharryhua.nanjin.http.longpoll

import cats.effect.Async
import cats.effect.std.Dispatcher
import fs2.concurrent.Channel
import org.cometd.bayeux.Message
import org.cometd.bayeux.client.ClientSession
import org.cometd.client.BayeuxClient
import cats.effect.syntax.all._
import cats.syntax.all._

class BayeuxFsm[F[_]](client: BayeuxClient, bus: Channel[F, Message])(implicit F: Async[F]) {
  def handshak: F[Message] =
    F.async_ { (callback: Either[Throwable, Message] => Unit) =>
      client.handshake((message: Message) =>
        if (message.isSuccessful) callback(Right(message))
        else callback(Left(BayeuxException.HandshakeException(message))))
    }
}
