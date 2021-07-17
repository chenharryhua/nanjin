package com.github.chenharryhua.nanjin.http.longpoll

import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import fs2.concurrent.Channel as Bus
import org.cometd.bayeux.client.ClientSessionChannel
import org.cometd.bayeux.{Channel, Message}
import org.cometd.client.BayeuxClient

sealed trait BayeuxEvent
object BayeuxEvent {
  final case class Subscribed() extends BayeuxEvent
  final case class Unsubscribed() extends BayeuxEvent
}
class BayeuxStateMachine[F[_]: Async](client: BayeuxClient, bus: Bus[F, BayeuxEvent], dispatcher: Dispatcher[F]) {
  import BayeuxEvent._
  private def subscribeListener =
    client
      .getChannel(Channel.META_SUBSCRIBE)
      .addListener(new ClientSessionChannel.MessageListener {
        override def onMessage(csc: ClientSessionChannel, message: Message): Unit =
          if (!message.isSuccessful) {
            dispatcher.unsafeRunSync(bus.send(Unsubscribed()).void)
          } else {
            dispatcher.unsafeRunSync(bus.send(Subscribed()).void)
          }
      })
}
