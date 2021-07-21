package com.github.chenharryhua.nanjin.http.longpoll

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import fs2.Stream
import org.cometd.bayeux.client.ClientSessionChannel
import org.cometd.bayeux.{Channel, Message}
import org.cometd.client.BayeuxClient

sealed trait BayeuxState[F[_]] {
  def client: BayeuxClient
}
object BayeuxState {
  final case class Initialized[F[_]](client: BayeuxClient) extends BayeuxState[F] {
    def handshake(implicit F: Async[F]): F[Handshaked[F]] =
      F.async_ { (callback: Either[Throwable, Handshaked[F]] => Unit) =>
        client.handshake((message: Message) =>
          if (message.isSuccessful) callback(Right(Handshaked[F](client)))
          else callback(Left(BayeuxException.HandshakeException(message))))
      }
  }
  final case class Handshaked[F[_]](client: BayeuxClient) extends BayeuxState[F] {
    def subscribe(channel: String)(implicit F: Async[F]): Stream[F, Message] =
      for {
        dispatcher <- Stream.resource(Dispatcher[F])
        msg <- Stream.eval(fs2.concurrent.Channel.unbounded[F, Message]).flatMap { bus =>
          client
            .getChannel(channel)
            .subscribe((channel: ClientSessionChannel, message: Message) =>
              dispatcher.unsafeRunSync(bus.send(message).void))
          bus.stream
        }
      } yield msg
  }

  final case class Connected[F[_]](client: BayeuxClient) extends BayeuxState[F]
  final case class Subscribed[F[_]](client: BayeuxClient) extends BayeuxState[F]
  final case class DataExchanging[F[_]](client: BayeuxClient) extends BayeuxState[F]
  final case class Unsubscribed[F[_]](client: BayeuxClient) extends BayeuxState[F]
  final case class Disconnected[F[_]](client: BayeuxClient) extends BayeuxState[F]
}

class BayeuxFsm[F[_]](client: BayeuxClient)(implicit F: Async[F]) {

  private def listenTo(channel: String): F[Message] =
    F.async_ { (callback: Either[Throwable, Message] => Unit) =>
      client
        .getChannel(channel)
        .addListener(new ClientSessionChannel.MessageListener {
          override def onMessage(channel: ClientSessionChannel, message: Message): Unit =
            if (message.isSuccessful) callback(Right(message))
            else callback(Left(BayeuxException.ListenerException(message)))
        })
    }

  def subscribe(channel: String): F[Message] = F.async_ { (callback: Either[Throwable, Message] => Unit) =>
    client
      .getChannel(channel)
      .subscribe((channel: ClientSessionChannel, message: Message) =>
        if (message.isSuccessful) callback(Right(message))
        else callback(Left(BayeuxException.SubscribeException(message))))
    ()
  }
  def subscribe: F[Message] = subscribe(Channel.META_SUBSCRIBE)

  def unsubscribe: F[Message] = listenTo(Channel.META_UNSUBSCRIBE)
  def connect: F[Message]     = listenTo(Channel.META_CONNECT)
  def disconnect: F[Message]  = listenTo(Channel.META_DISCONNECT)

}
