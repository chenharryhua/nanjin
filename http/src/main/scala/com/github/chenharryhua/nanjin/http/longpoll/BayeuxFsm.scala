package com.github.chenharryhua.nanjin.http.longpoll

import cats.effect.Async
import org.cometd.bayeux.client.ClientSession.MessageListener
import org.cometd.bayeux.client.ClientSessionChannel
import org.cometd.bayeux.{Channel, Message}
import org.cometd.client.BayeuxClient

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

  def handshake: F[Message] = F.async_ { (callback: Either[Throwable, Message] => Unit) =>
    client.handshake((message: Message) =>
      if (message.isSuccessful) callback(Right(message))
      else callback(Left(BayeuxException.HandshakeException(message))))
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
