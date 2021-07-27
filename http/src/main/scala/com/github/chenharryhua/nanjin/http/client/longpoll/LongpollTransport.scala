package com.github.chenharryhua.nanjin.http.client.longpoll

import cats.effect.Async
import cats.effect.std.Dispatcher
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.parser.parse
import org.cometd.bayeux.Message
import org.cometd.client.http.common.AbstractHttpClientTransport
import org.cometd.client.transport.TransportListener
import org.cometd.common.JacksonJSONContextClient
import org.http4s.Method.POST
import org.http4s.Uri
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl

import java.util
import java.util.concurrent.Executors
import scala.collection.JavaConverters.*

class LongpollTransport[F[_]: Async](url: String, client: Client[F], dispatcher: Dispatcher[F])
    extends AbstractHttpClientTransport(url, new util.HashMap[String, AnyRef](), Executors.newScheduledThreadPool(1))
    with Http4sClientDsl[F] {
  private val jackson = new JacksonJSONContextClient
  override def send(listener: TransportListener, messages: util.List[Message.Mutable]): Unit =
    messages.asScala.foreach { msg =>
      parse(jackson.generate(msg)) match {
        case Left(value) => listener.onFailure(value, List(msg).asJava)
        case Right(value) =>
          val req  = POST(value, Uri.unsafeFromString(url + msg.getChannel))
          val resp = dispatcher.unsafeRunSync(client.expect[Json](req)).noSpaces
          listener.onMessages(jackson.parse(resp).toList.asJava)
      }
    }
}
