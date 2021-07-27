package com.github.chenharryhua.nanjin.http.client.longpoll

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.syntax.all.*
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

class LongpollTransport[F[_]](url: String, client: Client[F], dispatcher: Dispatcher[F])(implicit F: Async[F])
    extends AbstractHttpClientTransport(url, new util.HashMap[String, AnyRef](), Executors.newScheduledThreadPool(1))
    with Http4sClientDsl[F] {

  private val jackson = new JacksonJSONContextClient

  override def send(listener: TransportListener, messages: util.List[Message.Mutable]): Unit =
    messages.asScala.foreach { msg =>
      val run: F[Unit] = F
        .fromEither(parse(jackson.generate(msg)))
        .flatMap { reqJson =>
          val req = POST(reqJson, Uri.unsafeFromString(url + msg.getChannel))
          client.expect[Json](req)
        }
        .map(respJson => jackson.parse(respJson.noSpaces).toList)
        .attempt
        .map {
          case Left(ex)  => listener.onFailure(ex, List(msg).asJava)
          case Right(ls) => listener.onMessages(ls.asJava)
        }
      dispatcher.unsafeRunSync(run)
    }
}
