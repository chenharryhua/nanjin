package com.github.chenharryhua.nanjin.http.client.longpoll

import cats.effect.kernel.Async
import io.circe.generic.auto.*
import io.circe.literal.JsonStringContext
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.implicits.http4sLiteralsSyntax

final case class Ext(replay: Boolean, `payload.format`: Boolean)
final case class Advice(timeout: Long, interval: Long, reconnect: String)

final class Handshake[F[_]] extends Http4sClientDsl[F] {
  case class Response(
    ext: Ext,
    minimumVersion: String,
    clientId: String,
    supportedConnectionTypes: List[String],
    channel: String,
    version: String,
    successful: Boolean)

  def execute(client: Client[F])(implicit F: Async[F]): F[List[Response]] =
    client.expect[List[Response]](
      POST(
        json"""{
       "supportedConnectionTypes": ["long-polling"],
       "channel": "/meta/handshake",
       "version": "1.0"}""",
        uri"/meta/handshake"
      ))
}

final class Connect[F[_]](clientId: String) extends Http4sClientDsl[F] {

  case class Response(clientId: String, advice: Advice, channel: String, successful: Boolean)

  def execute(client: Client[F])(implicit F: Async[F]): F[List[Response]] =
    client.expect[List[Response]](
      POST(
        json"""{
       "clientId": $clientId,
       "channel": "/meta/connect",
       "connectionType": "long-polling"}""",
        uri"/meta/connect"
      ))
}
