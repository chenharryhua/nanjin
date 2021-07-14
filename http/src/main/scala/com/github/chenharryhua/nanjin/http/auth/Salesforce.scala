package com.github.chenharryhua.nanjin.http.auth

import cats.effect.Async
import cats.effect.kernel.Resource
import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Stream
import io.circe.generic.JsonCodec
import org.http4s.*
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.io.POST

import scala.concurrent.duration.*

@JsonCodec
final case class SalesforceIotTokenResponse(
  access_token: String,
  instance_url: String,
  id: String,
  token_type: String,
  issued_at: String,
  signature: String)

@JsonCodec
final case class MarketingCloudTokenResponse(
  access_token: String,
  token_type: String,
  expires_in: Long,
  scope: String,
  soap_instance_url: String,
  rest_instance_url: String
)

sealed abstract class SalesforceToken(name: String)

object SalesforceToken {
  final case class MarketingCloud[F[_]](
    client_id: String,
    client_secret: String,
    authUri: Uri
  ) extends Http4sClientDsl[F] {

    val loginRequest: Request[F] = POST(
      UrlForm(
        "grant_type" -> "client_credentials",
        "client_id" -> client_id,
        "client_secret" -> client_secret
      ),
      authUri
    )
  }

  final case class Iot[F[_]](
    client_id: String,
    client_secret: String,
    username: String,
    password: String,
    authUri: Uri
  ) extends Http4sClientDsl[F] with Login[F] {
    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val iotToken =
        client.expect[SalesforceIotTokenResponse](
          POST(
            UrlForm(
              "grant_type" -> "password",
              "client_id" -> client_id,
              "client_secret" -> client_secret,
              "username" -> username,
              "password" -> password
            ),
            authUri))

      Stream.resource(Supervisor[F].flatMap { supervisor =>
        Resource.eval(for {
          ref <- iotToken.flatMap(F.ref)
          _ <- supervisor.supervise(ref.get.flatMap(t => iotToken.delayBy(2.hours).flatMap(ref.set)).foreverM[Unit])
        } yield Client[F] { req =>
          val decorated: F[Request[F]] = ref.get.map { t =>
            req
              .withUri(Uri.unsafeFromString(t.instance_url))
              .putHeaders(
                Headers("Authorization" -> s"${t.token_type} ${t.access_token}", "Content-Type" -> "application/json"))
          }
          Resource.eval(decorated).flatMap(client.run)
        })
      })
    }
  }
}
