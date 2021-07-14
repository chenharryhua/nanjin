package com.github.chenharryhua.nanjin.http.auth

import cats.effect.Async
import cats.effect.kernel.Resource
import cats.effect.std.{Hotswap, Supervisor}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Stream
import io.circe.generic.JsonCodec
import org.http4s.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.client.middleware.Retry
import org.http4s.implicits.http4sLiteralsSyntax

import java.util.concurrent.TimeUnit
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

sealed abstract class SalesforceToken(val name: String)

object SalesforceToken {

  //https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/authorization-code.htm
  final case class MarketingCloud[F[_]](
    client_id: String,
    client_secret: String,
    auth_endpoint: Uri
  ) extends Http4sClientDsl[F] with Login[F] {

    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: F[MarketingCloudTokenResponse] =
        Retry(authPolicy[F])(client).expect[MarketingCloudTokenResponse](
          POST(
            UrlForm(
              "grant_type" -> "client_credentials",
              "client_id" -> client_id,
              "client_secret" -> client_secret
            ),
            auth_endpoint.withPath(path"/v2/token")
          ))

      Stream.resource(for {
        hotswap <- Hotswap.create[F, Response[F]]
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.flatMap(F.ref))
        _ <- Resource.eval(
          supervisor.supervise(
            ref.get
              .flatMap(t => getToken.delayBy(FiniteDuration(t.expires_in / 2, TimeUnit.SECONDS)).flatMap(ref.set))
              .foreverM[Unit]))
      } yield Client[F] { req =>
        Resource.eval(ref.get.flatMap(t =>
          hotswap.swap(client.run(req.putHeaders(Headers("Authorization" -> s"${t.token_type} ${t.access_token}"))))))
      })
    }
  }

  //https://developer.salesforce.com/docs/atlas.en-us.api_iot.meta/api_iot/qs_auth_access_token.htm
  final case class Iot[F[_]](
    client_id: String,
    client_secret: String,
    username: String,
    password: String,
    auth_endpoint: Uri
  ) extends Http4sClientDsl[F] with Login[F] {
    override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {
      val getToken: F[SalesforceIotTokenResponse] =
        Retry(authPolicy[F])(client).expect[SalesforceIotTokenResponse](
          POST(
            UrlForm(
              "grant_type" -> "password",
              "client_id" -> client_id,
              "client_secret" -> client_secret,
              "username" -> username,
              "password" -> password
            ),
            auth_endpoint.withPath(path"/services/oauth2/token")
          ))

      Stream.resource(for {
        hotswap <- Hotswap.create[F, Response[F]]
        supervisor <- Supervisor[F]
        ref <- Resource.eval(getToken.flatMap(F.ref))
        _ <- Resource.eval(supervisor.supervise(getToken.delayBy(2.hours).flatMap(ref.set).foreverM[Unit]))
      } yield Client[F] { req =>
        Resource.eval(ref.get.flatMap(t =>
          hotswap.swap(client.run(req.putHeaders(Headers("Authorization" -> s"${t.token_type} ${t.access_token}"))))))
      })
    }
  }
}
