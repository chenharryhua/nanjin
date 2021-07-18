package com.github.chenharryhua.nanjin.http.auth

import cats.effect.Async
import cats.effect.kernel.Resource
import cats.effect.syntax.all.*
import fs2.Stream
import io.circe.generic.auto.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{BasicCredentials, Headers, Uri, UrlForm}

import scala.concurrent.duration.{DurationLong, FiniteDuration}

final private case class RefreshableTokenResponse(
  token_type: String,
  access_token: String,
  expires_in: Long,
  refresh_token: String)

final class RefreshableToken[F[_]] private (
  auth_endpoint: Uri,
  client_id: String,
  client_secret: String,
  config: AuthConfig)
    extends Http4sClientDsl[F] with Login[F] {

  val params: AuthParams = config.evalConfig

  override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {

    val authURI: Uri = auth_endpoint.withPath(path"oauth/token")
    val getToken: Stream[F, RefreshableTokenResponse] =
      Stream.eval(
        params
          .authClient(client)
          .expect[RefreshableTokenResponse](
            POST(
              UrlForm("grant_type" -> "client_credentials", "client_id" -> client_id, "client_secret" -> client_secret),
              authURI).putHeaders("Cache-Control" -> "no-cache")))

    getToken.evalMap(F.ref).flatMap { token =>
      val refresh: Stream[F, Unit] =
        Stream
          .eval(token.get)
          .evalMap { t =>
            params
              .authClient(client)
              .expect[RefreshableTokenResponse](
                POST(
                  UrlForm("grant_type" -> "refresh_token", "refresh_token" -> t.refresh_token),
                  authURI,
                  Authorization(BasicCredentials(client_id, client_secret))
                ).putHeaders("Cache-Control" -> "no-cache"))
              .delayBy(params.offset(t.expires_in.seconds))
          }
          .evalMap(token.set)
          .repeat
      Stream[F, Client[F]](Client[F] { req =>
        Resource
          .eval(token.get)
          .flatMap(t => client.run(req.putHeaders(Headers("Authorization" -> s"${t.token_type} ${t.access_token}"))))
      }).concurrently(refresh)
    }
  }

  private def updateConfig(f: AuthConfig => AuthConfig): RefreshableToken[F] =
    new RefreshableToken[F](auth_endpoint, client_id, client_secret, f(config))
  def withMaxRetries(times: Int): RefreshableToken[F]       = updateConfig(_.withMaxRetries(times))
  def withMaxWait(dur: FiniteDuration): RefreshableToken[F] = updateConfig(_.withMaxWait(dur))
}
object RefreshableToken {
  def apply[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String): RefreshableToken[F] =
    new RefreshableToken[F](auth_endpoint, client_id, client_secret, AuthConfig(0.seconds))
}
