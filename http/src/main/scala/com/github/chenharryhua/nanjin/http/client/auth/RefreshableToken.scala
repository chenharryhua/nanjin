package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.Kleisli
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.effect.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import fs2.Stream
import io.circe.generic.auto.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{BasicCredentials, Credentials, Uri, UrlForm}
import org.typelevel.ci.CIString

import scala.concurrent.duration.DurationLong

final private case class RefreshableTokenResponse(
  token_type: String,
  access_token: String,
  expires_in: Long, // in seconds
  refresh_token: String)

private object RefreshableTokenResponse {
  implicit val expirableRefreshableTokenResponse: IsExpirableToken[RefreshableTokenResponse] =
    (a: RefreshableTokenResponse) => a.expires_in.seconds
}

final class RefreshableToken[F[_]] private (
  auth_endpoint: Uri,
  client_id: String,
  client_secret: String,
  config: AuthConfig,
  middleware: Kleisli[F, Client[F], Client[F]])
    extends Http4sClientDsl[F] with Login[F, RefreshableToken[F]] with UpdateConfig[AuthConfig, RefreshableToken[F]] {

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
              .delayBy(params.calcDelay(t))
          }
          .evalMap(token.set)
          .repeat
      Stream
        .eval(middleware(client))
        .map { c =>
          Client[F] { req =>
            Resource
              .eval(token.get)
              .flatMap(t =>
                c.run(req.putHeaders(Authorization(Credentials.Token(CIString(t.token_type), t.access_token)))))
          }
        }
        .concurrently(refresh)
    }
  }

  override def updateConfig(f: AuthConfig => AuthConfig): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      config = f(config),
      middleware = middleware)

  override def withMiddlewareM(f: Client[F] => F[Client[F]])(implicit F: Monad[F]): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      config = config,
      middleware = compose(f, middleware))
}

object RefreshableToken {
  def apply[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String)(implicit
    F: Applicative[F]): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      config = AuthConfig(1.day),
      middleware = Kleisli(F.pure))
}
