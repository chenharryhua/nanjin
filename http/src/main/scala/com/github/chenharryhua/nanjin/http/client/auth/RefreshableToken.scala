package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.common.UpdateConfig
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

final class RefreshableToken[F[_]] private (
  auth_endpoint: Uri,
  client_id: String,
  client_secret: String,
  config: AuthConfig,
  middleware: Kleisli[F, Client[F], Client[F]])
    extends Http4sClientDsl[F] with Login[F, RefreshableToken[F]] with UpdateConfig[AuthConfig, RefreshableToken[F]] {
  private case class Token(
    token_type: String,
    access_token: String,
    expires_in: Long, // in seconds
    refresh_token: String)
  implicit private val expirable: IsExpirableToken[Token] = (a: Token) => a.expires_in.seconds

  val params: AuthParams = config.evalConfig

  override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {

    val authURI: Uri = auth_endpoint.withPath(path"oauth/token")
    val getToken: F[Token] =
      params
        .authClient(client)
        .expect[Token](
          POST(
            UrlForm("grant_type" -> "client_credentials", "client_id" -> client_id, "client_secret" -> client_secret),
            authURI).putHeaders("Cache-Control" -> "no-cache"))

    def updateToken(ref: Ref[F, Token]): F[Unit] = for {
      old <- ref.get
      newToken <- params
        .authClient(client)
        .expect[Token](
          POST(
            UrlForm("grant_type" -> "refresh_token", "refresh_token" -> old.refresh_token),
            authURI,
            Authorization(BasicCredentials(client_id, client_secret))
          ).putHeaders("Cache-Control" -> "no-cache"))
        .delayBy(params.whenNext(old))
      _ <- ref.set(newToken)
    } yield ()

    for {
      supervisor <- Supervisor[F]
      ref <- Resource.eval(getToken.flatMap(F.ref))
      _ <- Resource.eval(supervisor.supervise(updateToken(ref).foreverM))
      c <- Resource.eval(middleware(client))
    } yield Client[F] { req =>
      for {
        token <- Resource.eval(ref.get)
        out <- c.run(req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token))))
      } yield out
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
