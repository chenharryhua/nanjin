package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.Reader
import cats.effect.kernel.{Async, Ref}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.Endo
import com.github.chenharryhua.nanjin.common.UpdateConfig
import io.circe.generic.auto.*
import org.http4s.{Credentials, Request, Uri, UrlForm}
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci.CIString

import scala.concurrent.duration.DurationLong
import fs2.Stream

final class RefreshableToken[F[_]] private (
  auth_endpoint: Uri,
  client_id: String,
  client_secret: String,
  cfg: AuthConfig,
  middleware: Reader[Client[F], Client[F]])
    extends Http4sClientDsl[F] with Login[F, RefreshableToken[F]]
    with UpdateConfig[AuthConfig, RefreshableToken[F]] {

  private case class Token(
    token_type: String,
    access_token: String,
    expires_in: Long, // in seconds
    refresh_token: String)

  private val params: AuthParams = cfg.evalConfig

  override def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = {

    val authURI: Uri = auth_endpoint.withPath(path"oauth/token")
    val getToken: F[Token] =
      params
        .authClient(client)
        .expect[Token](
          POST(
            UrlForm(
              "grant_type" -> "client_credentials",
              "client_id" -> client_id,
              "client_secret" -> client_secret),
            authURI))

    def viaRefreshToken(pre: Token): F[Token] =
      params
        .authClient(client)
        .expect[Token](
          POST(
            UrlForm(
              "grant_type" -> "refresh_token",
              "refresh_token" -> pre.refresh_token,
              "client_id" -> client_id,
              "client_secret" -> client_secret),
            authURI))

    def updateToken(ref: Ref[F, Token]): F[Unit] =
      for {
        oldToken <- ref.get
        newToken <- viaRefreshToken(oldToken).delayBy(params.dormant(oldToken.expires_in.seconds))
        _ <- ref.set(newToken)
      } yield ()

    def withToken(token: Token, req: Request[F]): Request[F] =
      req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))

    loginInternal(client, getToken, updateToken, withToken).map(middleware.run)

  }

  override def updateConfig(f: Endo[AuthConfig]): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      cfg = f(cfg),
      middleware = middleware)

  override def withMiddleware(f: Client[F] => Client[F]): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      cfg = cfg,
      middleware = middleware.compose(f))
}

object RefreshableToken {
  def apply[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      cfg = AuthConfig(),
      middleware = Reader(identity))
}
