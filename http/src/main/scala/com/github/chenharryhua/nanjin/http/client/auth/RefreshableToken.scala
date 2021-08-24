package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.Reader
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import io.circe.generic.auto.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Credentials, Uri, UrlForm}
import org.typelevel.ci.CIString

import scala.concurrent.duration.DurationLong

final class RefreshableToken[F[_]] private (
  auth_endpoint: Uri,
  client_id: String,
  client_secret: String,
  cfg: AuthConfig,
  middleware: Reader[Client[F], Resource[F, Client[F]]])
    extends Http4sClientDsl[F] with Login[F, RefreshableToken[F]] with UpdateConfig[AuthConfig, RefreshableToken[F]] {

  @SuppressWarnings(Array("FinalModifierOnCaseClass"))
  private case class Token(
    token_type: String,
    access_token: String,
    expires_in: Long, // in seconds
    refresh_token: String)
  implicit private val expirable: IsExpirableToken[Token] = (a: Token) => a.expires_in.seconds

  val params: AuthParams = cfg.evalConfig

  override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] = {

    val authURI: Uri = auth_endpoint.withPath(path"oauth/token")
    val getToken: F[Token] =
      params
        .authClient(client)
        .expect[Token](
          POST(
            UrlForm("grant_type" -> "client_credentials", "client_id" -> client_id, "client_secret" -> client_secret),
            authURI).putHeaders("Cache-Control" -> "no-cache"))

    def refreshToken(pre: Token): F[Token] =
      params
        .authClient(client)
        .expect[Token](
          POST(
            UrlForm(
              "grant_type" -> "refresh_token",
              "refresh_token" -> pre.refresh_token,
              "client_id" -> client_id,
              "client_secret" -> client_secret),
            authURI
          ).putHeaders("Cache-Control" -> "no-cache"))

    def updateToken(ref: Ref[F, Either[Throwable, Token]]): F[Unit] = for {
      newToken <- ref.get.flatMap {
        case Left(_)      => getToken.delayBy(params.whenNext).attempt
        case Right(value) => refreshToken(value).delayBy(params.whenNext(value)).attempt
      }
      _ <- ref.set(newToken)
    } yield ()

    for {
      supervisor <- Supervisor[F]
      ref <- Resource.eval(getToken.attempt.flatMap(F.ref))
      _ <- Resource.eval(supervisor.supervise(updateToken(ref).foreverM))
      c <- middleware.run(client)
    } yield Client[F] { req =>
      for {
        token <- Resource.eval(ref.get.rethrow)
        out <- c.run(req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token))))
      } yield out
    }
  }

  override def updateConfig(f: AuthConfig => AuthConfig): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      cfg = f(cfg),
      middleware = middleware)

  override def withMiddlewareR(f: Client[F] => Resource[F, Client[F]]): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      cfg = cfg,
      middleware = compose(f, middleware))
}

object RefreshableToken {
  def apply[F[_]](auth_endpoint: Uri, client_id: String, client_secret: String): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      cfg = AuthConfig(3.hours),
      middleware = Reader(Resource.pure))
}
