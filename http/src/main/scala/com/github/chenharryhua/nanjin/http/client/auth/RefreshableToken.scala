package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.UUIDGen
import cats.effect.syntax.all.*
import cats.syntax.all.*
import io.circe.generic.auto.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{`Idempotency-Key`, Authorization}
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Credentials, Request, Uri, UrlForm}
import org.typelevel.ci.CIString

import scala.concurrent.duration.DurationLong

final class RefreshableToken[F[_]] private (
  auth_endpoint: Uri,
  client_id: String,
  client_secret: String,
  authClient: Resource[F, Client[F]])
    extends Http4sClientDsl[F] with Login[F, RefreshableToken[F]] {

  private case class Token(
    token_type: String,
    access_token: String,
    expires_in: Long, // in seconds
    refresh_token: String)

  override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
    authClient.flatMap { authenticationClient =>
      val auth_uri: Uri = auth_endpoint.withPath(path"oauth/token")

      val get_token: F[Token] =
        UUIDGen[F].randomUUID.flatMap { uuid =>
          authenticationClient.expect[Token](
            POST(
              UrlForm(
                "grant_type" -> "client_credentials",
                "client_id" -> client_id,
                "client_secret" -> client_secret),
              auth_uri).putHeaders(`Idempotency-Key`(show"$uuid")))
        }

      def refresh_token(pre: Token): F[Token] =
        UUIDGen[F].randomUUID.flatMap { uuid =>
          authClient.use(
            _.expect[Token](
              POST(
                UrlForm(
                  "grant_type" -> "refresh_token",
                  "refresh_token" -> pre.refresh_token,
                  "client_id" -> client_id,
                  "client_secret" -> client_secret),
                auth_uri).putHeaders(`Idempotency-Key`(show"$uuid"))))
        }

      def update_token(ref: Ref[F, Token]): F[Unit] =
        for {
          oldToken <- ref.get
          newToken <- refresh_token(oldToken).delayBy(oldToken.expires_in.seconds)
          _ <- ref.set(newToken)
        } yield ()

      def with_token(token: Token, req: Request[F]): Request[F] =
        req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))

      loginInternal(client, get_token, update_token, with_token)
    }
}

object RefreshableToken {
  def apply[F[_]](authClient: Resource[F, Client[F]])(
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String): RefreshableToken[F] =
    new RefreshableToken[F](
      auth_endpoint = auth_endpoint,
      client_id = client_id,
      client_secret = client_secret,
      authClient = authClient)
}
