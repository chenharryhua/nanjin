package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.utils
import io.circe.generic.auto.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{`Idempotency-Key`, Authorization}
import org.http4s.{Credentials, Request, Uri, UrlForm}
import org.typelevel.ci.CIString

import scala.concurrent.duration.DurationLong

final class RefreshableToken[F[_]] private (
  credentials: RefreshableToken.Credential,
  authClient: Resource[F, Client[F]])
    extends Http4sClientDsl[F] with Login[F] {

  private case class Token(
    token_type: String,
    access_token: String,
    expires_in: Long, // in seconds
    refresh_token: String)

  override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
    authClient.flatMap { authenticationClient =>
      val get_token: F[Token] =
        utils.randomUUID[F].flatMap { uuid =>
          authenticationClient.expect[Token](
            POST(
              UrlForm(
                "grant_type" -> "client_credentials",
                "client_id" -> credentials.client_id,
                "client_secret" -> credentials.client_secret),
              credentials.auth_endpoint).putHeaders(`Idempotency-Key`(show"$uuid")))
        }

      def refresh_token(pre: Token): F[Token] =
        utils.randomUUID[F].flatMap { uuid =>
          authClient.use(
            _.expect[Token](POST(
              UrlForm(
                "grant_type" -> "refresh_token",
                "refresh_token" -> pre.refresh_token,
                "client_id" -> credentials.client_id,
                "client_secret" -> credentials.client_secret),
              credentials.auth_endpoint
            ).putHeaders(`Idempotency-Key`(show"$uuid"))))
        }

      def update_token(ref: Ref[F, Token]): F[Unit] =
        for {
          oldToken <- ref.get
          newToken <- refresh_token(oldToken).delayBy(oldToken.expires_in.seconds)
          _ <- ref.set(newToken)
        } yield ()

      def with_token(token: Token, req: Request[F]): Request[F] =
        req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))

      login_internal(client, get_token, update_token, with_token)
    }
}

object RefreshableToken {
  final case class Credential(auth_endpoint: Uri, client_id: String, client_secret: String)
  def apply[F[_]](authClient: Resource[F, Client[F]])(
    credential: Credential
  ): RefreshableToken[F] =
    new RefreshableToken[F](credentials = credential, authClient = authClient)
}
