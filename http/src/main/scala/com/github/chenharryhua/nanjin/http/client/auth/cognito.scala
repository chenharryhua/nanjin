package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.NonEmptyList
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
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{BasicCredentials, Credentials, Request, Uri, UrlForm}
import org.typelevel.ci.CIString

import scala.concurrent.duration.*

/** https://docs.aws.amazon.com/cognito/latest/developerguide/token-endpoint.html
  */

object cognito {

  final class AuthorizationCode[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    code: String,
    redirect_uri: String,
    code_verifier: String,
    authClient: Resource[F, Client[F]])
      extends Http4sClientDsl[F] with Login[F, AuthorizationCode[F]] {

    private case class Token(
      access_token: String,
      refresh_token: String,
      id_token: String,
      token_type: String,
      expires_in: Int // in second
    )

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val authURI = auth_endpoint.withPath(path"/oauth2/token")

        val get_token: F[Token] =
          utils.randomUUID[F].flatMap { uuid =>
            authenticationClient.expect[Token](POST(
              UrlForm(
                "grant_type" -> "authorization_code",
                "client_id" -> client_id,
                "code" -> code,
                "redirect_uri" -> redirect_uri,
                "code_verifier" -> code_verifier
              ),
              authURI,
              Authorization(BasicCredentials(client_id, client_secret))
            ).putHeaders(`Idempotency-Key`(show"$uuid")))
          }

        def refresh_token(pre: Token): F[Token] =
          utils.randomUUID[F].flatMap { uuid =>
            authClient.use(
              _.expect[Token](POST(
                UrlForm(
                  "grant_type" -> "refresh_token",
                  "client_id" -> client_id,
                  "refresh_token" -> pre.refresh_token),
                authURI,
                Authorization(BasicCredentials(client_id, client_secret))
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

        loginInternal(client, get_token, update_token, with_token)
      }
  }

  object AuthorizationCode {
    def apply[F[_]](authClient: Resource[F, Client[F]])(
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      code: String,
      redirect_uri: String,
      code_verifier: String): AuthorizationCode[F] =
      new AuthorizationCode[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        code = code,
        redirect_uri = redirect_uri,
        code_verifier = code_verifier,
        authClient = authClient
      )
  }

  final class ClientCredentials[F[_]] private (
    auth_endpoint: Uri,
    client_id: String,
    client_secret: String,
    scopes: NonEmptyList[String],
    authClient: Resource[F, Client[F]])
      extends Http4sClientDsl[F] with Login[F, ClientCredentials[F]] {

    private case class Token(
      access_token: String,
      token_type: String,
      expires_in: Int // in second
    )

    override def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val getToken: F[Token] =
          utils.randomUUID[F].flatMap { uuid =>
            authenticationClient.expect[Token](POST(
              UrlForm(
                "grant_type" -> "client_credentials",
                "scope" -> scopes.toList.mkString(" ")
              ),
              auth_endpoint.withPath(path"/oauth2/token"),
              Authorization(BasicCredentials(client_id, client_secret))
            ).putHeaders(`Idempotency-Key`(show"$uuid")))
          }

        def updateToken(ref: Ref[F, Token]): F[Unit] =
          for {
            oldToken <- ref.get
            newToken <- getToken.delayBy(oldToken.expires_in.seconds)
            _ <- ref.set(newToken)
          } yield ()

        def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))

        loginInternal(client, getToken, updateToken, withToken)
      }
  }

  object ClientCredentials {
    def apply[F[_]](authClient: Resource[F, Client[F]])(
      auth_endpoint: Uri,
      client_id: String,
      client_secret: String,
      scopes: NonEmptyList[String]): ClientCredentials[F] =
      new ClientCredentials[F](
        auth_endpoint = auth_endpoint,
        client_id = client_id,
        client_secret = client_secret,
        scopes = scopes,
        authClient = authClient)
  }
}
