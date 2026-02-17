package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.NonEmptyList
import cats.effect.implicits.genTemporalOps_
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.show.showInterpolator
import io.circe.generic.auto.*
import org.http4s.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.{`Idempotency-Key`, Authorization}
import org.typelevel.ci.CIString

import java.util.UUID
import scala.concurrent.duration.{DurationLong, FiniteDuration}

/*
 *@see Spec: https://datatracker.ietf.org/doc/html/rfc6749
 */

/** Credentials for OAuth 2.0 Client Credentials flow.
  *
  * Used to obtain an access token directly from the authorization server without user interaction.
  *
  * @param auth_endpoint
  *   the token endpoint URI of the authorization server
  * @param client_id
  *   the client ID issued by the authorization server
  * @param client_secret
  *   the client secret issued by the authorization server
  * @param scope
  *   optional list of scopes to request. If not provided, default server scopes are used
  */
final case class ClientCredentials(
  auth_endpoint: Uri,
  client_id: String,
  client_secret: String,
  scope: Option[NonEmptyList[String]] = None)

/** Credentials for OAuth 2.0 Authorization Code flow.
  *
  * Used to exchange an authorization code (obtained from user consent) for an access token. Supports optional
  * scopes.
  *
  * @param auth_endpoint
  *   the token endpoint URI of the authorization server
  * @param client_id
  *   the client ID issued by the authorization server
  * @param client_secret
  *   the client secret issued by the authorization server
  * @param code
  *   the authorization code obtained after user consent
  * @param redirect_uri
  *   the redirect URI used in the authorization request
  * @param scope
  *   optional list of scopes to request. If not provided, default server scopes are used
  */
final case class AuthorizationCode(
  auth_endpoint: Uri,
  client_id: String,
  client_secret: String,
  code: String,
  redirect_uri: String,
  scope: Option[NonEmptyList[String]] = None)

/*
 * private section
 */

/** OAuth 2.0 Client Credentials flow authenticator.
  *
  * Automatically obtains an access token from the authorization server and attaches it to requests. Supports
  * optional scopes and token renewal.
  *
  * @param credential
  *   client credentials and optional scopes
  * @param authClient
  *   an HTTP client used to fetch tokens
  */
private class ClientCredentialsAuth[F[_]: Async](
  credential: ClientCredentials,
  authClient: Resource[F, Client[F]],
  uuidGenerator: F[UUID]
) extends Login[F] {
  private case class Token(
    token_type: String,
    access_token: String,
    expires_in: Option[Long], // in seconds
    refresh_token: Option[String])

  private val urlForm: UrlForm = {
    val uf = UrlForm(
      "grant_type" -> "client_credentials",
      "client_id" -> credential.client_id,
      "client_secret" -> credential.client_secret)
    credential.scope.fold(uf)(s => uf + ("scope" -> s.toList.mkString(" ")))
  }

  override def login(client: Client[F]): Resource[F, Client[F]] =
    authClient.flatMap { authenticationClient =>
      val tac: TokenAuthClient[F, Token] = new TokenAuthClient[F, Token]() {
        override protected def getToken: F[Token] =
          post_token[Token](authenticationClient, credential.auth_endpoint, urlForm, uuidGenerator)

        private def refresh_access_token(refresh_token: String): F[Token] =
          uuidGenerator.flatMap { uuid =>
            authenticationClient.expect[Token](
              POST(
                UrlForm(
                  "grant_type" -> "refresh_token",
                  "refresh_token" -> refresh_token,
                  "client_id" -> credential.client_id,
                  "client_secret" -> credential.client_secret),
                credential.auth_endpoint
              ).putHeaders(`Idempotency-Key`(show"$uuid")))
          }

        /** @note A 30-second skew is applied when scheduling token renewal to avoid race conditions. */
        private def skewed(expire: Long): FiniteDuration = // suggested by ChatGPT
          (expire.seconds - 30.seconds).max(0.seconds)

        override protected def renewToken(ref: Ref[F, Token]): F[Unit] =
          for {
            oldToken <- ref.get
            newToken <- (oldToken.expires_in, oldToken.refresh_token) match {
              case (Some(expire), None) =>
                getToken.delayBy(skewed(expire))
              case (Some(expire), Some(token)) =>
                refresh_access_token(token).delayBy(skewed(expire))
              case _ =>
                Async[F].never[Token]
            }
            _ <- ref.set(newToken)
          } yield ()

        override protected def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
      }

      tac.wrap(client)
    }
}

/** OAuth 2.0 Authorization Code flow authenticator.
  *
  * Automatically exchanges the authorization code for an access token, attaches the token to requests, and
  * handles token refresh.
  *
  * @param credential
  *   authorization code, client credentials, redirect URI, and optional scopes
  * @param authClient
  *   an HTTP client used to fetch and refresh tokens
  */
private class AuthorizationCodeAuth[F[_]: Async](
  credential: AuthorizationCode,
  authClient: Resource[F, Client[F]],
  uuidGenerator: F[UUID])
    extends Login[F] {
  private case class Token(
    access_token: String,
    refresh_token: String,
    id_token: String,
    token_type: String,
    expires_in: Long // in second
  )

  private val urlForm: UrlForm = {
    val uf = UrlForm(
      "grant_type" -> "authorization_code",
      "client_id" -> credential.client_id,
      "code" -> credential.code,
      "redirect_uri" -> credential.redirect_uri
    )
    credential.scope.fold(uf)(s => uf + ("scope" -> s.toList.mkString(" ")))
  }

  override def login(client: Client[F]): Resource[F, Client[F]] =
    authClient.flatMap { authenticationClient =>
      val tac = new TokenAuthClient[F, Token] {
        override protected def getToken: F[Token] =
          uuidGenerator.flatMap { uuid =>
            authenticationClient.expect[Token](
              POST(
                urlForm,
                credential.auth_endpoint,
                Authorization(BasicCredentials(credential.client_id, credential.client_secret))
              ).putHeaders(`Idempotency-Key`(show"$uuid")))
          }

        private def refresh_access_token(pre: Token): F[Token] =
          uuidGenerator.flatMap { uuid =>
            authClient.use(
              _.expect[Token](POST(
                UrlForm(
                  "grant_type" -> "refresh_token",
                  "client_id" -> credential.client_id,
                  "refresh_token" -> pre.refresh_token),
                credential.auth_endpoint,
                Authorization(BasicCredentials(credential.client_id, credential.client_secret))
              ).putHeaders(`Idempotency-Key`(show"$uuid"))))
          }

        override protected def renewToken(ref: Ref[F, Token]): F[Unit] =
          for {
            oldToken <- ref.get
            newToken <- refresh_access_token(oldToken).delayBy(oldToken.expires_in.seconds)
            _ <- ref.set(newToken)
          } yield ()

        override protected def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
      }

      tac.wrap(client)
    }
}
