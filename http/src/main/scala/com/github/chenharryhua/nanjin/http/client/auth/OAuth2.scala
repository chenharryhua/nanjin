package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.Secret
import io.circe.Codec
import org.http4s.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.typelevel.ci.CIString

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
  client_secret: Secret,
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
  client_secret: Secret,
  code: Secret,
  redirect_uri: String,
  scope: Option[NonEmptyList[String]] = None)

/*
 * private section
 */

private def validate_expires_in[F[_]: Async, A](fa: F[A])(expires_in: A => Option[Long]): F[A] =
  Async[F].flatMap(fa) { value =>
    expires_in(value) match {
      case Some(seconds) if seconds <= 0L =>
        Async[F].raiseError(new IllegalArgumentException(s"expires_in must be positive, but was $seconds"))
      case _ =>
        Async[F].pure(value)
    }
  }

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
  authClient: Resource[F, Client[F]]
) extends Login[F] {
  private case class Token(
    token_type: String,
    access_token: String,
    expires_in: Option[Long], // in seconds
    refresh_token: Option[String])
      derives Codec.AsObject

  private val urlForm: UrlForm = {
    val uf = UrlForm(
      "grant_type" -> "client_credentials",
      "client_id" -> credential.client_id,
      "client_secret" -> credential.client_secret.value)
    credential.scope.fold(uf)(s => uf + ("scope" -> s.toList.mkString(" ")))
  }

  override def login(businessClient: Client[F]): Resource[F, Client[F]] =
    authClient.flatMap { authenticationClient =>
      val tac: TokenAuthClient[F] = new TokenAuthClient[F]() {
        override protected type T = Token

        override protected def getToken: F[Token] =
          validate_expires_in(postToken[Token](authenticationClient, credential.auth_endpoint, urlForm))(
            _.expires_in)

        override protected def refreshToken: Token => F[Token] = _ => getToken

        private def refreshAccessToken(refresh_token: String): F[Token] =
          validate_expires_in(
            authenticationClient.expect[Token](POST(
              UrlForm(
                "grant_type" -> "refresh_token",
                "refresh_token" -> refresh_token,
                "client_id" -> credential.client_id,
                "client_secret" -> credential.client_secret.value),
              credential.auth_endpoint
            )))(_.expires_in)

        override protected def renewToken: Token => F[Token] =
          token => token.refresh_token.fold(getToken)(refreshAccessToken)

        override protected def renewalDelay: Token => Option[FiniteDuration] =
          token => token.expires_in.map(skewed)

        override protected def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
      }

      tac.wrap(businessClient)
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
  authClient: Resource[F, Client[F]])
    extends Login[F] {
  private case class Token(
    access_token: String,
    refresh_token: String,
    id_token: String,
    token_type: String,
    expires_in: Long // in second
  ) derives Codec.AsObject

  private val urlForm: UrlForm = {
    val uf = UrlForm(
      "grant_type" -> "authorization_code",
      "client_id" -> credential.client_id,
      "code" -> credential.code.value,
      "redirect_uri" -> credential.redirect_uri
    )
    credential.scope.fold(uf)(s => uf + ("scope" -> s.toList.mkString(" ")))
  }

  override def login(businessClient: Client[F]): Resource[F, Client[F]] =
    authClient.flatMap { authenticationClient =>
      val tac = new TokenAuthClient[F] {
        override protected type T = Token

        override protected def getToken: F[Token] =
          validate_expires_in(
            authenticationClient.expect[Token](
              POST(
                urlForm,
                credential.auth_endpoint,
                Authorization(BasicCredentials(credential.client_id, credential.client_secret.value))
              )))(token => Some(token.expires_in))

        private def refreshAccessToken(pre: Token): F[Token] =
          validate_expires_in(
            authenticationClient.expect[Token](POST(
              UrlForm(
                "grant_type" -> "refresh_token",
                "client_id" -> credential.client_id,
                "refresh_token" -> pre.refresh_token),
              credential.auth_endpoint,
              Authorization(BasicCredentials(credential.client_id, credential.client_secret.value))
            )))(token => Some(token.expires_in))

        override protected def refreshToken: Token => F[Token] =
          refreshAccessToken

        override protected def renewalDelay: Token => Option[FiniteDuration] =
          token => Some(skewed(token.expires_in))

        override protected def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
      }

      tac.wrap(businessClient)
    }
}

/** Renewal-scheduling strategy for token-based auth.
  *
  * `skewed` renews long-lived tokens `SKEW` early. For lifetimes of `2 * SKEW` or less, renewal occurs
  * halfway through the lifetime so the delay remains positive while retaining time to replace the token
  * before expiry. `RENEW_FAILURE_BACKOFF` governs the failure path: after a renewal fails, the same
  * replacement is retried after this delay without reapplying the token's full renewal schedule.
  */

/** Renew long-lived tokens this early to avoid racing an in-flight request against expiry. */
private val SKEW: FiniteDuration = 30.seconds

/** Delay before the renewal loop retries after a failed renewal, bounding CPU / auth-endpoint load. */
private val RENEW_FAILURE_BACKOFF: FiniteDuration = 5.seconds

/** Schedule renewal `SKEW` early for long lifetimes, or halfway through a short lifetime. */
private def skewed(lifetime_seconds: Long): FiniteDuration = {
  val lifetime = lifetime_seconds.seconds
  lifetime - SKEW.min(lifetime / 2L)
}
