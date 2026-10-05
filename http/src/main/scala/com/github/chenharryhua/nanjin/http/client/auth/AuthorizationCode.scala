package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Resource}
import cats.syntax.flatMap.given
import com.github.chenharryhua.nanjin.common.Secret
import org.http4s.Method.POST
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, Request, Uri, UrlForm}

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.FiniteDuration

/** Credentials for OAuth 2.0 Authorization Code flow.
  *
  * Used to exchange an authorization code obtained from user consent for an access token. Scopes belong to
  * the preceding authorization request and are not repeated during the token exchange.
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
  */
final case class AuthorizationCode(
  auth_endpoint: Uri,
  client_id: String,
  client_secret: Secret,
  code: Secret,
  redirect_uri: String)

/** OAuth 2.0 Authorization Code flow authenticator.
  *
  * Exchanges the authorization code for an access token, attaches the token to requests, and renews through a
  * server-supplied `refresh_token`. Both the reactive (`renewOnRejection`) and proactive (`renewOnSchedule`)
  * paths refresh; a rejection with no refresh token fails, because the single-use code cannot be exchanged
  * again. Each instance permits one resource acquisition for the same reason.
  *
  * The leaf classes differ only in how the token request authenticates the client: `PostAuthorizationCode`
  * places `client_id`/`client_secret` in the form body (client-secret-post), while `BasicAuthorizationCode`
  * sends them in an HTTP Basic `Authorization` header and keeps them out of the body (client-secret-basic).
  *
  * @param authClient
  *   an HTTP client used to fetch and refresh tokens
  */
sealed abstract private class AuthorizationCodeAuth[F[_]](
  authClient: Resource[F, Client[F]]
)(using F: Async[F])
    extends OAuthTokenAuth[F](authClient) {
  private val code_available = new AtomicBoolean(true)

  private def claim_authorization_code: F[Unit] =
    F.delay(code_available.compareAndSet(true, false)).flatMap { claimed =>
      if (claimed) F.unit
      else
        F.raiseError(new IllegalStateException("authorization code login has already been acquired"))
    }

  // Authorization codes are single-use, so a rejected or expiring token can only be replaced through a
  // refresh token; without one the user must authorize again.
  override protected def renewOnRejection(current: Token, renewals: Renewals): F[Token] =
    renewals.refreshToken(current)
  override protected def renewOnSchedule(current: Token, renewals: Renewals): F[Token] =
    renewals.refreshToken(current)

  override protected def renewalDelay(token: Token): Option[FiniteDuration] =
    token.refresh_token.flatMap(_ => token.expires_in.filter(_ > 0L).map(skewed))

  override protected def noRefreshToken: Throwable =
    new IllegalStateException(
      "authorization code token has no refresh_token; user reauthorization is required")

  final override def login(businessClient: Client[F]): Resource[F, Client[F]] =
    Resource.eval(claim_authorization_code).flatMap(_ => wrapWithToken(businessClient))
}

final private class PostAuthorizationCode[F[_]: Async](
  authClient: Resource[F, Client[F]],
  credential: AuthorizationCode
) extends AuthorizationCodeAuth[F](authClient) {

  override protected val tokenRequestForm: UrlForm = UrlForm(
    "grant_type" -> "authorization_code",
    "client_id" -> credential.client_id,
    "client_secret" -> credential.client_secret.value,
    "code" -> credential.code.value,
    "redirect_uri" -> credential.redirect_uri
  )

  override protected def tokenRefreshForm(refresh_token: String): UrlForm =
    UrlForm(
      "grant_type" -> "refresh_token",
      "client_id" -> credential.client_id,
      "client_secret" -> credential.client_secret.value,
      "refresh_token" -> refresh_token)

  override protected def authenticatedRequest(form: UrlForm): Request[F] =
    Request[F](method = POST, uri = credential.auth_endpoint).withEntity(form)
}

final private class BasicAuthorizationCode[F[_]: Async](
  authClient: Resource[F, Client[F]],
  credential: AuthorizationCode
) extends AuthorizationCodeAuth[F](authClient) {

  override protected val tokenRequestForm: UrlForm = UrlForm(
    "grant_type" -> "authorization_code",
    "code" -> credential.code.value,
    "redirect_uri" -> credential.redirect_uri
  )

  override protected def tokenRefreshForm(refresh_token: String): UrlForm =
    UrlForm("grant_type" -> "refresh_token", "refresh_token" -> refresh_token)

  private val basic_credentials: BasicCredentials =
    encodedBasicCredentials(credential.client_id, credential.client_secret.value)

  override protected def authenticatedRequest(form: UrlForm): Request[F] =
    Request[F](method = POST, uri = credential.auth_endpoint)
      .withEntity(form)
      .putHeaders(Authorization(basic_credentials))
}
