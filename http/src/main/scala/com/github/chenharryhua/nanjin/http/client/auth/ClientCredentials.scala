package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.Secret
import org.http4s.Method.POST
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, Request, Uri, UrlForm}

import scala.concurrent.duration.FiniteDuration

/** Credentials for OAuth 2.0 Client Credentials flow.
  *
  * Used to obtain an access token directly from the authorization server without user interaction. See [OAuth
  * 2.0 RFC 6749](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4).
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

sealed abstract private class ClientCredentialsAuth[F[_]: Async](
  authClient: Resource[F, Client[F]]
) extends OAuthTokenAuth[F](authClient) {

  override protected def renewOnRejection(current: Token, renewals: Renewals): F[Token] =
    renewals.getTokenFromCredentials
  override protected def renewOnSchedule(current: Token, renewals: Renewals): F[Token] =
    renewals.refreshOrFetch(current)

  override protected def renewalDelay(token: Token): Option[FiniteDuration] =
    token.expires_in.filter(_ > 0L).map(skewed)

  // Client credentials never surface this: a scheduled refresh without a refresh token falls back to a fresh
  // client-credentials exchange, so `renewals.refreshOrFetch` is the only refresh path used.
  override protected def noRefreshToken: Throwable =
    new IllegalStateException("client credentials token has no refresh_token")

  final override def login(businessClient: Client[F]): Resource[F, Client[F]] =
    wrapWithToken(businessClient)
}

final private class PostClientCredentials[F[_]: Async](
  authClient: Resource[F, Client[F]],
  credential: ClientCredentials
) extends ClientCredentialsAuth[F](authClient) {
  override protected val tokenRequestForm: UrlForm = {
    val form = UrlForm(
      "grant_type" -> "client_credentials",
      "client_id" -> credential.client_id,
      "client_secret" -> credential.client_secret.value)
    credential.scope.fold(form)(scopes => form + ("scope" -> scopes.toList.mkString(" ")))
  }

  override protected def tokenRefreshForm(refresh_token: String): UrlForm =
    UrlForm(
      "grant_type" -> "refresh_token",
      "refresh_token" -> refresh_token,
      "client_id" -> credential.client_id,
      "client_secret" -> credential.client_secret.value)

  override protected def authenticatedRequest(form: UrlForm): Request[F] =
    Request[F](method = POST, uri = credential.auth_endpoint).withEntity(form)

}

final private class BasicClientCredentials[F[_]: Async](
  authClient: Resource[F, Client[F]],
  credential: ClientCredentials
) extends ClientCredentialsAuth[F](authClient) {

  override protected val tokenRequestForm: UrlForm = {
    val form = UrlForm("grant_type" -> "client_credentials")
    credential.scope.fold(form)(scopes => form + ("scope" -> scopes.toList.mkString(" ")))
  }

  override protected def tokenRefreshForm(refresh_token: String): UrlForm =
    UrlForm("grant_type" -> "refresh_token", "refresh_token" -> refresh_token)

  private val basic_credentials: BasicCredentials =
    encodedBasicCredentials(credential.client_id, credential.client_secret.value)

  override protected def authenticatedRequest(form: UrlForm): Request[F] =
    Request[F](method = POST, uri = credential.auth_endpoint)
      .withEntity(form)
      .putHeaders(Authorization(basic_credentials))

}
