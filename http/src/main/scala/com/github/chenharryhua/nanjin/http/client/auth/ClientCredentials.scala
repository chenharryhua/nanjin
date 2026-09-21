package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.Secret
import io.circe.Codec
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, Credentials, Request, Uri, UrlForm}
import org.typelevel.ci.CIString

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
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
) extends Login[F] {
  private case class Token(
    token_type: String,
    access_token: String,
    expires_in: Option[Long], // in seconds
    refresh_token: Option[String])
      derives Codec.AsObject

  protected def tokenRequestForm: UrlForm
  protected def tokenRefreshForm(refresh_token: String): UrlForm
  protected def authenticatedRequest(form: UrlForm): Request[F]

  final override def login(businessClient: Client[F]): Resource[F, Client[F]] =
    authClient.flatMap { authentication_client =>
      val token_auth_client: TokenAuthClient[F] = new TokenAuthClient[F] {
        override protected type T = Token

        override protected val getTokenFromCredentials: F[Token] =
          authentication_client.expect[Token](authenticatedRequest(tokenRequestForm))

        private def refresh_access_token(current_token: Token): F[Token] =
          current_token.refresh_token.fold(getTokenFromCredentials) { refresh_token =>
            authentication_client.expect[Token](authenticatedRequest(tokenRefreshForm(refresh_token)))
              .map { refreshed =>
                refreshed.copy(refresh_token = refreshed.refresh_token.orElse(current_token.refresh_token))
              }
          }

        override protected def renewOnRejection(token: Token): F[Token] = getTokenFromCredentials
        override protected def renewOnSchedule(token: Token): F[Token] = refresh_access_token(token)

        override protected def renewalDelay(token: Token): Option[FiniteDuration] =
          token.expires_in.filter(_ > 0L).map(skewed)

        override protected def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
      }

      token_auth_client.wrap(businessClient)
    }
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

  private val basic_credentials: BasicCredentials = {
    // RFC 6749 §2.3.1: encode each value using application/x-www-form-urlencoded
    // before using the client id/password as HTTP Basic credentials.
    def encode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)
    BasicCredentials(encode(credential.client_id), encode(credential.client_secret.value))
  }

  override protected def authenticatedRequest(form: UrlForm): Request[F] =
    Request[F](method = POST, uri = credential.auth_endpoint)
      .withEntity(form)
      .putHeaders(Authorization(basic_credentials))

}
