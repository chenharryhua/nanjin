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

/** Token-endpoint authentication method for a confidential OAuth 2.0 client.
  *
  * `ClientSecretPost` sends `client_id` and `client_secret` as form fields. `ClientSecretBasic` sends them in
  * an HTTP Basic `Authorization` header instead.
  */
enum ClientAuthentication:
  case ClientSecretPost
  case ClientSecretBasic

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

/** OAuth 2.0 Client Credentials flow authenticator.
  *
  * Obtains an access token from the authorization server and attaches it to requests. Proactive renewal uses
  * a refresh token when the server supplies one and otherwise performs another client-credentials exchange. A
  * rejected access token is always replaced through a fresh client-credentials exchange.
  *
  * @param credential
  *   client credentials and optional scopes
  * @param auth_client
  *   an HTTP client used to fetch and refresh tokens
  * @param authentication
  *   token-endpoint client authentication method
  */
final private class ClientCredentialsAuth[F[_]: Async](
  credential: ClientCredentials,
  auth_client: Resource[F, Client[F]],
  authentication: ClientAuthentication
) extends Login[F] {
  private case class Token(
    token_type: String,
    access_token: String,
    expires_in: Option[Long], // in seconds
    refresh_token: Option[String])
      derives Codec.AsObject

  private val token_request_form: UrlForm = {
    val form = authentication match {
      case ClientAuthentication.ClientSecretPost =>
        UrlForm(
          "grant_type" -> "client_credentials",
          "client_id" -> credential.client_id,
          "client_secret" -> credential.client_secret.value)
      case ClientAuthentication.ClientSecretBasic =>
        UrlForm("grant_type" -> "client_credentials")
    }
    credential.scope.fold(form)(scopes => form + ("scope" -> scopes.toList.mkString(" ")))
  }

  private def basic_credentials: BasicCredentials = {
    // RFC 6749 §2.3.1: encode each value using application/x-www-form-urlencoded
    // before using the client id/password as HTTP Basic credentials.
    def encode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)
    BasicCredentials(encode(credential.client_id), encode(credential.client_secret.value))
  }

  private def authenticated_request(form: UrlForm): Request[F] = {
    val request = Request[F](method = POST, uri = credential.auth_endpoint).withEntity(form)
    authentication match {
      case ClientAuthentication.ClientSecretPost  => request
      case ClientAuthentication.ClientSecretBasic =>
        request.putHeaders(Authorization(basic_credentials))
    }
  }

  private def refresh_token_form(refresh_token: String): UrlForm =
    authentication match {
      case ClientAuthentication.ClientSecretPost =>
        UrlForm(
          "grant_type" -> "refresh_token",
          "refresh_token" -> refresh_token,
          "client_id" -> credential.client_id,
          "client_secret" -> credential.client_secret.value)
      case ClientAuthentication.ClientSecretBasic =>
        UrlForm("grant_type" -> "refresh_token", "refresh_token" -> refresh_token)
    }

  override def login(businessClient: Client[F]): Resource[F, Client[F]] =
    auth_client.flatMap { authentication_client =>
      val token_auth_client: TokenAuthClient[F] = new TokenAuthClient[F] {
        override protected type T = Token

        override protected val getTokenFromCredentials: F[Token] =
          authentication_client.expect[Token](authenticated_request(token_request_form))

        private def refresh_access_token(current_token: Token): F[Token] =
          current_token.refresh_token.fold(getTokenFromCredentials) { refresh_token =>
            authentication_client.expect[Token](authenticated_request(refresh_token_form(refresh_token)))
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
