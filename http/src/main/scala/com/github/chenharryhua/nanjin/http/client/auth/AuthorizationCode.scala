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

import scala.concurrent.duration.FiniteDuration

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
final private class AuthorizationCodeAuth[F[_]: Async](
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

        override protected def getTokenFromCredentials: F[Token] =
          validate_expires_in(
            authenticationClient.expect[Token](
              POST(
                urlForm,
                credential.auth_endpoint,
                Authorization(BasicCredentials(credential.client_id, credential.client_secret.value))
              )),
            token => Some(token.expires_in)
          )

        private def refreshAccessToken(pre: Token): F[Token] =
          validate_expires_in(
            authenticationClient.expect[Token](
              POST(
                UrlForm(
                  "grant_type" -> "refresh_token",
                  "client_id" -> credential.client_id,
                  "refresh_token" -> pre.refresh_token),
                credential.auth_endpoint,
                Authorization(BasicCredentials(credential.client_id, credential.client_secret.value))
              )),
            token => Some(token.expires_in)
          )

        override protected def renewOnRejection: Token => F[Token] = refreshAccessToken
        override protected def renewOnSchedule: Token => F[Token] = refreshAccessToken

        override protected def renewalDelay: Token => Option[FiniteDuration] =
          token => Some(skewed(token.expires_in))

        override protected def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
      }

      tac.wrap(businessClient)
    }
}
