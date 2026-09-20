package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.Secret
import io.circe.Codec
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{Credentials, Request, Uri, UrlForm}
import org.typelevel.ci.CIString

import scala.concurrent.duration.FiniteDuration

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
final private class ClientCredentialsAuth[F[_]: Async](
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

        override protected def getTokenFromCredentials: F[Token] =
          authenticationClient.expect[Token](POST(urlForm, credential.auth_endpoint))

        private def refreshAccessToken(refresh_token: String): F[Token] =
          authenticationClient.expect[Token](
            POST(
              UrlForm(
                "grant_type" -> "refresh_token",
                "refresh_token" -> refresh_token,
                "client_id" -> credential.client_id,
                "client_secret" -> credential.client_secret.value),
              credential.auth_endpoint
            ))

        override protected def renewOnRejection: Token => F[Token] = _ => getTokenFromCredentials
        override protected def renewOnSchedule: Token => F[Token] =
          token => token.refresh_token.fold(getTokenFromCredentials)(refreshAccessToken)

        override protected def renewalDelay: Token => Option[FiniteDuration] =
          token => token.expires_in.filter(_ > 0L).map(skewed)

        override protected def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
      }

      tac.wrap(businessClient)
    }
}
