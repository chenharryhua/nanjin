package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.http.client.auth.UriJsonCodec.given
import io.circe.Codec
import org.http4s.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.{Authorization, Host}
import org.typelevel.ci.CIString

import scala.concurrent.duration.FiniteDuration

/** Salesforce-specific OAuth authentication helpers.
  *
  * Salesforce supports a non-standard OAuth 2.0 Password Grant flow, which exchanges a username and password
  * directly for an access token. This object provides a `Login` implementation tailored to Salesforce
  * semantics, including automatic request routing via the returned `instance_url`.
  *
  * @note
  *   Salesforce does not return an `expires_in` value, so scheduled renewal relies on a caller-provided token
  *   lifetime. A positive lifetime schedules renewal early (skewed before expiry); a non-positive or omitted
  *   lifetime disables scheduled renewal, leaving a rejected token to be replaced reactively.
  */
object Salesforce {

  /** Credentials for Salesforce OAuth 2.0 Password Grant flow.
    *
    * This flow exchanges a username and password directly for an access token. It should only be used in
    * trusted server-side environments.
    */
  final private class PasswordGrantAuth[F[_]: Async](
    credential: PasswordGrant,
    expiresIn: Option[FiniteDuration],
    authClient: Resource[F, Client[F]]
  ) extends Login[F] {

    private val urlForm: UrlForm = UrlForm(
      "grant_type" -> "password",
      "client_id" -> credential.client_id,
      "client_secret" -> credential.client_secret.value,
      "username" -> credential.username,
      "password" -> credential.password.value
    )

    private case class Token(
      access_token: String,
      instance_url: Uri,
      id: String,
      token_type: String,
      issued_at: String,
      signature: String)
        derives Codec.AsObject

    override def login(businessClient: Client[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val tac = new TokenAuthClient[F] {
          override protected type T = Token

          override protected val getTokenFromCredentials: F[Token] =
            authenticationClient.expect[Token](POST(urlForm, credential.auth_endpoint))

          override protected def renewOnRejection(token: Token): F[Token] = getTokenFromCredentials
          override protected def renewOnSchedule(token: Token): F[Token] = getTokenFromCredentials

          override protected def renewalDelay(token: Token): Option[FiniteDuration] =
            expiresIn.map(fd => skewed(fd.toSeconds))

          override protected def withToken(token: Token, req: Request[F]): Request[F] =
            req
              .withUri(
                token.instance_url
                  .withPath(req.uri.path)
                  .copy(query = req.uri.query, fragment = req.uri.fragment))
              .removeHeader[Host]
              .putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
        }

        tac.wrap(businessClient)
      }
  }

  final case class PasswordGrant(
    auth_endpoint: Uri,
    client_id: String,
    client_secret: Secret,
    username: String,
    password: Secret)

  /** Create a Salesforce `Login` using the Password Grant flow with scheduled token renewal.
    *
    * The resulting authenticated client automatically:
    *   - Fetches an access token using the password grant
    *   - Routes requests to the Salesforce `instance_url`
    *   - Re-authenticates with the supplied credentials before the token lifetime elapses
    *
    * @param authClient
    *   the HTTP client resource used for Salesforce token requests
    * @param credential
    *   password-grant credentials
    * @param expiresIn
    *   the assumed token lifetime. A positive value schedules renewal early (skewed before expiry); a
    *   non-positive value disables scheduled renewal, falling back to reactive replacement on rejection.
    */
  def apply[F[_]: Async](
    authClient: Resource[F, Client[F]],
    credential: PasswordGrant,
    expiresIn: FiniteDuration): Login[F] =
    new PasswordGrantAuth[F](credential, Some(expiresIn).filter(_.toSeconds > 0), authClient)

  /** Create a Salesforce `Login` using the Password Grant flow without scheduled renewal.
    *
    * Behaves like the three-argument overload but with no token lifetime, so no proactive renewal is
    * scheduled: a token is replaced only reactively, after the resource server rejects it.
    *
    * @param authClient
    *   the HTTP client resource used for Salesforce token requests
    * @param credential
    *   password-grant credentials
    */
  def apply[F[_]: Async](authClient: Resource[F, Client[F]], credential: PasswordGrant): Login[F] =
    new PasswordGrantAuth[F](credential, None, authClient)
}
