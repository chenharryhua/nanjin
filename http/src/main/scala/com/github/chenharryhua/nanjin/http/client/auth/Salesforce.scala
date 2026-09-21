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
  * The password grant sends both `client_secret` and `password` in the token request's form body. `Secret`
  * masks those values when configuration objects are rendered, but does not redact the wire request. The
  * token endpoint must use TLS outside local tests, and authentication-client middleware must not log request
  * bodies.
  *
  * @note
  *   Salesforce does not return an `expires_in` value, so scheduled renewal relies on a caller-provided token
  *   lifetime. A positive lifetime schedules renewal early (skewed before expiry); a non-positive or omitted
  *   lifetime disables scheduled renewal, leaving a rejected token to be replaced reactively.
  */
object Salesforce {

  /** Implements the Salesforce password-grant token exchange and authenticated-client wrapping. */
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
      id: Option[String],
      token_type: String,
      issued_at: Option[String],
      signature: Option[String])
        derives Codec.AsObject

    override def login(businessClient: Client[F]): Resource[F, Client[F]] =
      authClient.flatMap { authenticationClient =>
        val token_auth_client = new TokenAuthClient[F] {
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

        token_auth_client.wrap(businessClient)
      }
  }

  /** Credentials for Salesforce OAuth 2.0 Password Grant flow.
    *
    * This flow exchanges a username and password directly for an access token and should only be used by a
    * trusted server-side application. `client_secret` and `password` are transmitted in the token request's
    * form body. Their `Secret` wrappers mask normal object rendering only: use TLS and keep request-body
    * logging disabled on the authentication client.
    *
    * @param auth_endpoint
    *   Salesforce token endpoint, normally `https://login.salesforce.com/services/oauth2/token` or the
    *   sandbox equivalent at `https://test.salesforce.com/services/oauth2/token`
    * @param client_id
    *   connected-app consumer key
    * @param client_secret
    *   connected-app consumer secret
    * @param username
    *   Salesforce username
    * @param password
    *   Salesforce password accepted by the token endpoint
    */
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
    * Each token-endpoint request is attempted once by this layer. Configure retries and failure observability
    * on `authClient` when needed, while keeping request-body logging disabled because the form contains both
    * secrets. The password grant uses `POST`, so use `recklessHttpRetry` or a custom `httpRetry` predicate
    * only when repeating the exchange is acceptable.
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
    * scheduled: a token is replaced only reactively, after the resource server rejects it. Token-endpoint
    * requests retain the same single-attempt behavior; configure retries and failure observability on
    * `authClient`.
    *
    * @param authClient
    *   the HTTP client resource used for Salesforce token requests
    * @param credential
    *   password-grant credentials
    */
  def apply[F[_]: Async](authClient: Resource[F, Client[F]], credential: PasswordGrant): Login[F] =
    new PasswordGrantAuth[F](credential, None, authClient)
}
