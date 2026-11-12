package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Resource}
import cats.syntax.functor.given
import io.circe.{Decoder, HCursor}
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.typelevel.ci.CIString
import org.http4s.{Credentials, Request, UrlForm}

import scala.concurrent.duration.FiniteDuration

/** Shared OAuth 2.0 token-exchange machinery for `Login` flows that fetch a bearer token from a form-encoded
  * token endpoint.
  *
  * The base owns everything common to those flows: the token response shape, wiring a `TokenAuthClient` over
  * the supplied authentication client, retaining a prior `refresh_token` when a refresh response omits one,
  * and attaching the token as an `Authorization` header. Each flow supplies only its request forms, how it
  * authenticates the token request (client-secret-post vs client-secret-basic), and how it replaces a token
  * on the reactive (`renewOnRejection`) and proactive (`renewOnSchedule`) paths.
  *
  * The two replacement hooks receive the current token together with `getTokenFromCredentials` (the primary
  * grant) and `refreshToken` (exchange a stored `refresh_token`, carrying the old one forward when the
  * response omits a replacement). A flow composes them: client credentials re-exchange on rejection and
  * refresh on schedule; authorization code refreshes on both paths and treats a missing refresh token as an
  * error rather than falling back to the single-use code.
  *
  * @param authClient
  *   an HTTP client resource used to fetch and refresh tokens
  */
abstract private class OAuthTokenAuth[F[_]](authClient: Resource[F, Client[F]])(using F: Async[F])
    extends Login[F] {

  final protected case class Token(
    token_type: String,
    access_token: String,
    expires_in: Option[Long], // in seconds
    refresh_token: Option[String])

  protected object Token {

    /** RFC 6749 §5.1 specifies `expires_in` as a number, but servers are known to send it as a numeric
      * string. Accept either: a lifetime this layer only uses to schedule renewal should not fail the whole
      * token exchange, and a value that is neither still does.
      */
    private val lenient_seconds: Decoder[Long] =
      Decoder[Long].or(Decoder[String].emap(s => s.toLongOption.toRight(s"not a number: $s")))

    given Decoder[Token] = (c: HCursor) =>
      for {
        token_type <- c.get[String]("token_type")
        access_token <- c.get[String]("access_token")
        expires_in <- c.get[Option[Long]]("expires_in")(using Decoder.decodeOption(using lenient_seconds))
        refresh_token <- c.get[Option[String]]("refresh_token")
      } yield Token(token_type, access_token, expires_in, refresh_token)
  }

  /** The replacement grants available to the reactive and proactive renewal hooks. */
  final protected class Renewals private[OAuthTokenAuth] (
    val getTokenFromCredentials: F[Token],
    refresh: Token => F[Token]) {

    /** Exchange `token`'s refresh token, carrying the old one forward when the response omits one; fails with
      * `noRefreshToken` when `token` has no refresh token.
      */
    def refreshToken(token: Token): F[Token] = refresh(token)

    /** Exchange `token`'s refresh token when it has one, otherwise fall back to `getTokenFromCredentials`. */
    def refreshOrFetch(token: Token): F[Token] =
      token.refresh_token.fold(getTokenFromCredentials)(_ => refresh(token))
  }

  /** The form that exchanges the flow's primary grant for a token. */
  protected def tokenRequestForm: UrlForm

  /** The form that exchanges a stored `refresh_token` for a replacement token. */
  protected def tokenRefreshForm(refresh_token: String): UrlForm

  /** Builds the token-endpoint request from a form, applying the flow's client-authentication style. */
  protected def authenticatedRequest(form: UrlForm): Request[F]

  /** Replaces a token the resource server rejected with `Unauthorized`. */
  protected def renewOnRejection(current: Token, renewals: Renewals): F[Token]

  /** Replaces a token proactively before it expires, driven by `renewalDelay`. */
  protected def renewOnSchedule(current: Token, renewals: Renewals): F[Token]

  /** When to schedule proactive renewal of `token`, or `None` to disable it. */
  protected def renewalDelay(token: Token): Option[FiniteDuration]

  /** The error raised when a refresh is required but the token carries no `refresh_token`. Flows that fall
    * back to another grant never surface it; a flow with specific guidance overrides it.
    */
  protected def noRefreshToken: Throwable =
    new IllegalStateException("token has no refresh_token")

  /** Wraps `businessClient` with the flow's token lifecycle. */
  final protected def wrapWithToken(businessClient: Client[F]): Resource[F, Client[F]] =
    authClient.flatMap { authentication_client =>
      val token_auth_client: TokenAuthClient[F] = new TokenAuthClient[F] {
        override protected type T = Token

        override protected val getTokenFromCredentials: F[Token] =
          authentication_client.expect[Token](authenticatedRequest(tokenRequestForm))

        private def refresh_access_token(current: Token): F[Token] =
          current.refresh_token match {
            case None                => F.raiseError(noRefreshToken)
            case Some(refresh_token) =>
              authentication_client.expect[Token](authenticatedRequest(tokenRefreshForm(refresh_token)))
                .map(refreshed =>
                  refreshed.copy(refresh_token = refreshed.refresh_token.orElse(current.refresh_token)))
          }

        private val renewals = new Renewals(getTokenFromCredentials, refresh_access_token)

        override protected def renewOnRejection(token: Token): F[Token] =
          OAuthTokenAuth.this.renewOnRejection(token, renewals)

        override protected def renewOnSchedule(token: Token): F[Token] =
          OAuthTokenAuth.this.renewOnSchedule(token, renewals)

        override protected def renewalDelay(token: Token): Option[FiniteDuration] =
          OAuthTokenAuth.this.renewalDelay(token)

        override protected def withToken(token: Token, req: Request[F]): Request[F] =
          req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
      }

      token_auth_client.wrap(businessClient)
    }
}
