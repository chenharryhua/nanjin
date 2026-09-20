package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.Secret
import io.circe.Codec
import org.http4s.*
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.typelevel.ci.CIString

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
  * Exchanges the authorization code for an access token, attaches the token to requests, and uses a refresh
  * token when the server supplies one. Each instance permits one resource acquisition because authorization
  * codes are single-use.
  *
  * @param credential
  *   authorization code, client credentials, and redirect URI
  * @param auth_client
  *   an HTTP client used to fetch and refresh tokens
  */
final private class AuthorizationCodeAuth[F[_]: Async](
  credential: AuthorizationCode,
  auth_client: Resource[F, Client[F]])
    extends Login[F] {
  private case class Token(
    access_token: String,
    token_type: String,
    expires_in: Option[Long], // in seconds
    refresh_token: Option[String]
  ) derives Codec.AsObject

  private val code_available = new AtomicBoolean(true)

  private val token_request_form = UrlForm(
    "grant_type" -> "authorization_code",
    "client_id" -> credential.client_id,
    "code" -> credential.code.value,
    "redirect_uri" -> credential.redirect_uri
  )

  private def claim_authorization_code: F[Unit] =
    Async[F].flatMap(Async[F].delay(code_available.compareAndSet(true, false))) { claimed =>
      if (claimed) Async[F].unit
      else
        Async[F].raiseError(
          new IllegalStateException("authorization code login has already been acquired")
        )
    }

  override def login(businessClient: Client[F]): Resource[F, Client[F]] =
    Resource.eval(claim_authorization_code).flatMap { _ =>
      auth_client.flatMap { authentication_client =>
        val token_auth_client = new TokenAuthClient[F] {
          override protected type T = Token

          override protected val getTokenFromCredentials: F[Token] =
            authentication_client.expect[Token](
              POST(
                token_request_form,
                credential.auth_endpoint,
                Authorization(BasicCredentials(credential.client_id, credential.client_secret.value))
              ))

          private def refresh_access_token(current_token: Token): F[Token] =
            current_token.refresh_token.fold(
              Async[F].raiseError[Token](
                new IllegalStateException(
                  "authorization code token has no refresh_token; user reauthorization is required")
              )
            ) { refresh_token =>
              Async[F].map(authentication_client.expect[Token](POST(
                UrlForm(
                  "grant_type" -> "refresh_token",
                  "client_id" -> credential.client_id,
                  "refresh_token" -> refresh_token),
                credential.auth_endpoint,
                Authorization(BasicCredentials(credential.client_id, credential.client_secret.value))
              ))) { refreshed =>
                refreshed.copy(refresh_token = refreshed.refresh_token.orElse(current_token.refresh_token))
              }
            }

          override protected def renewOnRejection(token: Token): F[Token] = refresh_access_token(token)
          override protected def renewOnSchedule(token: Token): F[Token] = refresh_access_token(token)

          override protected def renewalDelay(token: Token): Option[FiniteDuration] =
            token.refresh_token.flatMap(_ => token.expires_in.filter(_ > 0L).map(skewed))

          override protected def withToken(token: Token, req: Request[F]): Request[F] =
            req.putHeaders(Authorization(Credentials.Token(CIString(token.token_type), token.access_token)))
        }

        token_auth_client.wrap(businessClient)
      }
    }
}
