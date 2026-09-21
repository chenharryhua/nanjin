package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.Resource
import cats.effect.kernel.Async
import org.http4s.client.Client

/** Creates a `Login` instance using OAuth 2.0 Client Credentials flow and `ClientSecretPost` token-endpoint
  * authentication.
  *
  * Automatically handles token acquisition, attaching the access token to requests, and optional token
  * renewal. A positive `expires_in` schedules renewal before expiry; omission, zero, or a negative value
  * disables scheduled renewal, leaving `renewOnRejection` to replace a rejected token through a fresh
  * client-credentials exchange. Scheduled renewal uses a returned `refresh_token` when available and retains
  * it when a refresh response omits a replacement.
  *
  * `ClientSecretPost` places the client secret in the request body. Body-logging middleware can therefore
  * expose it. The token endpoint should use TLS outside local test environments. Use the overload accepting
  * `ClientAuthentication` when the server supports the recommended `ClientSecretBasic` method.
  *
  * Each token-endpoint request is attempted once by this layer. Configure retries, logging, metrics, and
  * tracing on `client` when needed; every acquisition and renewal is performed through that supplied client.
  * Token exchanges use `POST`, so use `recklessHttpRetry` or a custom `httpRetry` predicate only when
  * repeating the exchange is acceptable. Avoid logging request bodies because `ClientSecretPost` places the
  * secret there, and redact the `Authorization` header when using `ClientSecretBasic`.
  *
  * Acquiring `login.login(businessClient)` eagerly obtains the initial token and creates one token cache and
  * renewal fiber. Acquire that resource once at application startup and share the resulting authenticated
  * client. Reacquiring it creates an independent cache and performs another initial token exchange.
  *
  * If a business request receives `Unauthorized`, it is replayed once with a replacement token. This applies
  * to every HTTP method, including `POST`: request entities must be repeatable, and the resource server must
  * reject unauthorized requests before executing application side effects.
  *
  * Example usage:
  * {{{
  *   import cats.effect.{IO, Resource}
  *   import com.github.chenharryhua.nanjin.http.client.auth
  *   import com.github.chenharryhua.nanjin.http.client.middleware.recklessHttpRetry
  *   import org.http4s.client.Client
  *   import org.http4s.client.middleware.Logger
  *   import java.time.ZoneId
  *   import scala.concurrent.duration.*
  *
  *   val baseAuthenticationClient: Resource[IO, Client[IO]] = ???
  *   val businessClient: Resource[IO, Client[IO]] = ???
  *   val credentials: ClientCredentials =
  *     ClientCredentials(auth_endpoint, client_id, client_secret)
  *
  *   // Token-endpoint policy belongs to the authentication client. POST retries must be explicit.
  *   val authenticationClient = baseAuthenticationClient
  *     .map(Logger(logHeaders = false, logBody = false))
  *     .map(recklessHttpRetry(ZoneId.systemDefault(), _.fixedDelay(1.second).repeat.limited(3)))
  *
  *   val authenticatedClient: Resource[IO, Client[IO]] =
  *     auth.clientCredentials(authenticationClient, credentials).login(businessClient)
  *
  *   // In an application, allocate this resource once and share `client` for its lifetime.
  *   authenticatedClient.use { client =>
  *     client.expect[String]("https://service.example/resource")
  *   }
  * }}}
  *
  * @param client
  *   the HTTP client resource used to fetch tokens
  * @param credential
  *   the client credentials including optional scopes
  * @tparam F
  *   effect type with `Async`
  * @return
  *   a `Login[F]` instance that can wrap a client to perform authenticated requests
  */
def clientCredentials[F[_]: Async](
  client: Resource[F, Client[F]],
  credential: ClientCredentials
): Login[F] =
  clientCredentials(client, credential, ClientAuthentication.ClientSecretPost)

/** Creates a `Login` instance using OAuth 2.0 Client Credentials flow and the selected token-endpoint client
  * authentication method.
  *
  * `ClientSecretPost` sends `client_id` and `client_secret` as form fields. `ClientSecretBasic` sends them in
  * an HTTP Basic `Authorization` header and omits both fields from the form. Header- or body-logging
  * middleware must redact credentials as appropriate.
  *
  * Retry, logging, metrics, and tracing belong on the supplied `client`; this authenticator attempts each
  * token exchange once. Each acquisition of the returned login resource owns an independent token cache and
  * renewal fiber, so applications should normally acquire it once and share the authenticated client. A
  * business request rejected with `Unauthorized` is replayed once, including non-idempotent methods; request
  * entities must be repeatable and the server must authenticate before application side effects occur.
  *
  * @param client
  *   the HTTP client resource used to fetch tokens
  * @param credential
  *   the client credentials including optional scopes
  * @param authentication
  *   token-endpoint client authentication method
  * @tparam F
  *   effect type with `Async`
  * @return
  *   a `Login[F]` instance that can wrap a client to perform authenticated requests
  */
def clientCredentials[F[_]: Async](
  client: Resource[F, Client[F]],
  credential: ClientCredentials,
  authentication: ClientAuthentication
): Login[F] =
  new ClientCredentialsAuth[F](credential, client, authentication)

/** Creates a `Login` instance using OAuth 2.0 Authorization Code flow.
  *
  * Exchanges the authorization code for an access token and attaches it to requests. A positive `expires_in`
  * schedules renewal before expiry when the server supplies a `refresh_token`; an omitted or non-positive
  * `expires_in`, or an omitted `refresh_token`, disables scheduled renewal. A rejected token without a
  * refresh token requires the user to authorize again.
  *
  * The returned `Login` permits one resource acquisition because authorization codes are single-use. Once
  * acquisition starts, another acquisition fails even if the exchange fails, because the server may already
  * have consumed the code.
  *
  * Each token-endpoint request is attempted once by this layer. Configure retries and failure observability
  * on `client` when needed. Token exchanges use `POST`, and authorization codes are single-use, so a retry
  * policy must account for the possibility that the server consumed the code before the response was lost.
  *
  * Example usage:
  * {{{
  *   import cats.effect.IO
  *   import org.http4s.client.Client
  *   import com.github.chenharryhua.nanjin.http.client.auth
  *
  *   val clientResource: Resource[IO, Client[IO]] = ???
  *   val credential: AuthorizationCode =
  *     AuthorizationCode(auth_endpoint, client_id, client_secret, code, redirect_uri)
  *
  *   val login: Login[IO] = auth.authorizationCode(clientResource, credential)
  * }}}
  *
  * @param client
  *   the HTTP client resource used to fetch tokens
  * @param credential
  *   the authorization code credentials and redirect URI
  * @tparam F
  *   effect type with `Async`
  * @return
  *   a single-use `Login[F]` instance that can wrap a client to perform authenticated requests
  */
def authorizationCode[F[_]: Async](
  client: Resource[F, Client[F]],
  credential: AuthorizationCode
): Login[F] =
  new AuthorizationCodeAuth[F](credential, client)
