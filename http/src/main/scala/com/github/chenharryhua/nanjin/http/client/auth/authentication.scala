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
  * Each token-endpoint request is attempted once by this layer. Configure retries and failure observability
  * on `client` when needed. Token exchanges use `POST`, so use `recklessHttpRetry` or a custom `httpRetry`
  * predicate only when repeating the exchange is acceptable.
  *
  * Example usage:
  * {{{
  *   import cats.effect.{IO, Resource}
  *   import org.http4s.client.Client
  *   import com.github.chenharryhua.nanjin.http.client.auth
  *
  *   val clientResource: Resource[IO, Client[IO]] = ???
  *   val credentials: ClientCredentials =
  *     ClientCredentials(auth_endpoint, client_id, client_secret)
  *
  *   val login: Login[IO] = auth.clientCredentials(clientResource, credentials)
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
