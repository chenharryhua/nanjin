package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.Resource
import cats.effect.kernel.Async
import org.http4s.client.Client

/** Creates a `Login` instance using OAuth 2.0 Client Credentials flow.
  *
  * Automatically handles token acquisition, attaching the access token to requests, and optional token
  * renewal. A positive `expires_in` schedules renewal before expiry; omission, zero, or a negative value
  * disables scheduled renewal, leaving `renewOnRejection` to replace a rejected token.
  *
  * Each token-endpoint request is attempted once by this layer. Configure retries and failure observability
  * on `client` when needed. Token exchanges use `POST`, so use `recklessHttpRetry` or a custom `httpRetry`
  * predicate only when repeating the exchange is acceptable.
  *
  * Example usage:
  * {{{
  *   import cats.effect.IO
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
  new ClientCredentialsAuth[F](credential, client)

/** Creates a `Login` instance using OAuth 2.0 Authorization Code flow.
  *
  * Automatically exchanges the authorization code for an access token, attaches it to requests, and handles
  * token renewal. A positive `expires_in` schedules renewal before expiry; zero or a negative value disables
  * scheduled renewal, leaving `renewOnRejection` to replace a rejected token.
  *
  * Each token-endpoint request is attempted once by this layer. Configure retries and failure observability
  * on `client` when needed. Token exchanges use `POST`, and authorization codes are normally single-use, so a
  * retry policy must account for the possibility that the server consumed the code before the response was
  * lost.
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
  *   the authorization code credentials including optional scopes
  * @tparam F
  *   effect type with `Async`
  * @return
  *   a `Login[F]` instance that can wrap a client to perform authenticated requests
  */
def authorizationCode[F[_]: Async](
  client: Resource[F, Client[F]],
  credential: AuthorizationCode
): Login[F] =
  new AuthorizationCodeAuth[F](credential, client)
