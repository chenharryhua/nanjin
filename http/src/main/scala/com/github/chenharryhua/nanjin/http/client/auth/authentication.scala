package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.Resource
import cats.effect.kernel.Async
import cats.effect.std.{SecureRandom, UUIDGen}
import cats.syntax.functor.given
import org.http4s.client.Client

/** Creates a `Login` instance using OAuth 2.0 Client Credentials flow.
  *
  * Automatically handles token acquisition, attaching the access token to requests, and optional token
  * renewal.
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
  *   val login: Resource[IO, Login[IO]] = auth.clientCredentials(clientResource, credentials)
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
): Resource[F, Login[F]] =
  Resource.eval(SecureRandom.javaSecuritySecureRandom[F].map { implicit sr =>
    new ClientCredentialsAuth[F](credential, client, UUIDGen.randomUUID)
  })

/** Creates a `Login` instance using OAuth 2.0 Authorization Code flow.
  *
  * Automatically exchanges the authorization code for an access token, attaches it to requests, and handles
  * token refresh if applicable.
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
  *   val login: Resource[IO, Login[IO]] = auth.authorizationCode(clientResource, credential)
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
): Resource[F, Login[F]] =
  Resource.eval(SecureRandom.javaSecuritySecureRandom[F].map { implicit sr =>
    new AuthorizationCodeAuth[F](credential, client, UUIDGen.randomUUID)
  })
