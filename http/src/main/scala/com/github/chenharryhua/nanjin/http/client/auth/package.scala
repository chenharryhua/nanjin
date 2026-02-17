package com.github.chenharryhua.nanjin.http.client

import cats.effect.Resource
import cats.effect.kernel.Async
import cats.effect.std.{SecureRandom, UUIDGen}
import cats.syntax.functor.toFunctorOps
import org.http4s.client.Client

/** Entry points for creating OAuth 2.0 authenticators.
  *
  * This package provides convenient helper methods to create a `Login` instance from either:
  *   - `ClientCredentials` (Client Credentials flow), or
  *   - `AuthorizationCode` (Authorization Code flow)
  *
  * Example usage:
  * {{{
  *   import cats.effect.IO
  *   import fs2.Stream
  *   import org.http4s.client.Client
  *   import com.github.chenharryhua.nanjin.http.client.auth
  *
  *   val clientResource: Resource[IO, Client[IO]] = ???
  *   val credentials: ClientCredentials =
  *     ClientCredentials(auth_endpoint, client_id, client_secret)
  *
  *   // create a login instance using Client Credentials flow
  *   val login: Login[IO] = auth.clientCredentials(clientResource, credentials)
  *
  *   // obtain a stream of authenticated clients
  *   val clientStream: Stream[IO, Client[IO]] = login.login(clientResource)
  *
  *   // similarly, using Authorization Code flow
  *   val authCode: AuthorizationCode =
  *     AuthorizationCode(auth_endpoint, client_id, client_secret, code, redirect_uri)
  *   val loginAuth: Login[IO] = auth.authorizationCode(clientResource, authCode)
  * }}}
  */
package object auth {

  /** Creates a `Login` instance using OAuth 2.0 Client Credentials flow.
    *
    * Automatically handles token acquisition, attaching the access token to requests, and optional token
    * renewal.
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
}
