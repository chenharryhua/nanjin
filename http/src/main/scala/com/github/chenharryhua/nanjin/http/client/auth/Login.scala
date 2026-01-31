package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import fs2.Stream
import org.http4s.client.Client
import org.http4s.{Request, Response, Status}

trait Login[F[_]] {

  def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]]

  final def loginR(client: Resource[F, Client[F]])(implicit F: Async[F]): Resource[F, Client[F]] =
    client.flatMap(loginR)

  final def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] =
    Stream.resource(loginR(client))

  final def login(client: Resource[F, Client[F]])(implicit F: Async[F]): Stream[F, Client[F]] =
    Stream.resource(loginR(client))

  /** @param client
    *   http4s client
    * @param getToken
    *   get the initial token
    * @param updateToken
    *   update the token periodically according to the expiresIn parameter
    * @param withToken
    *   add token to header
    * @tparam T
    *   Token
    * @return
    */

  final protected def login_internal[T](
    client: Client[F],
    getToken: F[T],
    updateToken: Ref[F, T] => F[Unit],
    withToken: (T, Request[F]) => Request[F]
  )(implicit F: Async[F]): Resource[F, Client[F]] =
    for {
      authToken <- Resource.eval(getToken.flatMap(F.ref))
      _ <- F.background[Nothing](updateToken(authToken).attempt.foreverM)
      refreshLock <- Resource.eval(Semaphore[F](1))
    } yield Client[F] { req =>
      def runWithToken(token: T): Resource[F, Response[F]] =
        client.run(withToken(token, req))

      Resource.eval(authToken.get).flatMap { token =>
        runWithToken(token).flatMap { response =>
          if (response.status === Status.Unauthorized) {
            // Retry once with refreshed token, single-refresh protected by semaphore
            Resource
              .eval(refreshLock.permit.use { _ =>
                getToken.flatTap(authToken.set)
              })
              .flatMap(runWithToken)
          } else Resource.pure(response)
        }
      }
    }
}
