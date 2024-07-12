package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import org.http4s.client.Client
import org.http4s.{Request, Status}

/** @tparam A
  *   Implementation class
  */

trait Login[F[_], A] {

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

  final protected def loginInternal[T](
    client: Client[F],
    getToken: F[T],
    updateToken: Ref[F, T] => F[Unit],
    withToken: (T, Request[F]) => Request[F]
  )(implicit F: Async[F]): Resource[F, Client[F]] =
    for {
      authToken <- Resource.eval(getToken.flatMap(F.ref))
      _ <- F.background[Nothing](updateToken(authToken).attempt.foreverM)
    } yield Client[F] { req =>
      for {
        token <- Resource.eval(authToken.get)
        out <- client.run(withToken(token, req)).flatMap { response =>
          if (response.status === Status.Unauthorized) {
            Resource
              .eval(getToken) // retrieve token in case token was expired
              .evalTap(authToken.set) // update cache if success
              .flatMap(tkn => client.run(withToken(tkn, req))) // re-run the request using new token
          } else Resource.pure(response)
        }
      } yield out
    }
}
