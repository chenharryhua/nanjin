package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Concurrent, Ref, Resource}
import cats.implicits.toFlatMapOps
import fs2.Stream
import org.http4s.client.Client
import org.http4s.Request

/** @tparam A
  *   Implementation class
  */

trait Login[F[_], A] {

  def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]]

  final def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]] =
    login(client).compile.resource.lastOrError

  def withMiddleware(f: Client[F] => Client[F]): A

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
  )(implicit F: Concurrent[F]): Stream[F, Client[F]] =
    Stream.eval(getToken.flatMap(F.ref)).flatMap { refToken =>
      val nc: Client[F] =
        Client[F] { req =>
          for {
            token <- Resource.eval(refToken.get)
            out <- client.run(withToken(token, req))
          } yield out
        }
      Stream(nc).concurrently(Stream.repeatEval(updateToken(refToken)))
    }
}
