package com.github.chenharryhua.nanjin.http.client

import cats.effect.kernel.{Concurrent, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import org.http4s.client.Client
import org.http4s.Request

package object auth {
  def buildClient[F[_], A](
    client: Client[F],
    initToken: F[A],
    refreshToken: Ref[F, A] => F[Unit],
    withToken: (A, Request[F]) => Request[F])(implicit F: Concurrent[F]): Resource[F, Client[F]] =
    Stream
      .eval(initToken.flatMap(F.ref))
      .flatMap { refToken =>
        val nc: Client[F] =
          Client[F] { req =>
            for {
              token <- Resource.eval(refToken.get)
              out <- client.run(withToken(token, req))
            } yield out
          }
        Stream(nc).concurrently(Stream.repeatEval(refreshToken(refToken)))
      }
      .compile
      .resource
      .lastOrError
}
