package com.github.chenharryhua.nanjin.http.client.auth

import cats.effect.kernel.{Async, Resource}
import fs2.Stream
import org.http4s.client.Client

trait Login[F[_], A] {
  def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]]
  final def loginS(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] =
    Stream.resource(loginR(client))

  // compose middleware
  def withMiddleware(f: Client[F] => Client[F]): A
}
