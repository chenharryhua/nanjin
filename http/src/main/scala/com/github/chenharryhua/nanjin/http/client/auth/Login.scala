package com.github.chenharryhua.nanjin.http.client.auth

import cats.Monad
import cats.data.Kleisli
import cats.effect.Async
import fs2.Stream
import org.http4s.client.Client

trait Login[F[_], A] {
  def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]]

  final protected def compose(f: Client[F] => F[Client[F]], g: Kleisli[F, Client[F], Client[F]])(implicit
    F: Monad[F]): Kleisli[F, Client[F], Client[F]] =
    Kleisli[F, Client[F], Client[F]](c => F.flatMap(f(c))(g.run))

  def withMiddlewareM(f: Client[F] => F[Client[F]])(implicit F: Monad[F]): A
  final def withMiddleware(f: Client[F] => Client[F])(implicit F: Monad[F]): A =
    withMiddlewareM(a => F.pure(f(a)))
}
