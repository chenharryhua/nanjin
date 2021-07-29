package com.github.chenharryhua.nanjin.http.client.auth

import cats.Monad
import cats.data.Kleisli
import cats.effect.kernel.{Async, Resource}
import fs2.Stream
import org.http4s.client.Client

import scala.concurrent.duration.FiniteDuration

trait Login[F[_], A] {
  def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]]
  final def loginS(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] = Stream.resource(loginR(client))

  final protected def compose(f: Client[F] => F[Client[F]], g: Kleisli[F, Client[F], Client[F]])(implicit
    F: Monad[F]): Kleisli[F, Client[F], Client[F]] =
    Kleisli[F, Client[F], Client[F]](c => F.flatMap(f(c))(g.run))

  def withMiddlewareM(f: Client[F] => F[Client[F]])(implicit F: Monad[F]): A
  final def withMiddleware(f: Client[F] => Client[F])(implicit F: Monad[F]): A =
    withMiddlewareM(a => F.pure(f(a)))
}

private[auth] trait IsExpirableToken[A] {
  def expiresIn(a: A): FiniteDuration
}
