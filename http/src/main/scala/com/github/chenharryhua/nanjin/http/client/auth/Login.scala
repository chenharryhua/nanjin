package com.github.chenharryhua.nanjin.http.client.auth

import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import fs2.Stream
import org.http4s.client.Client

import scala.concurrent.duration.FiniteDuration

trait Login[F[_], A] {
  def loginR(client: Client[F])(implicit F: Async[F]): Resource[F, Client[F]]
  final def loginS(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]] =
    Stream.resource(loginR(client))

  final protected def compose(
    f: Client[F] => Resource[F, Client[F]],
    g: Reader[Client[F], Resource[F, Client[F]]]): Reader[Client[F], Resource[F, Client[F]]] =
    Reader((a: Client[F]) => f(a).flatMap(g.run))

  /** the order of apply withMiddleware matters:
    *
    * {{{
    *   cred.withMiddleware(cookieBox(cm)).withMiddleware(logHead) // show cookies
    *   cred.withMiddleware(logHead).withMiddleware(cookieBox(cm)) // not show cookies
    *
    * }}}
    */
  def withMiddlewareR(f: Client[F] => Resource[F, Client[F]]): A
  final def withMiddlewareM(f: Client[F] => F[Client[F]]): A = withMiddlewareR(a => Resource.eval(f(a)))
  final def withMiddleware(f: Client[F] => Client[F]): A     = withMiddlewareR(a => Resource.pure(f(a)))
}

private[auth] trait IsExpirableToken[A] {
  def expiresIn(a: A): FiniteDuration
}
