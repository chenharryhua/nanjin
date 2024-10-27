package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.concurrent.Channel

import scala.concurrent.Future

final class NJAction[F[_]: Async] private[guard] (
  metricName: MetricName,
  serviceParams: ServiceParams,
  channel: Channel[F, NJEvent]
) {

  private val F = Async[F]

  // retries
  def retry[A, Z](kleisli: Kleisli[F, A, Z]): BuildWith[F, A, Z] =
    new BuildWith[F, A, Z](
      metricName = metricName,
      channel = channel,
      serviceParams = serviceParams,
      arrow = kleisli
    )

  def retry[Z](fz: => F[Z]): BuildWith[F, Unit, Z] =
    retry(Kleisli((_: Unit) => fz))

  def delay[Z](z: => Z): BuildWith[F, Unit, Z] =
    retry(Kleisli((_: Unit) => F.delay(z)))

  def retry[A, Z](f: A => F[Z]): BuildWith[F, A, Z] =
    retry(Kleisli(f))

  def retry[A, B, Z](f: (A, B) => F[Z]): BuildWith[F, (A, B), Z] =
    retry(Kleisli(f.tupled))

  def retry[A, B, C, Z](f: (A, B, C) => F[Z]): BuildWith[F, (A, B, C), Z] =
    retry(Kleisli(f.tupled))

  def retry[A, B, C, D, Z](f: (A, B, C, D) => F[Z]): BuildWith[F, (A, B, C, D), Z] =
    retry(Kleisli(f.tupled))

  def retry[A, B, C, D, E, Z](f: (A, B, C, D, E) => F[Z]): BuildWith[F, (A, B, C, D, E), Z] =
    retry(Kleisli(f.tupled))

  // future
  def retryFuture[Z](future: => F[Future[Z]]): BuildWith[F, Unit, Z] =
    retry(F.fromFuture(future))

  def retryFuture[A, Z](f: A => Future[Z]): BuildWith[F, A, Z] =
    retry((a: A) => F.fromFuture(F.delay(f(a))))

  def retryFuture[A, B, Z](f: (A, B) => Future[Z]): BuildWith[F, (A, B), Z] =
    retry((a: A, b: B) => F.fromFuture(F.delay(f(a, b))))

  def retryFuture[A, B, C, Z](f: (A, B, C) => Future[Z]): BuildWith[F, (A, B, C), Z] =
    retry((a: A, b: B, c: C) => F.fromFuture(F.delay(f(a, b, c))))

  def retryFuture[A, B, C, D, Z](f: (A, B, C, D) => Future[Z]): BuildWith[F, (A, B, C, D), Z] =
    retry((a: A, b: B, c: C, d: D) => F.fromFuture(F.delay(f(a, b, c, d))))

  def retryFuture[A, B, C, D, E, Z](f: (A, B, C, D, E) => Future[Z]): BuildWith[F, (A, B, C, D, E), Z] =
    retry((a: A, b: B, c: C, d: D, e: E) => F.fromFuture(F.delay(f(a, b, c, d, e))))
}
