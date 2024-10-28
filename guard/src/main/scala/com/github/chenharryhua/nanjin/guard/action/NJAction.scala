package com.github.chenharryhua.nanjin.guard.action

import cats.Endo
import cats.data.{Kleisli, Reader}
import cats.effect.kernel.{Async, Resource}
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import com.github.chenharryhua.nanjin.guard.config.{MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.concurrent.Channel
import io.circe.Json
import org.typelevel.cats.time.instances.localdatetime

import java.time.temporal.ChronoUnit

final class NJAction[F[_]: Async] private[guard] (
  metricName: MetricName,
  serviceParams: ServiceParams,
  channel: Channel[F, NJEvent],
  policy: Policy
) {
  // retries
  def apply[A, Z](kleisli: Kleisli[F, A, Z]): BuildWith[F, A, Z] =
    new BuildWith[F, A, Z](
      metricName = metricName,
      channel = channel,
      serviceParams = serviceParams,
      arrow = kleisli,
      policy = policy
    )

  def apply[Z](fz: => F[Z]): BuildWith[F, Unit, Z] =
    apply(Kleisli((_: Unit) => fz))

  def apply[A, Z](f: A => F[Z]): BuildWith[F, A, Z] =
    apply(Kleisli(f))

  def apply[A, B, Z](f: (A, B) => F[Z]): BuildWith[F, (A, B), Z] =
    apply(Kleisli(f.tupled))

  def apply[A, B, C, Z](f: (A, B, C) => F[Z]): BuildWith[F, (A, B, C), Z] =
    apply(Kleisli(f.tupled))

  def apply[A, B, C, D, Z](f: (A, B, C, D) => F[Z]): BuildWith[F, (A, B, C, D), Z] =
    apply(Kleisli(f.tupled))

  def apply[A, B, C, D, E, Z](f: (A, B, C, D, E) => F[Z]): BuildWith[F, (A, B, C, D, E), Z] =
    apply(Kleisli(f.tupled))
}

final class BuildWith[F[_]: Async, IN, OUT] private[action] (
  metricName: MetricName,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  arrow: Kleisli[F, IN, OUT],
  policy: Policy
) extends localdatetime {

  private val init: BuildWith.Builder[F, IN, OUT] =
    new BuildWith.Builder[F, IN, OUT](
      transError = Reader[(Tick, IN, Throwable), Json] { case (tick, _, _) =>
        val wakeup = tick.zonedWakeup.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show
        val when   = tick.zonedAcquire.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show
        Json.obj(
          "index" -> Json.fromLong(tick.index),
          "when" -> Json.fromString(when),
          "retry" -> Json.fromString(wakeup))
      },
      isWorthRetry = Reader[(IN, Throwable), Boolean](_ => true)
    )

  def buildWith(f: Endo[BuildWith.Builder[F, IN, OUT]]): Resource[F, Kleisli[F, IN, OUT]] =
    f(init).build(metricName, channel, serviceParams, arrow, policy)

  val build: Resource[F, Kleisli[F, IN, OUT]] =
    init.build(metricName, channel, serviceParams, arrow, policy)
}

private object BuildWith {
  final class Builder[F[_]: Async, IN, OUT] private[action] (
    transError: Reader[(Tick, IN, Throwable), Json],
    isWorthRetry: Reader[(IN, Throwable), Boolean]
  ) {
    def tapError(f: (Tick, IN, Throwable) => Json): Builder[F, IN, OUT] =
      new Builder[F, IN, OUT](transError = Reader(f.tupled), isWorthRetry)

    def worthRetry(f: (IN, Throwable) => Boolean): Builder[F, IN, OUT] =
      new Builder[F, IN, OUT](transError, isWorthRetry = Reader(f.tupled))

    def worthRetry(f: Throwable => Boolean): Builder[F, IN, OUT] =
      new Builder[F, IN, OUT](transError, isWorthRetry = Reader(f).local((tup: (IN, Throwable)) => tup._2))

    private[action] def build(
      metricName: MetricName,
      channel: Channel[F, NJEvent],
      serviceParams: ServiceParams,
      arrow: Kleisli[F, IN, OUT],
      policy: Policy): Resource[F, Kleisli[F, IN, OUT]] =
      ActionRetry(
        channel = channel,
        serviceParams = serviceParams,
        metricName = metricName,
        zerothTickStatus = serviceParams.initialStatus.renewPolicy(policy),
        arrow = arrow,
        transError = transError,
        isWorthRetry = isWorthRetry
      )
  }
}
