package com.github.chenharryhua.nanjin.guard.action

import cats.{Monad, Show}
import cats.effect.kernel.Clock
import cats.syntax.functor.*
import cats.syntax.show.*
import cats.syntax.traverse.*
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Digested, Importance, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.concurrent.Channel

final class NJAlert[F[_]: Monad: Clock] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  isCounting: Boolean
) {
  private lazy val errorCounter: Counter =
    metricRegistry.counter(alertMRName(digested, Importance.Critical))
  private lazy val warnCounter: Counter = metricRegistry.counter(alertMRName(digested, Importance.High))
  private lazy val infoCounter: Counter = metricRegistry.counter(alertMRName(digested, Importance.Medium))

  def withCounting: NJAlert[F] =
    new NJAlert[F](digested, metricRegistry, channel, serviceParams, true)

  def error[S: Show](msg: S): F[Unit] =
    publisher
      .alert(channel, serviceParams, digested, msg.show, Importance.Critical)
      .map(_ => if (isCounting) errorCounter.inc(1))

  def error[S: Show](msg: Option[S]): F[Unit] = msg.traverse(error(_)).void

  def warn[S: Show](msg: S): F[Unit] =
    publisher
      .alert(channel, serviceParams, digested, msg.show, Importance.High)
      .map(_ => if (isCounting) warnCounter.inc(1))

  def warn[S: Show](msg: Option[S]): F[Unit] = msg.traverse(warn(_)).void

  def info[S: Show](msg: S): F[Unit] =
    publisher
      .alert(channel, serviceParams, digested, msg.show, Importance.Medium)
      .map(_ => if (isCounting) infoCounter.inc(1))

  def info[S: Show](msg: Option[S]): F[Unit] = msg.traverse(info(_)).void
}
