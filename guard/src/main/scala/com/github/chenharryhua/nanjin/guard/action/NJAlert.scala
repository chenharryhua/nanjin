package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Clock
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import cats.{Monad, Show}
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{AlertLevel, Digested, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.InstantAlert
import fs2.concurrent.Channel

final class NJAlert[F[_]: Monad: Clock] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  isCounting: Boolean,
  dispatcher: Dispatcher[F]
) {
  private lazy val errorCounter: Counter = metricRegistry.counter(alertMRName(digested, AlertLevel.Error))
  private lazy val warnCounter: Counter  = metricRegistry.counter(alertMRName(digested, AlertLevel.Warn))
  private lazy val infoCounter: Counter  = metricRegistry.counter(alertMRName(digested, AlertLevel.Info))

  private def alert(msg: String, alertLevel: AlertLevel): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        InstantAlert(
          digested = digested,
          timestamp = ts,
          serviceParams = serviceParams,
          alertLevel = alertLevel,
          message = msg))
    } yield ()

  def withCounting: NJAlert[F] =
    new NJAlert[F](digested, metricRegistry, channel, serviceParams, true, dispatcher)

  def error[S: Show](msg: S): F[Unit] =
    alert(msg.show, AlertLevel.Error).map(_ => if (isCounting) errorCounter.inc(1))
  def error[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(error(_)).void
  def unsafeError[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(error(msg))
  def unsafeError[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(error(msg))

  def warn[S: Show](msg: S): F[Unit] =
    alert(msg.show, AlertLevel.Warn).map(_ => if (isCounting) warnCounter.inc(1))
  def warn[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(warn(_)).void
  def unsafeWarn[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(warn(msg))
  def unsafeWarn[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(warn(msg))

  def info[S: Show](msg: S): F[Unit] =
    alert(msg.show, AlertLevel.Info).map(_ => if (isCounting) infoCounter.inc(1))
  def info[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(info(_)).void
  def unsafeInfo[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(info(msg))
  def unsafeInfo[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(info(msg))
}
