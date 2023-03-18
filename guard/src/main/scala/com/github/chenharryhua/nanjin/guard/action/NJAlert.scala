package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Clock
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import cats.{Monad, Show}
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{AlertLevel, MeasurementName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID, NJEvent}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.InstantAlert
import fs2.concurrent.Channel
import io.circe.syntax.EncoderOps

final class NJAlert[F[_]: Monad: Clock] private[guard] (
  name: MeasurementName,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  isCounting: Boolean,
  dispatcher: Dispatcher[F]
) {
  private lazy val errorCounter: Counter =
    metricRegistry.counter(MetricID(name, MetricCategory.AlertErrorCounter).asJson.noSpaces)
  private lazy val warnCounter: Counter =
    metricRegistry.counter(MetricID(name, MetricCategory.AlertWarnCounter).asJson.noSpaces)
  private lazy val infoCounter: Counter =
    metricRegistry.counter(MetricID(name, MetricCategory.AlertInfoCounter).asJson.noSpaces)

  private def alert(msg: String, alertLevel: AlertLevel): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        InstantAlert(
          name = name,
          timestamp = ts,
          serviceParams = serviceParams,
          alertLevel = alertLevel,
          message = msg))
    } yield ()

  def withCounting: NJAlert[F] =
    new NJAlert[F](name, metricRegistry, channel, serviceParams, true, dispatcher)

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
