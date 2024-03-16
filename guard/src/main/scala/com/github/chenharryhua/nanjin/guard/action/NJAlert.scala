package com.github.chenharryhua.nanjin.guard.action

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{
  AlertLevel,
  Category,
  CounterKind,
  MetricID,
  MetricName,
  ServiceParams
}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceAlert
import fs2.concurrent.Channel
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

final class NJAlert[F[_]: Monad: Clock] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  dispatcher: Dispatcher[F],
  isCounting: Boolean
) {
  private lazy val errorCounter: Counter =
    metricRegistry.counter(MetricID(name, Category.Counter(CounterKind.AlertError)).identifier)
  private lazy val warnCounter: Counter =
    metricRegistry.counter(MetricID(name, Category.Counter(CounterKind.AlertWarn)).identifier)
  private lazy val infoCounter: Counter =
    metricRegistry.counter(MetricID(name, Category.Counter(CounterKind.AlertInfo)).identifier)

  private def alert(msg: Json, alertLevel: AlertLevel): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        ServiceAlert(
          metricName = name,
          timestamp = ts,
          serviceParams = serviceParams,
          alertLevel = alertLevel,
          message = msg))
    } yield ()

  def counted: NJAlert[F] =
    new NJAlert[F](name, metricRegistry, channel, serviceParams, dispatcher, true)

  def error[S: Encoder](msg: S): F[Unit] =
    alert(msg.asJson, AlertLevel.Error).map(_ => if (isCounting) errorCounter.inc(1))
  def error[S: Encoder](msg: Option[S]): F[Unit] = msg.traverse(error(_)).void

  def unsafeError[S: Encoder](msg: S): Unit         = dispatcher.unsafeRunSync(error(msg))
  def unsafeError[S: Encoder](msg: Option[S]): Unit = dispatcher.unsafeRunSync(error(msg))

  def warn[S: Encoder](msg: S): F[Unit] =
    alert(msg.asJson, AlertLevel.Warn).map(_ => if (isCounting) warnCounter.inc(1))
  def warn[S: Encoder](msg: Option[S]): F[Unit] = msg.traverse(warn(_)).void

  def unsafeWarn[S: Encoder](msg: S): Unit         = dispatcher.unsafeRunSync(warn(msg))
  def unsafeWarn[S: Encoder](msg: Option[S]): Unit = dispatcher.unsafeRunSync(warn(msg))

  def info[S: Encoder](msg: S): F[Unit] =
    alert(msg.asJson, AlertLevel.Info).map(_ => if (isCounting) infoCounter.inc(1))
  def info[S: Encoder](msg: Option[S]): F[Unit] = msg.traverse(info(_)).void

  def unsafeInfo[S: Encoder](msg: S): Unit         = dispatcher.unsafeRunSync(info(msg))
  def unsafeInfo[S: Encoder](msg: Option[S]): Unit = dispatcher.unsafeRunSync(info(msg))
}
