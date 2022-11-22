package com.github.chenharryhua.nanjin.guard.action

import cats.{Monad, Show}
import cats.effect.kernel.Clock
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Digested, Importance, ServiceParams}
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
  private lazy val errorCounter: Counter = metricRegistry.counter(alertMRName(digested, Importance.Critical))
  private lazy val warnCounter: Counter  = metricRegistry.counter(alertMRName(digested, Importance.Notice))
  private lazy val infoCounter: Counter  = metricRegistry.counter(alertMRName(digested, Importance.Silent))
  private lazy val debugCounter: Counter = metricRegistry.counter(alertMRName(digested, Importance.Trivial))

  private def alert(msg: String, importance: Importance): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        InstantAlert(
          digested = digested,
          timestamp = ts,
          serviceParams = serviceParams,
          importance = importance,
          message = msg))
    } yield ()

  def withCounting: NJAlert[F] =
    new NJAlert[F](digested, metricRegistry, channel, serviceParams, true, dispatcher)

  def error[S: Show](msg: S): F[Unit] =
    alert(msg.show, Importance.Critical).map(_ => if (isCounting) errorCounter.inc(1))
  def error[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(error(_)).void
  def unsafeError[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(error(msg))
  def unsafeError[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(error(msg))

  def warn[S: Show](msg: S): F[Unit] =
    alert(msg.show, Importance.Notice).map(_ => if (isCounting) warnCounter.inc(1))
  def warn[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(warn(_)).void
  def unsafeWarn[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(warn(msg))
  def unsafeWarn[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(warn(msg))

  def info[S: Show](msg: S): F[Unit] =
    alert(msg.show, Importance.Silent).map(_ => if (isCounting) infoCounter.inc(1))
  def info[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(info(_)).void
  def unsafeInfo[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(info(msg))
  def unsafeInfo[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(info(msg))

  def debug[S: Show](msg: S): F[Unit] =
    alert(msg.show, Importance.Trivial).map(_ => if (isCounting) debugCounter.inc(1))
  def debug[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(debug(_)).void
  def unsafeDebug[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(debug(msg))
  def unsafeDebug[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(debug(msg))
}
