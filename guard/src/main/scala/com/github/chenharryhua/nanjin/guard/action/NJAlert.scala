package com.github.chenharryhua.nanjin.guard.action

import cats.Show
import cats.effect.kernel.Temporal
import cats.effect.std.Dispatcher
import cats.syntax.functor.*
import cats.syntax.show.*
import cats.syntax.traverse.*
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{CountAction, Digested, Importance, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.concurrent.Channel

final class NJAlert[F[_]: Temporal] private[guard] (
  metricName: Digested,
  dispatcher: Dispatcher[F],
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  isCounting: CountAction
) {
  private val publisher: InstantEventPublisher[F] = new InstantEventPublisher[F](channel, serviceParams)
  private lazy val errorCounter: Counter          = metricRegistry.counter(alertMRName(metricName, Importance.Critical))
  private lazy val warnCounter: Counter           = metricRegistry.counter(alertMRName(metricName, Importance.High))
  private lazy val infoCounter: Counter           = metricRegistry.counter(alertMRName(metricName, Importance.Medium))

  def withCounting: NJAlert[F] =
    new NJAlert[F](metricName, dispatcher, metricRegistry, channel, serviceParams, CountAction.Yes)

  def error[S: Show](msg: S): F[Unit] =
    publisher.alert(metricName, msg.show, Importance.Critical).map(_ => if (isCounting.value) errorCounter.inc(1))
  def error[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(error(_)).void
  def unsafeError[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(error(msg))
  def unsafeError[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(error(msg))

  def warn[S: Show](msg: S): F[Unit] =
    publisher.alert(metricName, msg.show, Importance.High).map(_ => if (isCounting.value) warnCounter.inc(1))
  def warn[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(warn(_)).void
  def unsafeWarn[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(warn(msg))
  def unsafeWarn[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(warn(msg))

  def info[S: Show](msg: S): F[Unit] =
    publisher.alert(metricName, msg.show, Importance.Medium).map(_ => if (isCounting.value) infoCounter.inc(1))
  def info[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(info(_)).void
  def unsafeInfo[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(info(msg))
  def unsafeInfo[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(info(msg))
}
