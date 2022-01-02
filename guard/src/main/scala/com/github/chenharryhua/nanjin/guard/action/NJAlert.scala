package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.Dispatcher
import cats.syntax.functor.*
import cats.syntax.show.*
import cats.syntax.traverse.*
import cats.{Applicative, Show}
import com.codahale.metrics.Counter
import com.github.chenharryhua.nanjin.guard.config.{DigestedName, Importance}
import com.github.chenharryhua.nanjin.guard.event.EventPublisher

final class NJAlert[F[_]: Applicative] private[guard] (
  name: DigestedName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F]) {
  private lazy val errorCounter: Counter =
    eventPublisher.metricRegistry.counter(alertMRName(name, Importance.Critical))
  private lazy val warnCounter: Counter =
    eventPublisher.metricRegistry.counter(alertMRName(name, Importance.High))
  private lazy val infoCounter: Counter =
    eventPublisher.metricRegistry.counter(alertMRName(name, Importance.Medium))

  def error[S: Show](msg: S): F[Unit] =
    eventPublisher.alert(name, msg.show, Importance.Critical).map(_ => errorCounter.inc(1))
  def error[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(error(_)).void
  def unsafeError[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(error(msg))
  def unsafeError[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(error(msg))

  def warn[S: Show](msg: S): F[Unit] =
    eventPublisher.alert(name, msg.show, Importance.High).map(_ => warnCounter.inc(1))
  def warn[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(warn(_)).void
  def unsafeWarn[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(warn(msg))
  def unsafeWarn[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(warn(msg))

  def info[S: Show](msg: S): F[Unit] =
    eventPublisher.alert(name, msg.show, Importance.Medium).map(_ => infoCounter.inc(1))
  def info[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(info(_)).void
  def unsafeInfo[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(info(msg))
  def unsafeInfo[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(info(msg))
}
