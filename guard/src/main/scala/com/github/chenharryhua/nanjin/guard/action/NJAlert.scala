package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.Dispatcher
import cats.syntax.functor.*
import cats.syntax.show.*
import cats.syntax.traverse.*
import cats.{Applicative, Show}
import com.github.chenharryhua.nanjin.guard.config.{Importance, MetricName}
import com.github.chenharryhua.nanjin.guard.event.EventPublisher

final class NJAlert[F[_]: Applicative](
  metricName: MetricName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F]) {

  def error[S: Show](msg: S): F[Unit]            = eventPublisher.alert(metricName, msg.show, Importance.Critical)
  def error[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(error(_)).void
  def unsafeError[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(error(msg))
  def unsafeError[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(error(msg))

  def warn[S: Show](msg: S): F[Unit]            = eventPublisher.alert(metricName, msg.show, Importance.High)
  def warn[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(warn(_)).void
  def unsafeWarn[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(warn(msg))
  def unsafeWarn[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(warn(msg))

  def info[S: Show](msg: S): F[Unit]            = eventPublisher.alert(metricName, msg.show, Importance.Medium)
  def info[S: Show](msg: Option[S]): F[Unit]    = msg.traverse(info(_)).void
  def unsafeInfo[S: Show](msg: S): Unit         = dispatcher.unsafeRunSync(info(msg))
  def unsafeInfo[S: Show](msg: Option[S]): Unit = dispatcher.unsafeRunSync(info(msg))
}
