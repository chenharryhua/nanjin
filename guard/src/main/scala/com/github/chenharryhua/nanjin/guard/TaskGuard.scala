package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync
import fs2.Stream
import monocle.macros.Lenses
import retry.Sleep
import cats.syntax.all._
import scala.concurrent.duration.FiniteDuration

@Lenses final case class TaskGuard[F[_]](
  notifiables: Set[Notifiable[F]],
  alterEveryNRetries: AlertEveryNRetries,
  maximumRetries: MaximumRetries,
  retryInterval: RetryInterval) {

  def withNotifiable(noti: Notifiable[F]): TaskGuard[F] =
    TaskGuard.notifiables.modify((ns: Set[Notifiable[F]]) => ns + noti)(this)

  def withAlertEveryNRetries(v: Int): TaskGuard[F] =
    TaskGuard.alterEveryNRetries.modify(_.copy(value = v))(this)

  def withMaximumRetries(v: Long): TaskGuard[F] =
    TaskGuard.maximumRetries.modify(_.copy(value = v))(this)

  def withRetryInterval(dur: FiniteDuration): TaskGuard[F] =
    TaskGuard.retryInterval.modify(_.copy(value = dur))(this)

  def info() = {}
  def warn = {}
  def error = {}

  def forever[A](action: F[A])(implicit F: Sync[F], sleep: Sleep[F]) =
    new RetryForever[F](retryInterval, alterEveryNRetries).forever(action).run { rfs =>
      notifiables.toList.traverse(_.error).void
    }

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Sync[F], sleep: Sleep[F]) =
    new RetryForever[F](retryInterval, alterEveryNRetries).infiniteStream(stream).run(rfs => F.unit)

}
