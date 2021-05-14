package com.github.chenharryhua.nanjin.guard

import monocle.macros.Lenses

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

  def withMaximumRetries(v: Int): TaskGuard[F] =
    TaskGuard.maximumRetries.modify(_.copy(value = v))(this)

  def withRetryInterval(dur: FiniteDuration): TaskGuard[F] =
    TaskGuard.retryInterval.modify(_.copy(value = dur))(this)

}
