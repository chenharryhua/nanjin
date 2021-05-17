package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync
import cats.syntax.all._
import fs2.Stream
import monocle.macros.Lenses
import retry.Sleep

import scala.concurrent.duration._
import cats.effect.kernel.Async

@Lenses final case class TaskGuard[F[_]](
  loggers: List[LogService[F]],
  alterEveryNRetries: AlertEveryNRetries,
  maximumRetries: MaximumRetries,
  retryInterval: RetryInterval) {

  def withLogService(noti: LogService[F]): TaskGuard[F] =
    TaskGuard.loggers.modify((ns: List[LogService[F]]) => noti :: ns)(this)

  def withAlertEveryNRetries(v: Int): TaskGuard[F] =
    TaskGuard.alterEveryNRetries.modify(_.copy(value = v))(this)

  def withMaximumRetries(v: Long): TaskGuard[F] =
    TaskGuard.maximumRetries.modify(_.copy(value = v))(this)

  def withRetryInterval(dur: FiniteDuration): TaskGuard[F] =
    TaskGuard.retryInterval.modify(_.copy(value = dur))(this)

  def info(msg: String)(implicit F: Sync[F]): F[Unit]                 = loggers.traverse(_.info(msg)).void
  def warn(msg: String, ex: Throwable)(implicit F: Sync[F]): F[Unit]  = loggers.traverse(_.warn(msg, ex)).void
  def error(msg: String, ex: Throwable)(implicit F: Sync[F]): F[Unit] = loggers.traverse(_.error(msg, ex)).void

  def forever[A](action: F[A])(implicit F: Sync[F], sleep: Sleep[F]): F[A] =
    new RetryForever[F](retryInterval, alterEveryNRetries).forever(action).run { rfs =>
      loggers.traverse(_.retryForeverError(rfs)).void
    }

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F], sleep: Sleep[F]): F[Unit] =
    new RetryForever[F](retryInterval, alterEveryNRetries)
      .infiniteStream(stream)
      .run(rfs => loggers.traverse(_.retryForeverError(rfs)).void)
}

object TaskGuard {

  def apply[F[_]]: TaskGuard[F] =
    TaskGuard[F](List.empty, AlertEveryNRetries(10), MaximumRetries(10), RetryInterval(10.seconds))
}
