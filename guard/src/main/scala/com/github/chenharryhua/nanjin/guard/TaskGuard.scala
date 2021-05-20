package com.github.chenharryhua.nanjin.guard

import cats.Show
import cats.effect.Async
import fs2.Stream

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._

final class TaskGuard[F[_]] private (
  alertServices: List[AlertService[F]],
  serviceConfig: ServiceConfig,
  actionConfig: ActionConfig
) {

  // config
  private def updateS(f: ServiceConfig => ServiceConfig): TaskGuard[F] =
    new TaskGuard[F](alertServices, f(serviceConfig), actionConfig)

  private def updateA(f: ActionConfig => ActionConfig): TaskGuard[F] =
    new TaskGuard[F](alertServices, serviceConfig, f(actionConfig))

  def addAlertService(value: AlertService[F]): TaskGuard[F] =
    new TaskGuard[F](value :: alertServices, serviceConfig, actionConfig)

  def withServiceName(value: String): TaskGuard[F] =
    updateS(_.withServiceName(value)).updateA(_.withServiceName(value))

  def withRestartInterval(value: FiniteDuration): TaskGuard[F]     = updateS(_.withRestartInterval(value))
  def withHealthCheckInterval(value: FiniteDuration): TaskGuard[F] = updateS(_.withHealthCheckInterval(value))
  def withActionMaxRetries(value: Long): TaskGuard[F]              = updateA(_.withActionMaxRetries(value))
  def withActionRetryInterval(value: FiniteDuration): TaskGuard[F] = updateA(_.withActionRetryInterval(value))
  def offAlertSucc: TaskGuard[F]                                   = updateA(_.offSucc)
  def offAlertFail: TaskGuard[F]                                   = updateA(_.offFail)

  // non-stop action
  def nonStop[A](action: F[A])(implicit F: Async[F]): F[Unit] =
    new RunForever[F](alertServices, serviceConfig).nonStop(action)

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F]): Stream[F, Unit] =
    new RunForever[F](alertServices, serviceConfig).infiniteStream(stream)

  // retry eval
  def retryEval[A: Show, B](a: A)(f: A => F[B])(implicit F: Async[F]): F[B] =
    new LimitRetry[F](alertServices, actionConfig, ActionID(UUID.randomUUID())).retryEval(a, f)

  def retryEvalFuture[A: Show, B](a: A)(f: A => Future[B])(implicit F: Async[F]): F[B] =
    retryEval[A, B](a)((a: A) => F.fromFuture(F.blocking(f(a))))

}

object TaskGuard {

  def apply[F[_]](applicationName: String): TaskGuard[F] =
    new TaskGuard[F](List(new LogService[F]), ServiceConfig(applicationName), ActionConfig(applicationName))
}
