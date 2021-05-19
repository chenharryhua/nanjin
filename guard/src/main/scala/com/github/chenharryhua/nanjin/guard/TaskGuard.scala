package com.github.chenharryhua.nanjin.guard

import cats.Show
import cats.effect.Async
import fs2.Stream

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._

final class TaskGuard[F[_]] private (
  applicationName: ApplicationName,
  serviceName: ServiceName,
  alertServices: List[AlertService[F]],
  serviceAlertEveryNRetries: ServiceAlertEveryNRetries,
  serviceRestartInterval: ServiceRestartInterval,
  healthCheckInterval: HealthCheckInterval,
  actionMaximumRetries: ActionMaximumRetries,
  actionRetryInterval: ActionRetryInterval,
  alertLevel: AlertLevel
) {

  val getApplicationName: String = applicationName.value
  val getServiceName: String     = serviceName.value

  // config
  def withServiceName(value: String) =
    new TaskGuard[F](
      applicationName,
      ServiceName(value),
      alertServices,
      serviceAlertEveryNRetries,
      serviceRestartInterval,
      healthCheckInterval,
      actionMaximumRetries,
      actionRetryInterval,
      alertLevel)

  def addAlertService(value: AlertService[F]): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      value :: alertServices,
      serviceAlertEveryNRetries,
      serviceRestartInterval,
      healthCheckInterval,
      actionMaximumRetries,
      actionRetryInterval,
      alertLevel)

  def withServiceAlertEveryNRetries(value: Int): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      ServiceAlertEveryNRetries(value),
      serviceRestartInterval,
      healthCheckInterval,
      actionMaximumRetries,
      actionRetryInterval,
      alertLevel)

  def withServiceRestartInterval(value: FiniteDuration): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      serviceAlertEveryNRetries,
      ServiceRestartInterval(value),
      healthCheckInterval,
      actionMaximumRetries,
      actionRetryInterval,
      alertLevel)

  def withHealthCheckInterval(value: FiniteDuration): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      serviceAlertEveryNRetries,
      serviceRestartInterval,
      HealthCheckInterval(value),
      actionMaximumRetries,
      actionRetryInterval,
      alertLevel)

  def withActionMaximumRetries(value: Long): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      serviceAlertEveryNRetries,
      serviceRestartInterval,
      healthCheckInterval,
      ActionMaximumRetries(value),
      actionRetryInterval,
      alertLevel)

  def withActionRetryInterval(value: FiniteDuration): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      serviceAlertEveryNRetries,
      serviceRestartInterval,
      healthCheckInterval,
      actionMaximumRetries,
      ActionRetryInterval(value),
      alertLevel)

  def withAlertSuccOnly: TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      serviceAlertEveryNRetries,
      serviceRestartInterval,
      healthCheckInterval,
      actionMaximumRetries,
      actionRetryInterval,
      AlertSuccOnly)

  def withAlertFailOnly: TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      serviceAlertEveryNRetries,
      serviceRestartInterval,
      healthCheckInterval,
      actionMaximumRetries,
      actionRetryInterval,
      AlertFailOnly)

  // non-stop action
  def nonStopAction[A](action: F[A])(implicit F: Async[F]): F[Unit] =
    new RunForever[F](
      alertServices,
      applicationName,
      serviceName,
      serviceRestartInterval,
      serviceAlertEveryNRetries,
      healthCheckInterval).nonStopAction(action)

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F]): Stream[F, Unit] =
    new RunForever[F](
      alertServices,
      applicationName,
      serviceName,
      serviceRestartInterval,
      serviceAlertEveryNRetries,
      healthCheckInterval).infiniteStream(stream)

  // retry eval
  def retryEval[A: Show, B](a: A)(f: A => F[B])(implicit F: Async[F]): F[B] =
    new LimitRetry[F](
      alertServices,
      applicationName,
      serviceName,
      actionMaximumRetries,
      actionRetryInterval,
      ActionID(UUID.randomUUID()),
      alertLevel).retryEval(a, f)

  def retryEvalFuture[A: Show, B](a: A)(f: A => Future[B])(implicit F: Async[F]): F[B] =
    retryEval[A, B](a)((a: A) => F.fromFuture(F.blocking(f(a))))

}

object TaskGuard {

  def apply[F[_]](applicationName: String): TaskGuard[F] =
    new TaskGuard[F](
      ApplicationName(applicationName),
      ServiceName(""),
      List(new LogService[F]),
      ServiceAlertEveryNRetries(30), // 15 minutes raise an alert
      ServiceRestartInterval(30.seconds),
      HealthCheckInterval(6.hours),
      ActionMaximumRetries(3),
      ActionRetryInterval(10.seconds),
      AlertBoth
    )
}
