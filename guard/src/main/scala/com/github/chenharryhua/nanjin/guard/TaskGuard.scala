package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import fs2.Stream

import scala.concurrent.duration._

final case class ApplicationName(value: String) extends AnyVal
final case class ServiceName(value: String) extends AnyVal

final private case class AlertEveryNRetries(value: Int) extends AnyVal
final private case class MaximumRetries(value: Long) extends AnyVal
final private case class RetryInterval(value: FiniteDuration) extends AnyVal
final private case class HealthCheckInterval(value: FiniteDuration) extends AnyVal

final class TaskGuard[F[_]] private (
  val applicationName: ApplicationName,
  val serviceName: ServiceName,
  alertService: AlertService[F],
  alertEveryNRetries: AlertEveryNRetries,
  maximumRetries: MaximumRetries,
  retryInterval: RetryInterval,
  healthCheckInterval: HealthCheckInterval
) {

  // config
  def withAlertService(value: AlertService[F]): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      value,
      alertEveryNRetries,
      maximumRetries,
      retryInterval,
      healthCheckInterval)

  def withAlertEveryNRetries(value: Int): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertService,
      AlertEveryNRetries(value),
      maximumRetries,
      retryInterval,
      healthCheckInterval)

  def withMaximumRetries(value: Long): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertService,
      alertEveryNRetries,
      MaximumRetries(value),
      retryInterval,
      healthCheckInterval)

  def withRetryInterval(value: FiniteDuration): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertService,
      alertEveryNRetries,
      maximumRetries,
      RetryInterval(value),
      healthCheckInterval)

  def withHealthCheckInterval(value: FiniteDuration): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertService,
      alertEveryNRetries,
      maximumRetries,
      retryInterval,
      HealthCheckInterval(value))

  // actions

  def alert(msg: SlackNotification)(implicit F: Async[F]): F[Unit] =
    alertService.alert(msg)

  def foreverAction[A](action: F[A])(implicit F: Async[F]): F[Unit] =
    new RetryForever[F](
      alertService,
      applicationName,
      serviceName,
      retryInterval,
      alertEveryNRetries,
      healthCheckInterval).foreverAction(action)

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F]): Stream[F, Unit] =
    new RetryForever[F](
      alertService,
      applicationName,
      serviceName,
      retryInterval,
      alertEveryNRetries,
      healthCheckInterval).infiniteStream(stream)

  def limitRetry[A](action: F[A])(implicit F: Async[F]): F[A] =
    new LimitRetry[F](alertService, applicationName, serviceName, maximumRetries, retryInterval).limitRetry(action)

}

object TaskGuard {

  def apply[F[_]](applicationName: String, serviceName: String, alertService: AlertService[F]): TaskGuard[F] =
    new TaskGuard[F](
      ApplicationName(applicationName),
      ServiceName(serviceName),
      alertService,
      AlertEveryNRetries(30), // 15 minutes raise an alert
      MaximumRetries(3),
      RetryInterval(30.seconds),
      HealthCheckInterval(6.hours)
    )
}
