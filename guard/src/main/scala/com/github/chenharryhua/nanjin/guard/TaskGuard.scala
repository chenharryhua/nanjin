package com.github.chenharryhua.nanjin.guard

import cats.Show
import cats.effect.Async
import fs2.Stream

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

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

  // alert only
  def alert(msg: SlackNotification)(implicit F: Async[F]): F[Unit] =
    alertService.alert(msg)

  private val slack: Slack = new Slack(applicationName, serviceName)

  // non-stop action
  def foreverAction[A](action: F[A])(implicit F: Async[F]): F[Unit] =
    new RetryForever[F](alertService, slack, retryInterval, alertEveryNRetries, healthCheckInterval)
      .foreverAction(action)

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F]): Stream[F, Unit] =
    new RetryForever[F](alertService, slack, retryInterval, alertEveryNRetries, healthCheckInterval)
      .infiniteStream(stream)

  // retry eval
  def retryEval[A: Show, B](a: A)(f: A => F[B])(implicit F: Async[F]): F[B] =
    new LimitRetry[F](alertService, slack, maximumRetries, retryInterval).retryEval(a)(f)

  def retryEvalTry[A: Show, B](a: A)(f: A => Try[B])(implicit F: Async[F]): F[B] =
    retryEval[A, B](a)((a: A) => F.fromTry(f(a)))

  def retryEvalEither[A: Show, B](a: A)(f: A => Either[Throwable, B])(implicit F: Async[F]): F[B] =
    retryEval[A, B](a)((a: A) => F.fromEither(f(a)))

  def retryEvalFuture[A: Show, B](a: A)(f: A => Future[B])(implicit F: Async[F]): F[B] =
    retryEval[A, B](a)((a: A) => F.fromFuture(F.blocking(f(a))))

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
