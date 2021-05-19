package com.github.chenharryhua.nanjin.guard

import cats.Show
import cats.effect.Async
import fs2.Stream

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

final private case class AlertEveryNRetries(value: Int) extends AnyVal
final private case class MaximumRetries(value: Long) extends AnyVal
final private case class RetryInterval(value: FiniteDuration) extends AnyVal
final private case class HealthCheckInterval(value: FiniteDuration) extends AnyVal

sealed private trait AlertLevel
private case object AlertFailOnly extends AlertLevel
private case object AlertSuccOnly extends AlertLevel
private case object AlertBoth extends AlertLevel

final class TaskGuard[F[_]] private (
  applicationName: ApplicationName,
  serviceName: ServiceName,
  alertServices: List[AlertService[F]],
  alertEveryNRetries: AlertEveryNRetries,
  maximumRetries: MaximumRetries,
  retryInterval: RetryInterval,
  healthCheckInterval: HealthCheckInterval,
  alertLevel: AlertLevel
) {

  val getApplicationName: String = applicationName.value
  val getServiceName: String     = serviceName.value

  // config
  def addAlertService(value: AlertService[F]): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      value :: alertServices,
      alertEveryNRetries,
      maximumRetries,
      retryInterval,
      healthCheckInterval,
      alertLevel)

  def withAlertEveryNRetries(value: Int): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      AlertEveryNRetries(value),
      maximumRetries,
      retryInterval,
      healthCheckInterval,
      alertLevel)

  def withMaximumRetries(value: Long): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      alertEveryNRetries,
      MaximumRetries(value),
      retryInterval,
      healthCheckInterval,
      alertLevel)

  def withRetryInterval(value: FiniteDuration): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      alertEveryNRetries,
      maximumRetries,
      RetryInterval(value),
      healthCheckInterval,
      alertLevel)

  def withHealthCheckInterval(value: FiniteDuration): TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      alertEveryNRetries,
      maximumRetries,
      retryInterval,
      HealthCheckInterval(value),
      alertLevel)

  def withAlertSuccOnly: TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      alertEveryNRetries,
      maximumRetries,
      retryInterval,
      healthCheckInterval,
      AlertSuccOnly)

  def withAlertFailOnly: TaskGuard[F] =
    new TaskGuard[F](
      applicationName,
      serviceName,
      alertServices,
      alertEveryNRetries,
      maximumRetries,
      retryInterval,
      healthCheckInterval,
      AlertFailOnly)

  // non-stop action
  def nonStopAction[A](action: F[A])(implicit F: Async[F]): F[Unit] =
    new RunForever[F](
      alertServices,
      applicationName,
      serviceName,
      retryInterval,
      alertEveryNRetries,
      healthCheckInterval).nonStopAction(action)

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F]): Stream[F, Unit] =
    new RunForever[F](
      alertServices,
      applicationName,
      serviceName,
      retryInterval,
      alertEveryNRetries,
      healthCheckInterval).infiniteStream(stream)

  // retry eval
  def retryEval[A: Show, B](a: A)(f: A => F[B])(implicit F: Async[F]): F[B] =
    new LimitRetry[F](
      alertServices,
      applicationName,
      serviceName,
      maximumRetries,
      retryInterval,
      ActionID(UUID.randomUUID()),
      alertLevel).retryEval(a, f)

  def retryEvalTry[A: Show, B](a: A)(f: A => Try[B])(implicit F: Async[F]): F[B] =
    retryEval[A, B](a)((a: A) => F.fromTry(f(a)))

  def retryEvalEither[A: Show, B](a: A)(f: A => Either[Throwable, B])(implicit F: Async[F]): F[B] =
    retryEval[A, B](a)((a: A) => F.fromEither(f(a)))

  def retryEvalFuture[A: Show, B](a: A)(f: A => Future[B])(implicit F: Async[F]): F[B] =
    retryEval[A, B](a)((a: A) => F.fromFuture(F.blocking(f(a))))

}

object TaskGuard {

  def apply[F[_]](applicationName: String, serviceName: String): TaskGuard[F] =
    new TaskGuard[F](
      ApplicationName(applicationName),
      ServiceName(serviceName),
      List(new LogService[F]),
      AlertEveryNRetries(30), // 15 minutes raise an alert
      MaximumRetries(3),
      RetryInterval(30.seconds),
      HealthCheckInterval(6.hours),
      AlertBoth
    )
}
