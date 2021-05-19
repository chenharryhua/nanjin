package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, Sleep}

import scala.util.control.NonFatal

final private class RunForever[F[_]](
  alertServices: List[AlertService[F]],
  applicationName: ApplicationName,
  serviceName: ServiceName,
  retryInterval: RetryInterval,
  alertEveryNRetry: AlertEveryNRetries,
  healthCheckInterval: HealthCheckInterval) {

  def nonStopAction[A](action: F[A])(implicit F: Async[F], sleep: Sleep[F]): F[Unit] = {

    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wd @ WillDelayAndRetry(_, _, _) =>
          alertServices
            .traverse(
              _.alert(
                ServiceRestarting(
                  applicationName = applicationName,
                  serviceName = serviceName,
                  willDelayAndRetry = wd,
                  alertEveryNRetry = alertEveryNRetry,
                  error = err)))
            .void
        case GivingUp(_, _) =>
          alertServices
            .traverse(
              _.alert(ServiceAbnormalStop(applicationName = applicationName, serviceName = serviceName, error = err)))
            .void
      }

    val healthChecking: F[Nothing] =
      alertServices
        .traverse(
          _.alert(
            ServiceHealthCheck(
              applicationName = applicationName,
              serviceName = serviceName,
              healthCheckInterval = healthCheckInterval)))
        .void
        .delayBy(healthCheckInterval.value)
        .foreverM

    val startService: F[Unit] =
      alertServices
        .traverse(_.alert(ServiceStarted(applicationName = applicationName, serviceName = serviceName)))
        .void
        .delayBy(retryInterval.value)

    val enrich: F[Unit] = for {
      fiber <- (startService <* healthChecking).start
      _ <- action.onCancel(fiber.cancel).onError { case _ => fiber.cancel }
      _ <- alertServices
        .traverse(
          _.alert(
            ServiceAbnormalStop(
              applicationName = applicationName,
              serviceName = serviceName,
              error = new Exception(s"service(${serviceName.value}) was abnormally stopped."))))
        .void
    } yield ()

    retry.retryingOnSomeErrors(
      RetryPolicies.constantDelay[F](retryInterval.value),
      (e: Throwable) => F.delay(NonFatal(e)),
      onError)(enrich)
  }

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F], sleep: Sleep[F]): Stream[F, Unit] =
    Stream.eval(nonStopAction((stream ++ Stream.never[F]).compile.drain))

}
