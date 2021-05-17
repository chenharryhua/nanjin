package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import org.log4s.Logger
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, Sleep}

import scala.util.control.NonFatal

final private class RetryForever[F[_]](
  alertService: AlertService[F],
  applicationName: ApplicationName,
  serviceName: ServiceName,
  retryInterval: RetryInterval,
  alertEveryNRetry: AlertEveryNRetries,
  healthCheckInterval: HealthCheckInterval) {
  private val logger: Logger = org.log4s.getLogger

  def forever[A](action: F[A])(implicit F: Async[F], sleep: Sleep[F]) = {

    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case WillDelayAndRetry(next, num, cumulative) =>
          (alertService
            .alert(slack
              .alert(applicationName, serviceName, RetryForeverState(alertEveryNRetry, next, num, cumulative, err)))
            .whenA(num % alertEveryNRetry.value == 0)) >>
            F.blocking(logger.error(err)(s"fatal error in service: ${applicationName.value}/${serviceName.value}"))
        case GivingUp(_, _) => F.unit
      }

    val healthChecking =
      alertService
        .alert(slack.healthCheck(applicationName, serviceName, healthCheckInterval))
        .delayBy(healthCheckInterval.value)
        .foreverM

    val startNotify = alertService.alert(slack.start(applicationName, serviceName)).delayBy(retryInterval.value)

    val enrich = for {
      fiber <- (startNotify <* healthChecking).start
      a <- action.onCancel(fiber.cancel).onError { case _ => fiber.cancel }
      _ <- alertService.alert(slack.shouldNotStop(applicationName, serviceName))
    } yield a

    retry.retryingOnSomeErrors(
      RetryPolicies.constantDelay[F](retryInterval.value),
      (e: Throwable) => F.delay(NonFatal(e)),
      onError)(enrich)
  }

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F], sleep: Sleep[F]) =
    forever((stream ++ Stream.never[F]).compile.drain)

}
