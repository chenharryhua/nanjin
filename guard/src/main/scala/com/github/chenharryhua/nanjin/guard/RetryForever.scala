package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import org.log4s.Logger
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, Sleep}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final private case class RetryForeverState(
  alertEveryNRetry: AlertEveryNRetries,
  nextRetryIn: FiniteDuration,
  numOfRetries: Int,
  totalDelay: FiniteDuration,
  err: Throwable
)

final private class RetryForever[F[_]](
  alertService: AlertService[F],
  slack: Slack,
  retryInterval: RetryInterval,
  alertEveryNRetry: AlertEveryNRetries,
  healthCheckInterval: HealthCheckInterval) {
  private val logger: Logger = org.log4s.getLogger

  def foreverAction[A](action: F[A])(implicit F: Async[F], sleep: Sleep[F]): F[Unit] = {

    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case WillDelayAndRetry(next, num, cumulative) =>
          alertService
            .alert(slack.foreverAlert(RetryForeverState(alertEveryNRetry, next, num, cumulative, err)))
            .whenA(num % alertEveryNRetry.value == 0) >>
            F.blocking(logger.error(err)(s"fatal error in service: ${slack.name}"))
        case GivingUp(_, _) => F.unit
      }

    val healthChecking: F[Nothing] =
      alertService.alert(slack.healthCheck(healthCheckInterval)).delayBy(healthCheckInterval.value).foreverM

    val startNotify: F[Unit] =
      alertService.alert(slack.start).delayBy(retryInterval.value)

    val enrich: F[Unit] = for {
      fiber <- (startNotify <* healthChecking).start
      _ <- action.onCancel(fiber.cancel).onError { case _ => fiber.cancel }
      _ <- alertService.alert(slack.shouldNotStop)
    } yield ()

    retry.retryingOnSomeErrors(
      RetryPolicies.constantDelay[F](retryInterval.value),
      (e: Throwable) => F.delay(NonFatal(e)),
      onError)(enrich)
  }

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F], sleep: Sleep[F]): Stream[F, Unit] =
    Stream.eval(foreverAction((stream ++ Stream.never[F]).compile.drain))

}
