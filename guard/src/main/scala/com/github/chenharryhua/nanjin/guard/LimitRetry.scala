package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.implicits._
import org.log4s.Logger
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, RetryPolicy, Sleep}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final private case class LimitedRetryState(totalRetries: Int, totalDelay: FiniteDuration, err: Throwable)

final private class LimitRetry[F[_]](
  alertService: AlertService[F],
  slack: Slack,
  times: MaximumRetries,
  interval: RetryInterval) {
  private val logger: Logger = org.log4s.getLogger

  def limitRetry[A](action: F[A])(implicit F: Async[F], sleep: Sleep[F]): F[A] = {
    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case WillDelayAndRetry(_, sofar, _) =>
          val msg =
            s"error in service: ${slack.name}, retries so far: $sofar/${times.value}"
          F.blocking(logger.error(err)(msg))
        case GivingUp(totalRetries, totalDelay) =>
          val msg =
            s"error in service: ${slack.name}, give up after retry $totalRetries times"
          F.blocking(logger.error(err)(msg)) *>
            alertService.alert(slack.limitAlert(LimitedRetryState(totalRetries, totalDelay, err)))
      }

    val retryPolicy: RetryPolicy[F] =
      RetryPolicies.limitRetriesByCumulativeDelay[F](
        interval.value.mul(times.value),
        RetryPolicies.constantDelay[F](interval.value)
      )
    retry.retryingOnSomeErrors[A](retryPolicy, (e: Throwable) => F.delay(NonFatal(e)), onError)(action)
  }
}
