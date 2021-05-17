package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.implicits._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, RetryPolicy, Sleep}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final case class LimitedRetryState(totalRetries: Int, totalDelay: FiniteDuration, err: Throwable)

final private class LimitRetry[F[_]](
  alertService: AlertService[F],
  applicationName: ApplicationName,
  serviceName: ServiceName,
  times: MaximumRetries,
  interval: RetryInterval) {

  def limitRetry[A](action: F[A])(implicit F: Async[F], sleep: Sleep[F]): F[A] = {
    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case WillDelayAndRetry(_, _, _) => F.unit
        case GivingUp(totalRetries, totalDelay) =>
          alertService.alert(
            slack.limitAlert(applicationName, serviceName, LimitedRetryState(totalRetries, totalDelay, err)))
      }

    val retryPolicy: RetryPolicy[F] =
      RetryPolicies.limitRetriesByCumulativeDelay[F](
        interval.value.mul(times.value),
        RetryPolicies.constantDelay[F](interval.value)
      )
    retry.retryingOnSomeErrors[A](retryPolicy, (e: Throwable) => F.delay(NonFatal(e)), onError)(action)
  }
}
