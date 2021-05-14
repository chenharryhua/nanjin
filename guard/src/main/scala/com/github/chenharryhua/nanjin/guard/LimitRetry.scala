package com.github.chenharryhua.nanjin.guard

import cats.MonadError
import cats.data.Kleisli
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, Sleep}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final private case class LimitedRetryParams(totalRetries: Int, totalDelay: FiniteDuration, err: Throwable)

final private class LimitRetry[F[_]: Sleep](times: MaximumRetries, interval: RetryInterval)(implicit
  F: MonadError[F, Throwable]) {

  def limitRetry[A](action: F[A]): Kleisli[F, LimitedRetryParams => F[Unit], A] =
    Kleisli { handle =>
      def onError(err: Throwable, details: RetryDetails): F[Unit] =
        details match {
          case WillDelayAndRetry(_, _, _) => F.unit
          case GivingUp(totalRetries, totalDelay) =>
            handle(LimitedRetryParams(totalRetries, totalDelay, err))
        }
      val retryPolicy =
        RetryPolicies.limitRetriesByCumulativeDelay[F](
          interval.value.mul(times.value),
          RetryPolicies.constantDelay[F](interval.value)
        )
      retry.retryingOnSomeErrors[A](retryPolicy, NonFatal(_), onError)(action)
    }
}
