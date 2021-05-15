package com.github.chenharryhua.nanjin.guard

import cats.data.Kleisli
import cats.effect.Sync
import cats.implicits._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, Sleep}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final private case class LimitedRetryState(totalRetries: Int, totalDelay: FiniteDuration, err: Throwable)

final private class LimitRetry[F[_]](times: MaximumRetries, interval: RetryInterval) {

  def limitRetry[A](action: F[A])(implicit F: Sync[F], sleep: Sleep[F]): Kleisli[F, LimitedRetryState => F[Unit], A] =
    Kleisli { handle =>
      def onError(err: Throwable, details: RetryDetails): F[Unit] =
        details match {
          case WillDelayAndRetry(_, _, _) => F.unit
          case GivingUp(totalRetries, totalDelay) =>
            handle(LimitedRetryState(totalRetries, totalDelay, err))
        }
      val retryPolicy =
        RetryPolicies.limitRetriesByCumulativeDelay[F](
          interval.value.mul(times.value),
          RetryPolicies.constantDelay[F](interval.value)
        )
      retry.retryingOnSomeErrors[A](retryPolicy, (e: Throwable) => F.delay(NonFatal(e)), onError)(action)
    }
}
