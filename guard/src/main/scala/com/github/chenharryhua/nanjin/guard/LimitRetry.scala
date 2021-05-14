package com.github.chenharryhua.nanjin.guard

import cats.data.Kleisli
import cats.effect.Sync
import cats.implicits._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, Sleep}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final private case class LimitedRetryParams(totalRetries: Int, totalDelay: FiniteDuration, err: Throwable)

final private class LimitRetry[F[_]](times: MaximumRetries, interval: RetryInterval)(implicit
  F: Sync[F],
  sleep: Sleep[F]) {

  private def isWorthRetry(err: Throwable): F[Boolean] = F.delay {
    err match {
      case NonFatal(_) => true
      case _           => false
    }
  }

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
      retry.retryingOnSomeErrors[A](retryPolicy, isWorthRetry, onError)(action)
    }
}
