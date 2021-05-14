package com.github.chenharryhua.nanjin.guard

import cats.data.Kleisli
import cats.effect.Sync
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, Sleep}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.{ControlThrowable, NonFatal}
import cats.syntax.all._
import fs2.Stream

private case object StreamMustBeInfiniteError extends ControlThrowable

final private case class RetryForeverParams(
  alertEveryNRetry: AlertEveryNRetries,
  nextRetryIn: FiniteDuration,
  numOfRetries: Int,
  totalDelay: FiniteDuration,
  err: Throwable
)

final private class RetryForever[F[_]: Sleep](interval: RetryInterval, alertEveryNRetry: AlertEveryNRetries)(implicit
  F: Sync[F]) {

  def forever[A](action: F[A]): Kleisli[F, RetryForeverParams => F[Unit], A] =
    Kleisli { handle =>
      def onError(err: Throwable, details: RetryDetails): F[Unit] =
        details match {
          case WillDelayAndRetry(next, num, cumulative) =>
            handle(RetryForeverParams(alertEveryNRetry, next, num, cumulative, err))
              .whenA(num % alertEveryNRetry.value == 0)
          case GivingUp(_, _) => F.unit
        }
      retry.retryingOnSomeErrors(RetryPolicies.constantDelay[F](interval.value), NonFatal(_), onError)(action)
    }

  def infiniteStream[A](stream: Stream[F, A]): Kleisli[F, RetryForeverParams => F[Unit], Nothing] =
    Kleisli { handle =>
      def onError(err: Throwable, details: RetryDetails): F[Unit] =
        details match {
          case WillDelayAndRetry(next, num, cumulative) =>
            handle(RetryForeverParams(alertEveryNRetry, next, num, cumulative, err))
              .whenA(num % alertEveryNRetry.value == 0)
          case GivingUp(_, _) => F.unit
        }
      retry.retryingOnSomeErrors(RetryPolicies.constantDelay[F](interval.value), NonFatal(_), onError)(
        stream.compile.drain >> F.raiseError(StreamMustBeInfiniteError))
    }
}
