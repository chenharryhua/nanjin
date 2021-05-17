package com.github.chenharryhua.nanjin.guard

import cats.data.Kleisli
import cats.effect.{Async, Sync}
import cats.syntax.all._
import fs2.Stream
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, Sleep}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.{ControlThrowable, NonFatal}

private case object StreamMustBeInfiniteError extends ControlThrowable

final case class RetryForeverState(
  alertEveryNRetry: AlertEveryNRetries,
  nextRetryIn: FiniteDuration,
  numOfRetries: Int,
  totalDelay: FiniteDuration,
  err: Throwable
)

final private class RetryForever[F[_]](interval: RetryInterval, alertEveryNRetry: AlertEveryNRetries) {

  def forever[A](action: F[A])(implicit F: Sync[F], sleep: Sleep[F]): Kleisli[F, RetryForeverState => F[Unit], A] =
    Kleisli { handle =>
      def onError(err: Throwable, details: RetryDetails): F[Unit] =
        details match {
          case WillDelayAndRetry(next, num, cumulative) =>
            handle(RetryForeverState(alertEveryNRetry, next, num, cumulative, err))
              .whenA(num % alertEveryNRetry.value == 0)
          case GivingUp(_, _) => F.unit
        }

      retry.retryingOnSomeErrors(
        RetryPolicies.constantDelay[F](interval.value),
        (e: Throwable) => F.delay(NonFatal(e)),
        onError)(action)
    }

  def infiniteStream[A](
    stream: Stream[F, A])(implicit F: Async[F], sleep: Sleep[F]): Kleisli[F, RetryForeverState => F[Unit], Unit] =
    Kleisli { (handle: RetryForeverState => F[Unit]) =>
      def onError(err: Throwable, details: RetryDetails): F[Unit] =
        details match {
          case WillDelayAndRetry(next, num, cumulative) =>
            handle(RetryForeverState(alertEveryNRetry, next, num, cumulative, err))
              .whenA(num % alertEveryNRetry.value == 0)
          case GivingUp(_, _) => F.unit
        }
      retry.retryingOnSomeErrors(
        RetryPolicies.constantDelay[F](interval.value),
        (e: Throwable) => F.delay(NonFatal(e)),
        onError)((stream ++ Stream.never[F]).compile.drain)
    }
}
