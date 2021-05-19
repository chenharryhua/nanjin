package com.github.chenharryhua.nanjin.guard

import cats.Show
import cats.effect.Async
import cats.implicits._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, RetryPolicy, Sleep}

import scala.util.control.NonFatal

final private class LimitRetry[F[_]](
  alertServices: List[AlertService[F]],
  applicationName: ApplicationName,
  serviceName: ServiceName,
  times: MaximumRetries,
  interval: RetryInterval,
  actionID: ActionID,
  alertLevel: AlertLevel) {

  def retryEval[A: Show, B](a: A, f: A => F[B])(implicit F: Async[F], sleep: Sleep[F]): F[B] = {
    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wd @ WillDelayAndRetry(_, _, _) =>
          alertServices
            .traverse(
              _.alert(ActionRetrying(
                applicationName = applicationName,
                serviceName = serviceName,
                actionID = actionID,
                actionInput = ActionInput(a.show),
                error = err,
                willDelayAndRetry = wd,
                alertLevel = alertLevel
              )))
            .void
        case gu @ GivingUp(_, _) =>
          alertServices
            .traverse(
              _.alert(
                ActionFailed(
                  applicationName = applicationName,
                  serviceName = serviceName,
                  actionID = actionID,
                  actionInput = ActionInput(a.show),
                  error = err,
                  givingUp = gu,
                  alertLevel = alertLevel
                )))
            .void
      }

    val retryPolicy: RetryPolicy[F] =
      RetryPolicies.limitRetriesByCumulativeDelay[F](
        interval.value.mul(times.value),
        RetryPolicies.constantDelay[F](interval.value)
      )

    retry
      .retryingOnSomeErrors[B](retryPolicy, (e: Throwable) => F.delay(NonFatal(e)), onError)(f(a))
      .flatTap(_ =>
        F.blocking(
          alertServices.traverse(
            _.alert(
              ActionSucced(
                applicationName = applicationName,
                serviceName = serviceName,
                actionID = actionID,
                actionInput = ActionInput(a.show),
                alertLevel = alertLevel)))))
  }
}
