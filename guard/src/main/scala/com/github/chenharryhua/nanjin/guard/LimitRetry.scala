package com.github.chenharryhua.nanjin.guard

import cats.Show
import cats.effect.Async
import cats.implicits._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, RetryPolicy, Sleep}

import scala.util.control.NonFatal

final private class LimitRetry[F[_]](
  alertServices: List[AlertService[F]],
  config: ActionConfig,
  actionID: ActionID
) {
  private val params: ActionParams = config.evalConfig

  def retryEval[A: Show, B](a: A, f: A => F[B])(implicit F: Async[F], sleep: Sleep[F]): F[B] = {
    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wd @ WillDelayAndRetry(_, _, _) =>
          alertServices
            .traverse(
              _.alert(ActionRetrying(
                applicationName = params.applicationName,
                serviceName = params.serviceName,
                actionID = actionID,
                actionInput = ActionInput(a.show),
                error = err,
                willDelayAndRetry = wd,
                alertMask = params.alertMask
              )))
            .void
        case gu @ GivingUp(_, _) =>
          alertServices
            .traverse(
              _.alert(ActionFailed(
                applicationName = params.applicationName,
                serviceName = params.serviceName,
                actionID = actionID,
                actionInput = ActionInput(a.show),
                error = err,
                givingUp = gu,
                alertMask = params.alertMask
              )))
            .void
      }

    val retryPolicy: RetryPolicy[F] =
      RetryPolicies.limitRetriesByCumulativeDelay[F](
        params.actionRetryInterval.value.mul(params.actionMaxRetries.value),
        RetryPolicies.constantDelay[F](params.actionRetryInterval.value)
      )

    retry
      .retryingOnSomeErrors[B](retryPolicy, (e: Throwable) => F.delay(NonFatal(e)), onError)(f(a))
      .flatTap(_ =>
        alertServices
          .traverse(
            _.alert(
              ActionSucced(
                applicationName = params.applicationName,
                serviceName = params.serviceName,
                actionID = actionID,
                actionInput = ActionInput(a.show),
                alertMask = params.alertMask)))
          .void)
  }
}
