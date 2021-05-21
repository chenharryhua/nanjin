package com.github.chenharryhua.nanjin.guard

import cats.Show
import cats.effect.Async
import cats.implicits._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, RetryPolicy}

import java.time.Instant
import java.util.UUID

final class ActionGuard[F[_]](alertServices: List[AlertService[F]], config: ActionConfig) {
  private val params: ActionParams = config.evalConfig

  def updateConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](alertServices, f(config))

  def run[A: Show, B](a: A)(f: A => F[B])(implicit F: Async[F]): F[B] = {
    val action = RetriedAction(params.actionName, a.show, UUID.randomUUID(), Instant.now, params.zoneId)
    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wd @ WillDelayAndRetry(_, _, _) =>
          alertServices
            .traverse(
              _.alert(
                ActionRetrying(
                  applicationName = params.applicationName,
                  serviceName = params.serviceName,
                  action = action,
                  error = err,
                  willDelayAndRetry = wd,
                  alertMask = params.alertMask
                )))
            .void
        case gu @ GivingUp(_, _) =>
          alertServices
            .traverse(
              _.alert(
                ActionFailed(
                  applicationName = params.applicationName,
                  serviceName = params.serviceName,
                  action = action,
                  error = err,
                  givingUp = gu,
                  alertMask = params.alertMask
                )))
            .void
      }

    val retryPolicy: RetryPolicy[F] =
      params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries))

    retry
      .retryingOnAllErrors[B](retryPolicy, onError)(f(a))
      .flatTap(_ =>
        alertServices
          .traverse(
            _.alert(
              ActionSucced(
                applicationName = params.applicationName,
                serviceName = params.serviceName,
                action = action,
                alertMask = params.alertMask)))
          .void)
  }
}
