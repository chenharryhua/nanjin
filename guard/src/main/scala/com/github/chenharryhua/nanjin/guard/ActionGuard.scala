package com.github.chenharryhua.nanjin.guard

import cats.Show
import cats.effect.Async
import cats.implicits._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, RetryPolicy, Sleep}

import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final class ActionGuard[F[_]](
  alertServices: List[AlertService[F]],
  config: ActionConfig
) {
  private val params: ActionParams = config.evalConfig

  private def update(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](alertServices, f(config))

  def withActionName(value: String): ActionGuard[F]            = update(_.withActionName(value))
  def withMaxRetries(value: Long): ActionGuard[F]              = update(_.withMaxRetries(value))
  def withRetryInterval(value: FiniteDuration): ActionGuard[F] = update(_.withRetryInterval(value))
  def offAlertSucc: ActionGuard[F]                             = update(_.offSucc)
  def offAlertFail: ActionGuard[F]                             = update(_.offFail)

  def run[A: Show, B](a: A)(f: A => F[B])(implicit F: Async[F], sleep: Sleep[F]): F[B] = {
    val action = ProtectedAction(params.actionName, a.show, UUID.randomUUID())
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
      RetryPolicies.limitRetriesByCumulativeDelay[F](
        params.retryInterval.mul(params.maxRetries),
        RetryPolicies.constantDelay[F](params.retryInterval)
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
                action = action,
                alertMask = params.alertMask)))
          .void)
  }
}
