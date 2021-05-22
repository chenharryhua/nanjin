package com.github.chenharryhua.nanjin.guard

import cats.effect.{Async, Ref}
import cats.implicits._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, RetryPolicy}

import java.time.Instant
import java.util.UUID

final private class ExecutableAction[F[_]: Async, A, B](
  alertServices: List[AlertService[F]],
  params: ActionParams,
  input: A,
  exec: A => F[B],
  succ: (A, B) => String,
  fail: (A, Throwable) => String,
  ref: Ref[F, Int]) {

  def run: F[B] = {
    val action = RetriedAction(UUID.randomUUID(), Instant.now, params.zoneId)
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
                  willDelayAndRetry = wd)))
            .void *> ref.update(_ + 1)
        case gu @ GivingUp(_, _) =>
          alertServices
            .traverse(
              _.alert(ActionFailed(
                applicationName = params.applicationName,
                serviceName = params.serviceName,
                notes = fail(input, err),
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
      .retryingOnAllErrors[B](retryPolicy, onError)(exec(input))
      .flatTap(b =>
        ref.get.flatMap(count =>
          alertServices
            .traverse(
              _.alert(
                ActionSucced(
                  applicationName = params.applicationName,
                  serviceName = params.serviceName,
                  notes = succ(input, b),
                  action = action,
                  alertMask = params.alertMask,
                  retries = count)))
            .void))
  }
}

final class ActionGuard[F[_]](alertServices: List[AlertService[F]], config: ActionConfig) {
  val params: ActionParams = config.evalConfig

  def updateConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](alertServices, f(config))

  def run[A, B](a: A)(f: A => F[B])(succ: (A, B) => String)(fail: (A, Throwable) => String)(implicit
    F: Async[F]): F[B] =
    Ref.of(0).flatMap(ref => new ExecutableAction[F, A, B](alertServices, params, a, f, succ, fail, ref).run)
}
