package com.github.chenharryhua.nanjin.guard

import cats.effect.{Async, Ref}
import cats.implicits._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, RetryPolicy}

import java.time.Instant
import java.util.UUID

final class RetriableAction[F[_], A, B](
  alertServices: List[AlertService[F]],
  config: ActionConfig,
  input: A,
  exec: A => F[B],
  succ: (A, B) => String,
  fail: (A, Throwable) => String
) {
  val params: ActionParams = config.evalConfig

  def withSucc(succ: (A, B) => String): RetriableAction[F, A, B] =
    new RetriableAction[F, A, B](alertServices, config.succOn, input, exec, succ, fail)

  def withFail(fail: (A, Throwable) => String): RetriableAction[F, A, B] =
    new RetriableAction[F, A, B](alertServices, config.failOn, input, exec, succ, fail)

  def run(implicit F: Async[F]): F[B] = Ref.of[F, Int](0).flatMap(ref => internalRun(ref))

  private def internalRun(ref: Ref[F, Int])(implicit F: Async[F]): F[B] = {
    val action = RetriedAction(UUID.randomUUID(), Instant.now, params.zoneId)
    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wd @ WillDelayAndRetry(_, _, _) =>
          alertServices.traverse(
            _.alert(
              ActionRetrying(
                applicationName = params.applicationName,
                serviceName = params.serviceName,
                action = action,
                alertMask = params.alertMask,
                error = err,
                willDelayAndRetry = wd)).attempt) *> ref.update(_ + 1)
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
              )).attempt)
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
                  retries = count)).attempt)
            .void))
  }
}

final class ActionGuard[F[_]](alertServices: List[AlertService[F]], config: ActionConfig) {

  def updateConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](alertServices, f(config))

  def retry[A, B](a: A)(f: A => F[B]): RetriableAction[F, A, B] =
    new RetriableAction[F, A, B](alertServices, config, a, f, (_, _) => "", (_, _) => "")

  def retry[B](f: F[B]): RetriableAction[F, Unit, B] = retry[Unit, B](())(Unit => f)
}
