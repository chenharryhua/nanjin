package com.github.chenharryhua.nanjin.guard

import cats.data.{Kleisli, NonEmptyList, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies}

import java.util.UUID

final class RetryAction[F[_], A, B](
  applicationName: String,
  actionName: String,
  alertServices: NonEmptyList[AlertService[F]],
  config: ActionConfig,
  kleisli: Kleisli[F, A, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]
) {
  val params: ActionParams = config.evalConfig

  def withSuccInfo(succ: (A, B) => String): RetryAction[F, A, B] =
    new RetryAction[F, A, B](
      applicationName,
      actionName,
      alertServices,
      config.succOn,
      kleisli,
      Reader(succ.tupled),
      fail)

  def withFailInfo(fail: (A, Throwable) => String): RetryAction[F, A, B] =
    new RetryAction[F, A, B](
      applicationName,
      actionName,
      alertServices,
      config.failOn,
      kleisli,
      succ,
      Reader(fail.tupled))

  def run(input: A)(implicit F: Async[F]): F[B]       = Ref.of[F, Int](0).flatMap(ref => internalRun(input, ref))
  def run(implicit F: Async[F], ev: Unit =:= A): F[B] = Ref.of[F, Int](0).flatMap(ref => internalRun((), ref))

  private def internalRun(input: A, ref: Ref[F, Int])(implicit F: Async[F]): F[B] = F.realTimeInstant.flatMap { ts =>
    val actionInfo: ActionInfo =
      ActionInfo(actionName, params.retryPolicy.policy[F].show, ts, params.alertMask, UUID.randomUUID())
    def onError(error: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wdr @ WillDelayAndRetry(_, _, _) =>
          alertServices.traverse(
            _.alert(
              ActionRetrying(
                applicationName = applicationName,
                actionInfo = actionInfo,
                willDelayAndRetry = wdr,
                error = error
              )).attempt) *> ref.update(_ + 1)
        case gu @ GivingUp(_, _) =>
          alertServices
            .traverse(
              _.alert(
                ActionFailed(
                  applicationName = applicationName,
                  actionInfo = actionInfo,
                  givingUp = gu,
                  notes = fail.run((input, error)),
                  error = error
                )).attempt)
            .void
      }

    retry
      .retryingOnAllErrors[B](
        params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
        onError)(kleisli.run(input))
      .flatTap(b =>
        ref.get.flatMap(count =>
          alertServices
            .traverse(
              _.alert(
                ActionSucced(
                  applicationName = applicationName,
                  actionInfo = actionInfo,
                  notes = succ.run((input, b)),
                  numRetries = count
                )).attempt)
            .void))
  }
}

final class ActionGuard[F[_]](
  applicationName: String,
  actionName: String,
  alertServices: NonEmptyList[AlertService[F]],
  config: ActionConfig) {

  def updateConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](applicationName, actionName, alertServices, f(config))

  def retry[A, B](f: A => F[B]): RetryAction[F, A, B] =
    new RetryAction[F, A, B](
      applicationName,
      actionName,
      alertServices,
      config,
      Kleisli(f),
      Reader(_ => ""),
      Reader(_ => ""))

  def retry[B](f: F[B]): RetryAction[F, Unit, B] = retry[Unit, B](_ => f)
}
