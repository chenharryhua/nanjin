package com.github.chenharryhua.nanjin.guard.action

import cats.data.{EitherT, Kleisli, Reader}
import cats.effect.syntax.all._
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{ActionInfo, DailySummaries, NJEvent, ServiceInfo}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import fs2.concurrent.Channel
import retry.RetryPolicies

import java.util.UUID

/** When outer F[_] fails, return immedidately only retry when the inner Either is on the left branch
  */
final class ActionRetryEither[F[_], A, B](
  serviceInfo: ServiceInfo,
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  params: ActionParams,
  input: A,
  eitherT: EitherT[Kleisli[F, A, *], Throwable, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]) {

  def withSuccNotes(succ: (A, B) => String): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      eitherT = eitherT,
      succ = Reader(succ.tupled),
      fail = fail)

  def withFailNotes(fail: (A, Throwable) => String): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      eitherT = eitherT,
      succ = succ,
      fail = Reader(fail.tupled))

  def run(implicit F: Async[F]): F[B] =
    for {
      ref <- Ref.of[F, Int](0)
      ts <- F.realTimeInstant.map(_.atZone(params.serviceParams.taskParams.zoneId))
      actionInfo =
        ActionInfo(actionName = actionName, serviceInfo = serviceInfo, id = UUID.randomUUID(), launchTime = ts)
      base = new ActionRetryBase[F, A, B](actionInfo, ref, channel, dailySummaries, params, input, succ, fail)
      b <- retry
        .retryingOnAllErrors[Either[Throwable, B]](
          params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
          base.onError) {
          eitherT.value.run(input).attempt.flatMap {
            case Left(ex) => F.pure(Left(ex))
            case Right(outerRight) =>
              outerRight match {
                case Left(ex)     => F.raiseError(ex)
                case r @ Right(_) => F.pure(r)
              }
          }
        }
        .rethrow
        .guaranteeCase(base.guaranteeCase)
    } yield b
}
