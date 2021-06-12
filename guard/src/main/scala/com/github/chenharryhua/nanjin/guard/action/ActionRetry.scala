package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.syntax.all._
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{DailySummaries, NJEvent, ServiceInfo}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import fs2.concurrent.Channel
import retry.RetryPolicies

final class ActionRetry[F[_], A, B](
  serviceInfo: ServiceInfo,
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  params: ActionParams,
  input: A,
  kleisli: Kleisli[F, A, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]) {

  def withSuccNotes(succ: (A, B) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kleisli = kleisli,
      succ = Reader(succ.tupled),
      fail = fail)

  def withFailNotes(fail: (A, Throwable) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kleisli = kleisli,
      succ = succ,
      fail = Reader(fail.tupled))

  def run(implicit F: Async[F]): F[B] =
    for {
      ref <- Ref.of[F, Int](0) // hold number of retries
      base = new ActionRetryBase[F, A, B](
        actionName = actionName,
        serviceInfo = serviceInfo,
        ref = ref,
        channel = channel,
        dailySummaries = dailySummaries,
        params = params,
        input = input,
        succ = succ,
        fail = fail)
      actionInfo <- base.actionInfo
      ret <- retry
        .retryingOnAllErrors[B](
          params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
          base.onError(actionInfo))(kleisli.run(input))
        .guaranteeCase(base.guaranteeCase(actionInfo))
    } yield ret
}
