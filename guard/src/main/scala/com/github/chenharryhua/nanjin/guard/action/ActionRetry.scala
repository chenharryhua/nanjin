package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.syntax.all._
import cats.effect.{Async, Outcome, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{DailySummaries, NJEvent, ServiceInfo}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import fs2.concurrent.Channel
import retry.RetryPolicies

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class ActionRetry[F[_], A, B](
  serviceInfo: ServiceInfo,
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  params: ActionParams,
  input: A,
  fab: Kleisli[F, A, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String],
  isWorthRetry: Kleisli[F, Throwable, Boolean]) {

  def withSuccNotes(succ: (A, B) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      fab = fab,
      succ = Reader(succ.tupled),
      fail = fail,
      isWorthRetry = isWorthRetry)

  def withFailNotes(fail: (A, Throwable) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      fab = fab,
      succ = succ,
      fail = Reader(fail.tupled),
      isWorthRetry = isWorthRetry)

  def withPredicate(worthRetry: Throwable => F[Boolean]): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      fab = fab,
      succ = succ,
      fail = fail,
      isWorthRetry = Kleisli(worthRetry))

  def run(implicit F: Async[F]): F[B] =
    for {
      retryCount <- F.ref(0) // hold number of retries
      base = new ActionRetryBase[F, A, B](
        actionName = actionName,
        serviceInfo = serviceInfo,
        retryCount = retryCount,
        channel = channel,
        dailySummaries = dailySummaries,
        params = params,
        input = input,
        succ = succ,
        fail = fail)
      actionInfo <- base.actionInfo
      res <- F.uncancelable(poll =>
        retry.mtl
          .retryingOnSomeErrors[B](
            params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
            isWorthRetry.run,
            base.onError(actionInfo)) {
            for {
              gate <- F.deferred[Outcome[F, Throwable, B]]
              fiber <- F.start(fab.run(input).guaranteeCase(gate.complete(_).void))
              oc <- F.onCancel(
                poll(gate.get).flatMap(_.embed(F.raiseError[B](new Exception("the action was canceled")))),
                fiber.cancel)
            } yield oc
          }
          .guaranteeCase(base.handleOutcome(actionInfo)))
    } yield res
}
