package com.github.chenharryhua.nanjin.guard.action

import cats.collections.Predicate
import cats.data.{Kleisli, Reader}
import cats.effect.kernel.{Async, Outcome, Ref}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.alert.{ActionStart, DailySummaries, NJEvent, ServiceInfo}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import fs2.concurrent.Channel

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class ActionRetry[F[_], A, B](
  serviceInfo: ServiceInfo,
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  params: ActionParams,
  input: A,
  kfab: Kleisli[F, A, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String],
  isWorthRetry: Reader[Throwable, Boolean],
  postCondition: Predicate[B])(implicit F: Async[F]) {

  def withSuccNotes(succ: (A, B) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kfab = kfab,
      succ = Reader(succ.tupled),
      fail = fail,
      isWorthRetry = isWorthRetry,
      postCondition = postCondition)

  def withFailNotes(fail: (A, Throwable) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kfab = kfab,
      succ = succ,
      fail = Reader(fail.tupled),
      isWorthRetry = isWorthRetry,
      postCondition = postCondition)

  def withWorthRetry(isWorthRetry: Throwable => Boolean): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kfab = kfab,
      succ = succ,
      fail = fail,
      isWorthRetry = Reader(isWorthRetry),
      postCondition = postCondition)

  def withPostCondition(postCondition: B => Boolean): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kfab = kfab,
      succ = succ,
      fail = fail,
      isWorthRetry = isWorthRetry,
      postCondition = Predicate(postCondition))

  def run: F[B] =
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
      _ <- channel.send(ActionStart(actionInfo.launchTime, actionInfo, params))
      res <- F.uncancelable(poll =>
        retry.mtl
          .retryingOnSomeErrors[B](
            params.retry.policy[F],
            isWorthRetry.map(F.pure).run,
            base.onError(actionInfo)
          ) {
            for {
              gate <- F.deferred[Outcome[F, Throwable, B]]
              fiber <- F.start(kfab.run(input).guaranteeCase(gate.complete(_).void))
              oc <- F.onCancel(
                poll(gate.get).flatMap(_.embed(F.raiseError[B](ActionException.ActionCanceledInternally))),
                fiber.cancel)
              _ <- F.raiseError(ActionException.UnexpectedlyTerminated).whenA(!params.shouldTerminate)
              _ <- F.raiseError(ActionException.PostConditionUnsatisfied).whenA(!postCondition(oc))
            } yield oc
          }
          .guaranteeCase(base.handleOutcome(actionInfo)))
    } yield res
}

final class ActionRetryUnit[F[_]: Async, B](
  serviceInfo: ServiceInfo,
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  params: ActionParams,
  fb: F[B],
  succ: Reader[B, String],
  fail: Reader[Throwable, String],
  isWorthRetry: Reader[Throwable, Boolean],
  postCondition: Predicate[B]) {

  def withSuccNotes(succ: B => String): ActionRetryUnit[F, B] =
    new ActionRetryUnit[F, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      fb = fb,
      succ = Reader(succ),
      fail = fail,
      isWorthRetry = isWorthRetry,
      postCondition = postCondition)

  def withFailNotes(fail: Throwable => String): ActionRetryUnit[F, B] =
    new ActionRetryUnit[F, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      fb = fb,
      succ = succ,
      fail = Reader(fail),
      isWorthRetry = isWorthRetry,
      postCondition = postCondition)

  def withWorthRetry(isWorthRetry: Throwable => Boolean): ActionRetryUnit[F, B] =
    new ActionRetryUnit[F, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      fb = fb,
      succ = succ,
      fail = fail,
      isWorthRetry = Reader(isWorthRetry),
      postCondition = postCondition)

  def withPostCondition(postCondition: B => Boolean): ActionRetryUnit[F, B] =
    new ActionRetryUnit[F, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      fb = fb,
      succ = succ,
      fail = fail,
      isWorthRetry = isWorthRetry,
      postCondition = Predicate(postCondition))

  def run: F[B] =
    new ActionRetry[F, Unit, B](
      serviceInfo,
      dailySummaries,
      channel,
      actionName,
      params,
      (),
      Kleisli(_ => fb),
      succ.local(_._2),
      fail.local(_._2),
      isWorthRetry,
      postCondition
    ).run
}
