package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{ActionInfo, ActionSucced, DailySummaries, NJEvent}
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ActionParams}
import fs2.concurrent.Channel
import retry.RetryPolicies

import java.util.UUID

final class ActionRetry[F[_], A, B](
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  serviceName: String,
  appName: String,
  actionConfig: ActionConfig,
  input: A,
  kleisli: Kleisli[F, A, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]) {
  val params: ActionParams = actionConfig.evalConfig

  def withSuccNotes(succ: (A, B) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      serviceName = serviceName,
      appName = appName,
      actionConfig = actionConfig,
      input = input,
      kleisli = kleisli,
      succ = Reader(succ.tupled),
      fail = fail)

  def withFailNotes(fail: (A, Throwable) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      serviceName = serviceName,
      appName = appName,
      actionConfig = actionConfig,
      input = input,
      kleisli = kleisli,
      succ = succ,
      fail = Reader(fail.tupled))

  def run(implicit F: Async[F]): F[B] =
    for {
      ref <- Ref.of[F, Int](0) // hold number of retries
      ts <- F.realTimeInstant // timestamp when the action start
      actionInfo = ActionInfo(
        actionName = actionName,
        serviceName = serviceName,
        appName = appName,
        params = params,
        id = UUID.randomUUID(),
        launchTime = ts)
      base = new ActionRetryBase[F, A, B](input, succ, fail)
      res <- retry
        .retryingOnAllErrors[B](
          params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
          base.onError(actionInfo, channel, ref, dailySummaries))(kleisli.run(input))
        .flatTap(b =>
          for {
            count <- ref.get // number of retries before success
            now <- F.realTimeInstant // timestamp when the action successed
            _ <- channel.send(ActionSucced(actionInfo, now, count, base.succNotes(b)))
            _ <- dailySummaries.update(_.incActionSucc)
          } yield ())
    } yield res
}
