package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{ActionInfo, ActionSucced, DailySummaries, NJEvent, ServiceInfo}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import fs2.concurrent.Channel
import retry.RetryPolicies

import java.util.UUID

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
      ts <- F.realTimeInstant.map(_.atZone(params.serviceParams.taskParams.zoneId)) // timestamp when the action start
      actionInfo = ActionInfo(
        actionName = actionName,
        serviceInfo = serviceInfo,
        id = UUID.randomUUID(),
        launchTime = ts)
      base = new ActionRetryBase[F, A, B](input, succ, fail)
      res <- retry
        .retryingOnAllErrors[B](
          params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
          base.onError(actionInfo, params, channel, ref, dailySummaries))(kleisli.run(input))
        .flatTap(b =>
          for {
            count <- ref.get // number of retries before success
            now <- F.realTimeInstant.map(_.atZone(params.serviceParams.taskParams.zoneId))
            _ <- channel.send(
              ActionSucced(
                timestamp = now,
                actionInfo = actionInfo,
                params = params,
                numRetries = count,
                notes = base.succNotes(b)))
            _ <- dailySummaries.update(_.incActionSucc)
          } yield ())
    } yield res
}
