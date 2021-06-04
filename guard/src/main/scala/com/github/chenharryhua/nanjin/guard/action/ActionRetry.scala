package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{ActionInfo, ActionSucced, NJEvent}
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ActionParams}
import fs2.concurrent.Channel
import retry.RetryPolicies

import java.util.UUID

final class ActionRetry[F[_], A, B](
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
      ref <- Ref.of[F, Int](0)
      ts <- F.realTimeInstant
      actionInfo: ActionInfo =
        ActionInfo(
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
          base.onError(actionInfo, channel, ref))(kleisli.run(input))
        .flatTap(b =>
          for {
            count <- ref.get
            now <- F.realTimeInstant
            _ <- channel.send(
              ActionSucced(actionInfo = actionInfo, endAt = now, numRetries = count, notes = base.succNotes(b)))
          } yield ())
    } yield res
}
