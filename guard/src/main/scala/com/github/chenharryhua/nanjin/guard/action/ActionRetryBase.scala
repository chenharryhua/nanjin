package com.github.chenharryhua.nanjin.guard.action

import cats.data.Reader
import cats.effect.kernel.{Async, Outcome, Ref}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.alert._
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.realZonedDateTime
import fs2.concurrent.Channel
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.util.UUID

final private class ActionRetryBase[F[_], A, B](
  actionName: String,
  serviceInfo: ServiceInfo,
  retryCount: Ref[F, Int],
  channel: Channel[F, NJEvent],
  dailySummaries: Ref[F, DailySummaries],
  params: ActionParams,
  input: A,
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String])(implicit F: Async[F]) {

  private def failNotes(error: Throwable): Notes = Notes(fail.run((input, error)))
  private def succNotes(b: B): Notes             = Notes(succ.run((input, b)))

  val actionInfo: F[ActionInfo] = realZonedDateTime(params.serviceParams).map(ts =>
    ActionInfo(id = UUID.randomUUID(), launchTime = ts, actionName = actionName, serviceInfo = serviceInfo))

  def onError(actionInfo: ActionInfo)(error: Throwable, details: RetryDetails): F[Unit] =
    details match {
      case wdr: WillDelayAndRetry =>
        for {
          now <- realZonedDateTime(params.serviceParams)
          _ <- channel.send(
            ActionRetrying(
              timestamp = now,
              actionInfo = actionInfo,
              actionParams = params,
              willDelayAndRetry = wdr,
              error = NJError(error)))
          _ <- retryCount.update(_ + 1)
          _ <- dailySummaries.update(_.incActionRetries)
        } yield ()
      case _: GivingUp => F.unit
    }

  def handleOutcome(actionInfo: ActionInfo)(outcome: Outcome[F, Throwable, B]): F[Unit] =
    outcome match {
      case Outcome.Canceled() =>
        for {
          count <- retryCount.get // number of retries
          now <- realZonedDateTime(params.serviceParams)
          _ <- dailySummaries.update(_.incActionFail)
          _ <- channel.send(
            ActionFailed(
              timestamp = now,
              actionInfo = actionInfo,
              actionParams = params,
              numRetries = count,
              notes = failNotes(ActionException.ActionCanceledExternally),
              error = NJError(ActionException.ActionCanceledExternally)
            ))
        } yield ()
      case Outcome.Errored(error) =>
        for {
          count <- retryCount.get // number of retries
          now <- realZonedDateTime(params.serviceParams)
          _ <- dailySummaries.update(_.incActionFail)
          _ <- channel.send(
            ActionFailed(
              timestamp = now,
              actionInfo = actionInfo,
              actionParams = params,
              numRetries = count,
              notes = failNotes(error),
              error = NJError(error)
            ))
        } yield ()
      case Outcome.Succeeded(fb) =>
        for {
          count <- retryCount.get // number of retries before success
          now <- realZonedDateTime(params.serviceParams)
          b <- fb
          _ <- dailySummaries.update(_.incActionSucc)
          _ <- channel.send(
            ActionSucced(
              timestamp = now,
              actionInfo = actionInfo,
              actionParams = params,
              numRetries = count,
              notes = succNotes(b)))
        } yield ()
    }
}
