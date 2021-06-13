package com.github.chenharryhua.nanjin.guard.action

import cats.data.Reader
import cats.effect.{Async, Outcome, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert._
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import fs2.concurrent.Channel
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.ZonedDateTime
import java.util.UUID

private class ActionRetryBase[F[_], A, B](
  actionName: String,
  serviceInfo: ServiceInfo,
  retryCount: Ref[F, Int],
  channel: Channel[F, NJEvent],
  dailySummaries: Ref[F, DailySummaries],
  params: ActionParams,
  input: A,
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String])(implicit F: Async[F]) {

  def failNotes(error: Throwable): Notes = Notes(fail.run((input, error)))
  def succNotes(b: B): Notes             = Notes(succ.run((input, b)))

  private val realZonedDateTime: F[ZonedDateTime] =
    F.realTimeInstant.map(_.atZone(params.serviceParams.taskParams.zoneId))

  val actionInfo: F[ActionInfo] = realZonedDateTime.map(ts =>
    ActionInfo(actionName = actionName, serviceInfo = serviceInfo, id = UUID.randomUUID(), launchTime = ts))

  def onError(actionInfo: ActionInfo)(error: Throwable, details: RetryDetails): F[Unit] =
    details match {
      case wdr: WillDelayAndRetry =>
        for {
          now <- realZonedDateTime
          _ <- channel.send(
            ActionRetrying(
              timestamp = now,
              actionInfo = actionInfo,
              params = params,
              willDelayAndRetry = wdr,
              error = NJError(error)))
          _ <- retryCount.update(_ + 1)
          _ <- dailySummaries.update(_.incActionRetries)
        } yield ()
      case _: GivingUp => F.unit
    }

  def guaranteeCase(actionInfo: ActionInfo)(outcome: Outcome[F, Throwable, B]): F[Unit] = outcome match {
    case Outcome.Canceled() =>
      val error = new Exception("the action was cancelled")
      for {
        count <- retryCount.get
        now <- realZonedDateTime
        _ <- dailySummaries.update(_.incActionFail)
        _ <- channel.send(
          ActionFailed(
            timestamp = now,
            actionInfo = actionInfo,
            params = params,
            numRetries = count,
            notes = failNotes(error),
            error = NJError(error)
          ))
      } yield ()
    case Outcome.Errored(error) =>
      for {
        count <- retryCount.get
        now <- realZonedDateTime
        _ <- dailySummaries.update(_.incActionFail)
        _ <- channel.send(
          ActionFailed(
            timestamp = now,
            actionInfo = actionInfo,
            params = params,
            numRetries = count,
            notes = failNotes(error),
            error = NJError(error)
          ))
      } yield ()
    case Outcome.Succeeded(fb) =>
      for {
        count <- retryCount.get // number of retries before success
        now <- realZonedDateTime
        b <- fb
        _ <- dailySummaries.update(_.incActionSucc)
        _ <- channel.send(
          ActionSucced(
            timestamp = now,
            actionInfo = actionInfo,
            params = params,
            numRetries = count,
            notes = succNotes(b)))
      } yield ()
  }
}
