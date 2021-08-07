package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Async, Outcome, Ref}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.alert.*
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.realZonedDateTime
import fs2.concurrent.Channel
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

final private class ActionRetryBase[F[_], A, B](
  actionName: String,
  serviceInfo: ServiceInfo,
  retryCount: Ref[F, Int],
  channel: Channel[F, NJEvent],
  dailySummaries: Ref[F, DailySummaries],
  params: ActionParams,
  input: A,
  succ: Kleisli[F, (A, B), String],
  fail: Kleisli[F, (A, Throwable), String])(implicit F: Async[F]) {

  private def failNotes(error: Throwable): F[Notes] = fail.run((input, error)).map(Notes(_))
  private def succNotes(b: B): F[Notes]             = succ.run((input, b)).map(Notes(_))

  val actionInfo: F[ActionInfo] = for {
    ts <- realZonedDateTime(params.serviceParams)
    uuid <- UUIDGen.randomUUID
  } yield ActionInfo(id = uuid, launchTime = ts, actionName = actionName, serviceInfo = serviceInfo)

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
          fn <- failNotes(ActionException.ActionCanceledExternally)
          _ <- channel.send(
            ActionFailed(
              timestamp = now,
              actionInfo = actionInfo,
              actionParams = params,
              numRetries = count,
              notes = fn,
              error = NJError(ActionException.ActionCanceledExternally)
            ))
        } yield ()
      case Outcome.Errored(error) =>
        for {
          count <- retryCount.get // number of retries
          now <- realZonedDateTime(params.serviceParams)
          _ <- dailySummaries.update(_.incActionFail)
          fn <- failNotes(error)
          _ <- channel.send(
            ActionFailed(
              timestamp = now,
              actionInfo = actionInfo,
              actionParams = params,
              numRetries = count,
              notes = fn,
              error = NJError(error)
            ))
        } yield ()
      case Outcome.Succeeded(fb) =>
        for {
          count <- retryCount.get // number of retries before success
          now <- realZonedDateTime(params.serviceParams)
          b <- fb
          sn <- succNotes(b)
          _ <- dailySummaries.update(_.incActionSucc)
          _ <- channel.send(
            ActionSucced(
              timestamp = now,
              actionInfo = actionInfo,
              actionParams = params,
              numRetries = count,
              notes = sn))
        } yield ()
    }
}
