package com.github.chenharryhua.nanjin.guard.action

import cats.data.Reader
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert._
import fs2.concurrent.Channel
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.ZoneId

private class ActionRetryBase[F[_], A, B](input: A, succ: Reader[(A, B), String], fail: Reader[(A, Throwable), String])(
  implicit F: Async[F]) {

  def failNotes(error: Throwable): Notes = Notes(fail.run((input, error)))
  def succNotes(b: B): Notes             = Notes(succ.run((input, b)))

  def onError(
    zoneId: ZoneId,
    actionInfo: ActionInfo,
    channel: Channel[F, NJEvent],
    ref: Ref[F, Int],
    dailySummaries: Ref[F, DailySummaries])(error: Throwable, details: RetryDetails): F[Unit] =
    details match {
      case wdr: WillDelayAndRetry =>
        for {
          now <- F.realTimeInstant.map(_.atZone(zoneId))
          _ <- channel.send(ActionRetrying(now, actionInfo, wdr, NJError(error)))
          _ <- ref.update(_ + 1)
          _ <- dailySummaries.update(_.incActionRetries)
        } yield ()
      case gu: GivingUp =>
        for {
          now <- F.realTimeInstant.map(_.atZone(zoneId))
          _ <- channel.send(
            ActionFailed(
              timestamp = now,
              actionInfo = actionInfo,
              givingUp = gu,
              notes = failNotes(error),
              error = NJError(error)
            ))
          _ <- dailySummaries.update(_.incActionFail)
        } yield ()
    }
}
