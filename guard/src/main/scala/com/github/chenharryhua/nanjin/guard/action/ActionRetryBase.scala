package com.github.chenharryhua.nanjin.guard.action

import cats.data.Reader
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{ActionFailed, ActionInfo, ActionRetrying, NJEvent}
import fs2.concurrent.Channel
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

private class ActionRetryBase[F[_], A, B](input: A, succ: Reader[(A, B), String], fail: Reader[(A, Throwable), String])(
  implicit F: Async[F]) {

  def failNotes(error: Throwable): String =
    Option(fail.run((input, error))).getOrElse("null in failure notes")

  def succNotes(b: B): String =
    Option(succ.run((input, b))).getOrElse("null in success notes")

  def onError(actionInfo: ActionInfo, channel: Channel[F, NJEvent], ref: Ref[F, Int])(
    error: Throwable,
    details: RetryDetails): F[Unit] =
    details match {
      case wdr @ WillDelayAndRetry(_, _, _) =>
        channel.send(ActionRetrying(actionInfo, wdr, error)) *> ref.update(_ + 1)
      case gu @ GivingUp(_, _) =>
        for {
          now <- F.realTimeInstant
          _ <- channel.send(
            ActionFailed(
              actionInfo = actionInfo,
              givingUp = gu,
              endAt = now,
              notes = failNotes(error),
              error = error
            ))
        } yield ()
    }
}
