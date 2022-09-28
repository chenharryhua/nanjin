package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Unique
import cats.effect.kernel.{Ref, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionFail, ActionRetry, ActionStart, ActionSucc}
import fs2.concurrent.Channel
import io.circe.Json
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime
import scala.jdk.DurationConverters.ScalaDurationOps

/** fire action start when it is noticalbe
  *
  * fire action retry when it is non-trivial
  *
  * fire action succ when it is noticable
  *
  * fire action fail unconditionally
  */
final private class ActionEventPublisher[F[_]](
  channel: Channel[F, NJEvent],
  retryCount: Ref[F, Int]
)(implicit F: Temporal[F]) {

  def actionStart(actionParams: ActionParams, input: F[Json]): F[ActionInfo] =
    for {
      ts <- F.realTimeInstant.map(actionParams.serviceParams.toZonedDateTime)
      token <- Unique[F].unique.map(_.hash)
      ai = ActionInfo(actionParams = actionParams, actionId = token, launchTime = ts)
      _ <- input
        .flatMap(js => channel.send(ActionStart(actionInfo = ai, input = js)))
        .whenA(actionParams.isNotice)
    } yield ai

  def actionRetry(actionInfo: ActionInfo, willDelayAndRetry: WillDelayAndRetry, ex: Throwable): F[Unit] =
    for {
      ts <- F.realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      _ <- channel
        .send(
          ActionRetry(
            actionInfo = actionInfo,
            timestamp = ts,
            retriesSoFar = willDelayAndRetry.retriesSoFar,
            nextRetryTime = ts.plus(willDelayAndRetry.nextDelay.toJava),
            error = NJError(ex)
          ))
        .whenA(actionInfo.actionParams.isNonTrivial)
      _ <- retryCount.update(_ + 1)
    } yield ()

  def actionSucc(actionInfo: ActionInfo, output: F[Json]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      _ <- {
        for {
          num <- retryCount.get
          js <- output
          _ <- channel.send(
            ActionSucc(actionInfo = actionInfo, timestamp = ts, numRetries = num, output = js))
        } yield ()
      }.whenA(actionInfo.actionParams.isNotice)
    } yield ts

  def actionFail(actionInfo: ActionInfo, ex: Throwable, input: F[Json]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      numRetries <- retryCount.get
      js <- input
      _ <- channel.send(
        ActionFail(
          actionInfo = actionInfo,
          timestamp = ts,
          numRetries = numRetries,
          input = js,
          error = NJError(ex)))
    } yield ts
}
