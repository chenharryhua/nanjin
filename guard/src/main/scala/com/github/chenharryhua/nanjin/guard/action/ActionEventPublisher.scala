package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Unique
import cats.effect.kernel.{Ref, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionFail, ActionRetry, ActionStart, ActionSucc}
import com.github.chenharryhua.nanjin.guard.service.ServiceStatus
import fs2.concurrent.Channel
import io.circe.Json
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime
import scala.jdk.DurationConverters.ScalaDurationOps

final private class ActionEventPublisher[F[_]](
  serviceStatus: Ref[F, ServiceStatus],
  channel: Channel[F, NJEvent],
  retryCount: Ref[F, Int]
)(implicit F: Temporal[F]) {

  def actionStart(actionParams: ActionParams, info: F[Json]): F[ActionInfo] =
    for {
      ts <- F.realTimeInstant.map(actionParams.serviceParams.toZonedDateTime)
      token <- Unique[F].unique.map(_.hash)
      ai = ActionInfo(actionParams, token, ts)
      _ <- info.flatMap(js => channel.send(ActionStart(ai, js))).whenA(actionParams.isNotice)
      _ <- serviceStatus.update(_.include(ai)).whenA(actionParams.isExpensive)
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

  def actionSucc(actionInfo: ActionInfo, info: F[Json]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      _ <- {
        for {
          num <- retryCount.get
          js <- info
          _ <- channel.send(ActionSucc(actionInfo, ts, num, js))
        } yield ()
      }.whenA(actionInfo.actionParams.isNotice)
      _ <- serviceStatus.update(_.exclude(actionInfo)).whenA(actionInfo.actionParams.isExpensive)
    } yield ts

  def actionFail(actionInfo: ActionInfo, ex: Throwable, inputInfo: F[Json]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      numRetries <- retryCount.get
      info <- inputInfo
      _ <- channel.send(
        ActionFail(
          actionInfo = actionInfo,
          timestamp = ts,
          numRetries = numRetries,
          info = info,
          error = NJError(ex)))
      _ <- serviceStatus.update(_.exclude(actionInfo)).whenA(actionInfo.actionParams.isExpensive)
    } yield ts
}
