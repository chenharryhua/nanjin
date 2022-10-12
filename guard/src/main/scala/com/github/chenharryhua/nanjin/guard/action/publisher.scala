package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Clock, Unique}
import cats.syntax.all.*
import cats.Monad
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent, TraceInfo}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionFail, ActionStart, ActionSucc}
import fs2.concurrent.Channel
import io.circe.Json

import java.time.ZonedDateTime

private object publisher {

  def actionStart[F[_]: Monad: Clock: Unique](
    channel: Channel[F, NJEvent],
    actionParams: ActionParams,
    input: F[Json],
    traceInfo: Option[TraceInfo]): F[ActionInfo] =
    for {
      ts <- actionParams.serviceParams.zonedNow
      token <- Unique[F].unique.map(_.hash)
      json <- input
      ai = ActionInfo(
        traceInfo = traceInfo,
        actionParams = actionParams,
        actionId = token,
        launchTime = ts,
        input = json)
      _ <- channel.send(ActionStart(actionInfo = ai)).whenA(actionParams.isNotice)
    } yield ai

  def actionSucc[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    actionInfo: ActionInfo,
    output: F[Json]): F[ZonedDateTime] =
    for {
      ts <- actionInfo.actionParams.serviceParams.zonedNow
      _ <- output
        .flatMap(json => channel.send(ActionSucc(actionInfo = actionInfo, timestamp = ts, output = json)))
        .whenA(actionInfo.actionParams.isNotice)
    } yield ts

  def actionFail[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    actionInfo: ActionInfo,
    ex: Throwable): F[ZonedDateTime] =
    for {
      ts <- actionInfo.actionParams.serviceParams.zonedNow
      _ <- channel.send(ActionFail(actionInfo = actionInfo, timestamp = ts, error = NJError(ex)))
    } yield ts
}
