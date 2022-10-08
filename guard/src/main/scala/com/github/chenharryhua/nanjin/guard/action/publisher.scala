package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Clock, Unique}
import cats.syntax.all.*
import cats.Monad
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Digested, Importance, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent, TraceInfo}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  ActionFail,
  ActionRetry,
  ActionStart,
  ActionSucc,
  InstantAlert,
  PassThrough
}
import fs2.concurrent.Channel
import io.circe.Json
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime
import scala.jdk.DurationConverters.ScalaDurationOps

private object publisher {

  def passThrough[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    digested: Digested,
    json: Json,
    isError: Boolean): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        PassThrough(
          digested = digested,
          timestamp = ts,
          serviceParams = serviceParams,
          isError = isError,
          value = json))
    } yield ()

  def alert[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    digested: Digested,
    msg: String,
    importance: Importance): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        InstantAlert(
          digested = digested,
          timestamp = ts,
          serviceParams = serviceParams,
          importance = importance,
          message = msg))
    } yield ()

  def actionStart[F[_]: Monad: Clock: Unique](
    channel: Channel[F, NJEvent],
    actionParams: ActionParams,
    input: F[Json],
    traceInfo: Option[TraceInfo]): F[ActionInfo] =
    for {
      ts <- actionParams.serviceParams.zonedNow
      token <- Unique[F].unique.map(_.hash)
      ai = ActionInfo(traceInfo = traceInfo, actionParams = actionParams, actionId = token, launchTime = ts)
      _ <- input
        .flatMap(json => channel.send(ActionStart(actionInfo = ai, input = json)))
        .whenA(actionParams.isNotice)
    } yield ai

  def actionRetry[F[_]: Monad: Clock](
    channel: Channel[F, NJEvent],
    actionInfo: ActionInfo,
    willDelayAndRetry: WillDelayAndRetry,
    ex: Throwable): F[Unit] =
    for {
      ts <- actionInfo.actionParams.serviceParams.zonedNow
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
    } yield ()

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
    ex: Throwable,
    input: F[Json]): F[ZonedDateTime] =
    for {
      ts <- actionInfo.actionParams.serviceParams.zonedNow
      _ <- input.flatMap(json =>
        channel.send(ActionFail(actionInfo = actionInfo, timestamp = ts, input = json, error = NJError(ex))))
    } yield ts
}
