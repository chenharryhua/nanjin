package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Temporal, Unique}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Digested, Importance, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent}
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

  def passThrough[F[_]](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    digested: Digested,
    json: Json,
    isError: Boolean)(implicit F: Temporal[F]): F[Unit] =
    for {
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      _ <- channel.send(
        PassThrough(
          digested = digested,
          timestamp = ts,
          serviceParams = serviceParams,
          isError = isError,
          value = json))
    } yield ()

  def alert[F[_]](
    channel: Channel[F, NJEvent],
    serviceParams: ServiceParams,
    digested: Digested,
    msg: String,
    importance: Importance)(implicit F: Temporal[F]): F[Unit] =
    for {
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      _ <- channel.send(
        InstantAlert(
          digested = digested,
          timestamp = ts,
          serviceParams = serviceParams,
          importance = importance,
          message = msg))
    } yield ()

  def actionStart[F[_]](channel: Channel[F, NJEvent], actionParams: ActionParams, input: F[Json])(implicit
    F: Temporal[F]): F[ActionInfo] =
    for {
      ts <- F.realTimeInstant.map(actionParams.serviceParams.toZonedDateTime)
      token <- Unique[F].unique.map(_.hash)
      ai = ActionInfo(actionParams = actionParams, actionId = token, launchTime = ts)
      _ <- input
        .flatMap(json => channel.send(ActionStart(actionInfo = ai, input = json)))
        .whenA(actionParams.isNotice)
    } yield ai

  def actionRetry[F[_]](
    channel: Channel[F, NJEvent],
    actionInfo: ActionInfo,
    willDelayAndRetry: WillDelayAndRetry,
    ex: Throwable)(implicit F: Temporal[F]): F[Unit] =
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
    } yield ()

  def actionSucc[F[_]](channel: Channel[F, NJEvent], actionInfo: ActionInfo, output: F[Json])(implicit
    F: Temporal[F]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      _ <- output
        .flatMap(json => channel.send(ActionSucc(actionInfo = actionInfo, timestamp = ts, output = json)))
        .whenA(actionInfo.actionParams.isNotice)
    } yield ts

  def actionFail[F[_]](channel: Channel[F, NJEvent], actionInfo: ActionInfo, ex: Throwable, input: F[Json])(
    implicit F: Temporal[F]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      _ <- input.flatMap(json =>
        channel.send(ActionFail(actionInfo = actionInfo, timestamp = ts, input = json, error = NJError(ex))))
    } yield ts
}
