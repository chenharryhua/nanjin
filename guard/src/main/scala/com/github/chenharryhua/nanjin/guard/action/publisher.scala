package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Clock, Unique}
import cats.syntax.all.*
import cats.Monad
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
import natchez.Span
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
      ts <- Clock[F].realTimeInstant.map(serviceParams.toZonedDateTime)
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
      ts <- Clock[F].realTimeInstant.map(serviceParams.toZonedDateTime)
      _ <- channel.send(
        InstantAlert(
          digested = digested,
          timestamp = ts,
          serviceParams = serviceParams,
          importance = importance,
          message = msg))
    } yield ()

  def actionStart[F[_]: Monad: Clock: Unique](
    name: String,
    channel: Channel[F, NJEvent],
    actionParams: ActionParams,
    input: F[Json],
    span: Option[Span[F]]): F[ActionInfo] =
    for {
      ts <- Clock[F].realTimeInstant.map(actionParams.serviceParams.toZonedDateTime)
      token <- Unique[F].unique.map(_.hash)
      traceId <- span.flatTraverse(_.traceId)
      ai = ActionInfo(
        name = name,
        traceId = traceId,
        actionParams = actionParams,
        actionId = token,
        launchTime = ts)
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
      ts <- Clock[F].realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
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
      ts <- Clock[F].realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
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
      ts <- Clock[F].realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      _ <- input.flatMap(json =>
        channel.send(ActionFail(actionInfo = actionInfo, timestamp = ts, input = json, error = NJError(ex))))
    } yield ts
}
