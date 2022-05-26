package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, OptionT}
import cats.effect.Unique
import cats.effect.kernel.{Ref, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent, Notes}
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

  def actionStart[A](
    actionParams: ActionParams,
    a: A,
    startUp: OptionT[F, Kleisli[F, A, Json]]): F[ActionInfo] =
    for {
      ts <- F.realTimeInstant.map(actionParams.serviceParams.toZonedDateTime)
      token <- Unique[F].unique.map(_.hash)
      ai = ActionInfo(actionParams, token, ts)
      _ <- (for {
        info <- startUp.value.flatMap(_.traverse(_.run(a)))
        _ <- channel.send(ActionStart(ai, info))
      } yield ()).whenA(actionParams.isNotice)
      _ <- serviceStatus.update(_.include(ai)).whenA(actionParams.isExpensive.value)
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

  def actionSucc[A, B](
    actionInfo: ActionInfo,
    input: A,
    output: F[B],
    buildNotes: (A, F[B]) => F[Option[Notes]]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      _ <- {
        for {
          num <- retryCount.get
          notes <- buildNotes(input, output)
          _ <- channel.send(ActionSucc(actionInfo, ts, num, notes))
        } yield ()
      }.whenA(actionInfo.actionParams.isNotice)
      _ <- serviceStatus.update(_.exclude(actionInfo)).whenA(actionInfo.actionParams.isExpensive.value)
    } yield ts

  def actionFail[A](
    actionInfo: ActionInfo,
    input: A,
    ex: Throwable,
    buildNotes: (A, Throwable) => F[Option[Notes]]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(actionInfo.actionParams.serviceParams.toZonedDateTime)
      numRetries <- retryCount.get
      notes <- buildNotes(input, ex)
      _ <- channel.send(
        ActionFail(
          actionInfo = actionInfo,
          timestamp = ts,
          numRetries = numRetries,
          notes = notes,
          error = NJError(ex)))
      _ <- serviceStatus.update(_.exclude(actionInfo)).whenA(actionInfo.actionParams.isExpensive.value)
    } yield ts
}
