package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Unique
import cats.effect.kernel.{Ref, Temporal}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import retry.RetryDetails.WillDelayAndRetry

import java.time.ZonedDateTime

final private class ActionEventPublisher[F[_]: UUIDGen](
  serviceParams: ServiceParams,
  channel: Channel[F, NJEvent],
  ongoings: Ref[F, Set[ActionInfo]])(implicit F: Temporal[F]) {

  def actionStart(actionParams: ActionParams): F[ActionInfo] =
    for {
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      token <- Unique[F].unique.map(_.hash)
      ai = ActionInfo(actionParams, token, ts)
      _ <- channel.send(ActionStart(ai)).whenA(actionParams.isNotice)
      _ <- ongoings.update(_.incl(ai)).whenA(actionParams.isExpensive.value)
    } yield ai

  def actionRetry(
    actionInfo: ActionInfo,
    retryCount: Ref[F, Int],
    willDelayAndRetry: WillDelayAndRetry,
    ex: Throwable): F[Unit] =
    for {
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      uuid <- UUIDGen.randomUUID[F]
      _ <- channel.send(
        ActionRetry(
          actionInfo = actionInfo,
          timestamp = ts,
          willDelayAndRetry = willDelayAndRetry,
          error = NJError(uuid, ex)))
      _ <- retryCount.update(_ + 1)
    } yield ()

  def actionSucc[A, B](
    actionInfo: ActionInfo,
    retryCount: Ref[F, Int],
    input: A,
    output: F[B],
    buildNotes: (A, F[B]) => F[Notes]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      _ <- {
        for {
          num <- retryCount.get
          notes <- buildNotes(input, output)
          _ <- channel.send(ActionSucc(actionInfo, ts, num, notes))
        } yield ()
      }.whenA(actionInfo.actionParams.isNotice)
      _ <- ongoings.update(_.excl(actionInfo)).whenA(actionInfo.actionParams.isExpensive.value)
    } yield ts

  def actionFail[A](
    actionInfo: ActionInfo,
    retryCount: Ref[F, Int],
    ex: Throwable,
    input: A,
    buildNotes: (A, Throwable) => F[Notes]): F[ZonedDateTime] =
    for {
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      uuid <- UUIDGen.randomUUID[F]
      numRetries <- retryCount.get
      notes <- buildNotes(input, ex)
      _ <- channel.send(
        ActionFail(
          actionInfo = actionInfo,
          timestamp = ts,
          numRetries = numRetries,
          notes = notes,
          error = NJError(uuid, ex)))
      _ <- ongoings.update(_.excl(actionInfo)).whenA(actionInfo.actionParams.isExpensive.value)
    } yield ts
}
