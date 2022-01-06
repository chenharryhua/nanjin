package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.Unique
import cats.effect.kernel.{Ref, Temporal}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import retry.RetryDetails.WillDelayAndRetry

import java.time.Instant

final private class ActionEventPublisher[F[_]: UUIDGen](
  channel: Channel[F, NJEvent],
  ongoings: Ref[F, Set[ActionInfo]])(implicit F: Temporal[F]) {

  def actionStart(actionParams: ActionParams): F[ActionInfo] =
    for {
      ts <- F.realTimeInstant
      token <- Unique[F].unique.map(_.hash)
      ai = ActionInfo(actionParams, token, ts)
      _ <- (channel.send(ActionStart(ai)) *> ongoings.update(_.incl(ai))).whenA(ai.isNotice)
    } yield ai

  def actionRetry(
    actionInfo: ActionInfo,
    retryCount: Ref[F, Int],
    willDelayAndRetry: WillDelayAndRetry,
    ex: Throwable): F[Unit] =
    for {
      ts <- F.realTimeInstant
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
    buildNotes: Kleisli[F, (A, B), String]): F[Instant] =
    F.realTimeInstant.flatMap { ts =>
      val publish: F[Unit] = for {
        result <- output
        num <- retryCount.get
        notes <- buildNotes.run((input, result))
        _ <- channel.send(ActionSucc(actionInfo, ts, num, Notes(notes)))
        _ <- ongoings.update(_.excl(actionInfo))
      } yield ()
      publish.whenA(actionInfo.isNotice).as(ts)
    }

  def actionFail[A](
    actionInfo: ActionInfo,
    retryCount: Ref[F, Int],
    input: A,
    ex: Throwable,
    buildNotes: Kleisli[F, (A, Throwable), String]): F[Instant] =
    for {
      ts <- F.realTimeInstant
      uuid <- UUIDGen.randomUUID[F]
      numRetries <- retryCount.get
      notes <- buildNotes.run((input, ex))
      _ <- channel.send(
        ActionFail(
          actionInfo = actionInfo,
          timestamp = ts,
          numRetries = numRetries,
          notes = Notes(notes),
          error = NJError(uuid, ex)))
      _ <- ongoings.update(_.excl(actionInfo)).whenA(actionInfo.isNotice)
    } yield ts
}
