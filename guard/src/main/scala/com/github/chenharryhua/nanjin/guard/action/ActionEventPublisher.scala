package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Ref, Temporal}
import cats.effect.std.UUIDGen
import cats.implicits.{catsSyntaxApply, toFunctorOps}
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.guard.config.{CountAction, TimeAction}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import retry.RetryDetails.WillDelayAndRetry

import java.time.{Duration, ZonedDateTime}

final private[action] class ActionEventPublisher[F[_]: UUIDGen: Temporal](
  actionInfo: ActionInfo,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  ongoings: Ref[F, Set[ActionInfo]]) {
  private lazy val failCounter: Counter = metricRegistry.counter(actionFailMRName(actionInfo.actionParams))
  private lazy val succCounter: Counter = metricRegistry.counter(actionSuccMRName(actionInfo.actionParams))
  private lazy val timer: Timer         = metricRegistry.timer(actionTimerMRName(actionInfo.actionParams))

  private def timingAndCounting(isSucc: Boolean, now: ZonedDateTime): Unit = {
    if (actionInfo.actionParams.isTiming === TimeAction.Yes) timer.update(Duration.between(actionInfo.launchTime, now))
    if (actionInfo.actionParams.isCounting === CountAction.Yes) {
      if (isSucc) succCounter.inc(1) else failCounter.inc(1)
    }
  }

  def actionStart: F[Unit] =
    (channel.send(ActionStart(actionInfo)) *> ongoings.update(_.incl(actionInfo))).whenA(actionInfo.isNotice)

  def actionRetry(
    retryCount: Ref[F, Int],
    willDelayAndRetry: WillDelayAndRetry,
    ex: Throwable
  ): F[Unit] =
    for {
      ts <- realZonedDateTime2(actionInfo.actionParams)
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
    retryCount: Ref[F, Int],
    input: A,
    output: F[B],
    buildNotes: Kleisli[F, (A, B), String]): F[Unit] =
    realZonedDateTime2(actionInfo.actionParams).flatMap { ts =>
      val op: F[Unit] = for {
        result <- output
        num <- retryCount.get
        notes <- buildNotes.run((input, result))
        _ <- channel.send(ActionSucc(actionInfo, ts, num, Notes(notes)))
        _ <- ongoings.update(_.excl(actionInfo))
      } yield ()
      op.whenA(actionInfo.isNotice).map(_ => timingAndCounting(isSucc = true, ts))
    }

  def actionFail[A](
    retryCount: Ref[F, Int],
    input: A,
    ex: Throwable,
    buildNotes: Kleisli[F, (A, Throwable), String]
  ): F[Unit] =
    for {
      ts <- realZonedDateTime2(actionInfo.actionParams)
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
    } yield timingAndCounting(isSucc = false, ts)
}
