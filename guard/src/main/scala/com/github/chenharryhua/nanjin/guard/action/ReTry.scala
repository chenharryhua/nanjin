package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.codahale.metrics.{Counter, Timer}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionFail, ActionRetry, ActionStart, ActionSucc}
import fs2.concurrent.Channel
import io.circe.Json
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.time.{Duration, ZonedDateTime}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.control.NonFatal

final private class ReTry[F[_], IN, OUT](
  channel: Channel[F, NJEvent],
  retryPolicy: RetryPolicy[F],
  arrow: IN => F[OUT],
  transInput: IN => F[Json],
  transOutput: (IN, OUT) => F[Json],
  isWorthRetry: Throwable => F[Boolean],
  failCounter: Counter,
  succCounter: Counter,
  timer: Timer,
  actionInfo: ActionInfo,
  input: IN
)(implicit F: Temporal[F]) {

  private[this] def timingAndCounting(isSucc: Boolean, now: ZonedDateTime): Unit = {
    if (actionInfo.actionParams.isTiming) timer.update(Duration.between(actionInfo.launchTime, now))
    if (actionInfo.actionParams.isCounting) { if (isSucc) succCounter.inc(1) else failCounter.inc(1) }
  }

  @inline private[this] def buildJson(json: Either[Throwable, Json]): Json =
    json match {
      case Right(value) => value
      case Left(value)  => Json.fromString(ExceptionUtils.getMessage(value))
    }

  private[this] def sendFailureEvent(ex: Throwable): F[Unit] =
    for {
      ts <- actionInfo.actionParams.serviceParams.zonedNow
      json <- transInput(input).attempt.map(buildJson)
      _ <- channel.send(ActionFail(actionInfo, ts, NJError(ex), json))
    } yield timingAndCounting(isSucc = false, ts)

  private[this] def fail(ex: Throwable): F[Either[RetryStatus, OUT]] =
    sendFailureEvent(ex) >> F.raiseError[OUT](ex).map[Either[RetryStatus, OUT]](Right(_))

  private[this] def retrying(ex: Throwable, status: RetryStatus): F[Either[RetryStatus, OUT]] =
    retryPolicy.decideNextRetry(status).flatMap {
      case PolicyDecision.GiveUp => fail(ex)
      case PolicyDecision.DelayAndRetry(delay) =>
        for {
          _ <- F.whenA(actionInfo.actionParams.isNonTrivial)(for {
            ts <- actionInfo.actionParams.serviceParams.zonedNow
            _ <- channel.send(
              ActionRetry(
                actionInfo = actionInfo,
                timestamp = ts,
                retriesSoFar = status.retriesSoFar,
                resumeTime = ts.plus(delay.toJava),
                error = NJError(ex)
              ))
          } yield ())
          _ <- F.sleep(delay)
        } yield Left(status.addRetry(delay))
    }

  private[this] val sendActionStartEvent: F[Unit] =
    F.whenA(actionInfo.actionParams.isNotice)(transInput(input).attempt.flatMap(json =>
      channel.send(ActionStart(actionInfo, buildJson(json)))))

  private[this] val loop: F[OUT] = sendActionStartEvent >>
    F.tailRecM(RetryStatus.NoRetriesYet) { status =>
      arrow(input).attempt.flatMap {
        case Right(out) =>
          for {
            ts <- actionInfo.actionParams.serviceParams.zonedNow
            _ <- F.whenA(actionInfo.actionParams.isNotice)(transOutput(input, out).attempt.flatMap(json =>
              channel.send(ActionSucc(actionInfo, ts, buildJson(json)))))
          } yield {
            timingAndCounting(isSucc = true, ts)
            Right(out)
          }

        case Left(ex) if !NonFatal(ex) => fail(ex)
        case Left(ex)                  => isWorthRetry(ex).ifM(retrying(ex, status), fail(ex))
      }
    }

  val execute: F[OUT] = F.onCancel(loop, sendFailureEvent(ActionException.ActionCanceled))
}
