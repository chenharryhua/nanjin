package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.codahale.metrics.{Counter, Timer}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  ActionComplete,
  ActionFail,
  ActionRetry,
  ActionStart
}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent}
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
  transError: IN => F[Json],
  transOutput: (IN, OUT) => F[Json],
  isWorthRetry: Throwable => F[Boolean],
  failCounter: Option[Counter],
  succCounter: Option[Counter],
  retryCounter: Option[Counter],
  timer: Option[Timer],
  actionInfo: ActionInfo,
  input: IN
)(implicit F: Temporal[F]) {

  private[this] def timingAndCounting(isComplete: Boolean, now: ZonedDateTime): Unit = {
    timer.foreach(_.update(Duration.between(actionInfo.launchTime, now)))
    if (isComplete) succCounter.foreach(_.inc(1)) else failCounter.foreach(_.inc(1))
  }

  @inline private[this] def buildJson(json: Either[Throwable, Json]): Json =
    json match {
      case Right(value) => value
      case Left(value)  => Json.fromString(ExceptionUtils.getMessage(value))
    }

  private[this] def sendFailureEvent(ex: Throwable): F[Unit] =
    for {
      ts <- actionInfo.actionParams.serviceParams.zonedNow
      json <- transError(input).attempt.map(buildJson)
      _ <- channel.send(ActionFail(actionInfo, ts, NJError(ex), json))
    } yield timingAndCounting(isComplete = false, ts)

  private[this] def fail(ex: Throwable): F[Either[RetryStatus, OUT]] =
    sendFailureEvent(ex) >> F.raiseError[OUT](ex).map[Either[RetryStatus, OUT]](Right(_))

  private[this] def retrying(ex: Throwable, status: RetryStatus): F[Either[RetryStatus, OUT]] =
    retryPolicy.decideNextRetry(status).flatMap {
      case PolicyDecision.GiveUp => fail(ex)
      case PolicyDecision.DelayAndRetry(delay) =>
        for {
          ts <- actionInfo.actionParams.serviceParams.zonedNow
          _ <- channel.send(
            ActionRetry(
              actionInfo = actionInfo,
              timestamp = ts,
              retriesSoFar = status.retriesSoFar,
              resumeTime = ts.plus(delay.toJava),
              error = NJError(ex)
            ))
          _ <- F.sleep(delay)
        } yield {
          retryCounter.foreach(_.inc(1))
          Left(status.addRetry(delay))
        }
    }

  private[this] val sendActionStartEvent: F[Unit] =
    F.whenA(actionInfo.actionParams.importance.isPublishActionStart)(channel.send(ActionStart(actionInfo)))

  private[this] val loop: F[OUT] = sendActionStartEvent >>
    F.tailRecM(RetryStatus.NoRetriesYet) { status =>
      arrow(input).attempt.flatMap {
        case Right(out) =>
          for {
            ts <- actionInfo.actionParams.serviceParams.zonedNow
            _ <- F.whenA(actionInfo.actionParams.importance.isPublishActionComplete)(
              transOutput(input, out).attempt.flatMap(json =>
                channel.send(ActionComplete(actionInfo, ts, buildJson(json)))))
          } yield {
            timingAndCounting(isComplete = true, ts)
            Right(out)
          }

        case Left(ex) if !NonFatal(ex) => fail(ex)
        case Left(ex)                  => isWorthRetry(ex).ifM(retrying(ex, status), fail(ex))
      }
    }

  val execute: F[OUT] = F.onCancel(loop, sendFailureEvent(ActionCancelException))
}

private object ActionCancelException extends Exception("action was canceled")
