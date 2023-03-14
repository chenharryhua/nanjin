package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionComplete, ActionFail, ActionRetry}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, MetricCategory, MetricID, NJError, NJEvent}
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.syntax.EncoderOps
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
  actionInfo: ActionInfo,
  input: IN,
  measures: Measures
)(implicit F: Temporal[F]) {

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
    } yield measures.measureFailure(actionInfo.launchTime, ts)

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
          measures.countRetries()
          Left(status.addRetry(delay))
        }
    }

  private[this] val loop: F[OUT] =
    F.tailRecM(RetryStatus.NoRetriesYet) { status =>
      arrow(input).attempt.flatMap {
        case Right(out) =>
          for {
            ts <- actionInfo.actionParams.serviceParams.zonedNow
            _ <- F.whenA(actionInfo.actionParams.importance.isPublishActionComplete)(
              transOutput(input, out).attempt.flatMap(json =>
                channel.send(ActionComplete(actionInfo, ts, buildJson(json)))))
          } yield {
            measures.measureSuccess(actionInfo.launchTime, ts)
            Right(out)
          }
        case Left(ex) if !NonFatal(ex) => fail(ex)
        case Left(ex) => isWorthRetry(ex).attempt.map(_.exists(identity)).ifM(retrying(ex, status), fail(ex))
      }
    }

  val execute: F[OUT] = F.onCancel(loop, sendFailureEvent(ActionCancelException))
}

private object ActionCancelException extends Exception("action was canceled")

final private class Measures(
  failCounter: Option[Counter],
  succCounter: Option[Counter],
  retryCounter: Option[Counter],
  timing: (ZonedDateTime, ZonedDateTime) => Unit
) {
  def measureSuccess(launchTime: ZonedDateTime, now: ZonedDateTime): Unit = {
    succCounter.foreach(_.inc(1))
    timing(launchTime, now)
  }

  def measureFailure(launchTime: ZonedDateTime, now: ZonedDateTime): Unit = {
    failCounter.foreach(_.inc(1))
    timing(launchTime, now)
  }

  def countRetries(): Unit = retryCounter.foreach(_.inc(1))
}

private object Measures {
  def apply(actionParams: ActionParams, metricRegistry: MetricRegistry): Measures = {
    val (failCounter: Option[Counter], succCounter: Option[Counter], retryCounter: Option[Counter]) =
      if (actionParams.isCounting) {
        val fail = Some(
          metricRegistry.counter(MetricID(actionParams.id, MetricCategory.ActionFailCounter).asJson.noSpaces))
        val succ = Some(
          metricRegistry.counter(
            MetricID(actionParams.id, MetricCategory.ActionCompleteCounter).asJson.noSpaces))
        val retries = Some(
          metricRegistry.counter(
            MetricID(actionParams.id, MetricCategory.ActionRetryCounter).asJson.noSpaces))
        (fail, succ, retries)
      } else (None, None, None)

    val timing: (ZonedDateTime, ZonedDateTime) => Unit =
      if (actionParams.isTiming) {
        val mId: MetricID = MetricID(actionParams.id, MetricCategory.ActionTimer)
        val timer: Timer  = metricRegistry.timer(mId.asJson.noSpaces)
        (s: ZonedDateTime, t: ZonedDateTime) => timer.update(Duration.between(s, t))
      } else
        (_: ZonedDateTime, _: ZonedDateTime) => ()

    new Measures(failCounter, succCounter, retryCounter, timing)
  }
}
