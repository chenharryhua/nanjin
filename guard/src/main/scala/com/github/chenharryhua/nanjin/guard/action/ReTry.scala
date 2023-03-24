package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Category, CounterKind, MetricID}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionComplete, ActionFail, ActionRetry}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent}
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final private class ReTry[F[_], IN, OUT](
  channel: Channel[F, NJEvent],
  retryPolicy: RetryPolicy[F],
  arrow: IN => F[OUT],
  transError: IN => F[Json],
  transOutput: (IN, OUT) => F[Json],
  isWorthRetry: Throwable => F[Boolean],
  measures: Measures
)(implicit F: Temporal[F]) {

  @inline private[this] def buildJson(json: Either[Throwable, Json]): Json =
    json match {
      case Right(value) => value
      case Left(value)  => Json.fromString(ExceptionUtils.getMessage(value))
    }

  private[this] def sendFailureEvent(ai: ActionInfo, input: IN, ex: Throwable): F[Unit] =
    for {
      landTime <- F.realTime
      json <- transError(input).attempt.map(buildJson)
      _ <- channel.send(ActionFail(ai, landTime, NJError(ex), json))
    } yield measures.measureFailure(landTime - ai.launchTime)

  private[this] def fail(ai: ActionInfo, input: IN, ex: Throwable): F[Either[RetryStatus, OUT]] =
    sendFailureEvent(ai, input, ex) >> F.raiseError[OUT](ex).map[Either[RetryStatus, OUT]](Right(_))

  private[this] def retrying(
    ai: ActionInfo,
    input: IN,
    ex: Throwable,
    status: RetryStatus): F[Either[RetryStatus, OUT]] =
    retryPolicy.decideNextRetry(status).flatMap {
      case PolicyDecision.GiveUp => fail(ai, input, ex)
      case PolicyDecision.DelayAndRetry(delay) =>
        for {
          landTime <- F.realTime
          _ <- channel.send(
            ActionRetry(
              actionInfo = ai,
              landTime = landTime,
              retriesSoFar = status.retriesSoFar,
              delay = delay,
              error = NJError(ex)
            ))
          _ <- F.sleep(delay)
        } yield {
          measures.countRetries()
          Left(status.addRetry(delay))
        }
    }

  private[this] def go(ai: ActionInfo, input: IN): F[OUT] =
    F.tailRecM(RetryStatus.NoRetriesYet) { status =>
      arrow(input).attempt.flatMap {
        case Right(out) =>
          for {
            landTime <- F.realTime
            _ <- F.whenA(ai.actionParams.importance.isPublishActionComplete)(
              F.flatMap(F.attempt(transOutput(input, out)))(json =>
                channel.send(ActionComplete(ai, landTime, buildJson(json)))))
          } yield {
            measures.measureSuccess(landTime - ai.launchTime)
            Right(out)
          }
        case Left(ex) if !NonFatal(ex) => fail(ai, input, ex)
        case Left(ex) =>
          isWorthRetry(ex).attempt
            .map(_.exists(identity))
            .ifM(retrying(ai, input, ex, status), fail(ai, input, ex))
      }
    }

  def execute(actionInfo: ActionInfo, input: IN): F[OUT] =
    F.onCancel(go(actionInfo, input), sendFailureEvent(actionInfo, input, ActionCancelException))
}

private object ActionCancelException extends Exception("action was canceled")

final private class Measures(
  failCounter: Option[Counter],
  succCounter: Option[Counter],
  retryCounter: Option[Counter],
  timer: Option[Timer]
) {
  def measureSuccess(elapse: => FiniteDuration): Unit = {
    succCounter.foreach(_.inc(1))
    timer.foreach(_.update(elapse.toNanos, TimeUnit.NANOSECONDS))
  }

  def measureFailure(elapse: => FiniteDuration): Unit = {
    failCounter.foreach(_.inc(1))
    timer.foreach(_.update(elapse.toNanos, TimeUnit.NANOSECONDS))
  }

  def countRetries(): Unit = retryCounter.foreach(_.inc(1))
}

private object Measures {
  def apply(actionParams: ActionParams, metricRegistry: MetricRegistry): Measures = {
    val (failCounter: Option[Counter], succCounter: Option[Counter], retryCounter: Option[Counter]) =
      if (actionParams.isCounting) {
        val metricName = actionParams.metricID.metricName
        val fail = Some(
          metricRegistry.counter(
            MetricID(metricName, Category.Counter(Some(CounterKind.ActionFail))).asJson.noSpaces))
        val succ = Some(
          metricRegistry.counter(
            MetricID(metricName, Category.Counter(Some(CounterKind.ActionComplete))).asJson.noSpaces))
        val retries = Some(
          metricRegistry.counter(
            MetricID(metricName, Category.Counter(Some(CounterKind.ActionRetry))).asJson.noSpaces))
        (fail, succ, retries)
      } else (None, None, None)

    val timer =
      if (actionParams.isTiming) Some(metricRegistry.timer(actionParams.metricID.asJson.noSpaces)) else None
    new Measures(failCounter, succCounter, retryCounter, timer)
  }
}
