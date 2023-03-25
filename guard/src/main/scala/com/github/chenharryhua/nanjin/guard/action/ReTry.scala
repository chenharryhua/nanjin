package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Sync, Temporal}
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry}
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
  measures: Measures[F]
)(implicit F: Temporal[F]) {

  @inline private[this] def buildJson(json: Either[Throwable, Json]): Json =
    json match {
      case Right(value) => value
      case Left(value)  => Json.fromString(ExceptionUtils.getMessage(value))
    }

  private[this] def sendFailureEvent(ai: ActionInfo, input: IN, ex: Throwable): F[Unit] =
    for {
      landTime <- F.monotonic
      json <- transError(input).attempt.map(buildJson)
      fd = landTime.minus(ai.nano)
      _ <- channel.send(ActionFail(ai, ai.launchTime.plus(fd), NJError(ex), json))
      _ <- measures.measureFailure(fd)
    } yield ()

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
          _ <- measures.countRetry
          _ <- F.sleep(delay)
        } yield Left(status.addRetry(delay))
    }

  private[this] def go(ai: ActionInfo, input: IN): F[OUT] =
    F.tailRecM(RetryStatus.NoRetriesYet) { status =>
      arrow(input).attempt.flatMap {
        case Right(out) =>
          for {
            landTime <- F.monotonic
            fd = landTime.minus(ai.nano)
            _ <- F.whenA(ai.actionParams.importance.isPublishActionComplete)(
              F.flatMap(F.attempt(transOutput(input, out)))(json =>
                channel.send(ActionComplete(ai, ai.launchTime.plus(fd), buildJson(json)))))
            _ <- measures.measureSuccess(fd)
          } yield Right(out)
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

final private class Measures[F[_]](
  val measureSuccess: FiniteDuration => F[Unit],
  val measureFailure: FiniteDuration => F[Unit],
  val countRetry: F[Unit])

private object Measures {
  def apply[F[_]](actionParams: ActionParams, metricRegistry: MetricRegistry)(implicit
    F: Sync[F]): Measures[F] = {
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

    val mSucc: FiniteDuration => F[Unit] =
      (succCounter, timer) match {
        case (Some(c), Some(t)) =>
          fd =>
            F.delay {
              c.inc(1)
              t.update(fd.toNanos, TimeUnit.NANOSECONDS)
            }
        case (Some(c), None) => _ => F.delay(c.inc(1))
        case (None, Some(t)) => fd => F.delay(t.update(fd.toNanos, TimeUnit.NANOSECONDS))
        case (None, None)    => _ => F.unit
      }

    val mFail: FiniteDuration => F[Unit] =
      (failCounter, timer) match {
        case (Some(c), Some(t)) =>
          fd =>
            F.delay {
              c.inc(1)
              t.update(fd.toNanos, TimeUnit.NANOSECONDS)
            }
        case (Some(c), None) => _ => F.delay(c.inc(1))
        case (None, Some(t)) => fd => F.delay(t.update(fd.toNanos, TimeUnit.NANOSECONDS))
        case (None, None)    => _ => F.unit
      }

    val mRetry: F[Unit] = retryCounter match {
      case Some(c) => F.delay(c.inc(1))
      case None    => F.unit
    }

    new Measures(mSucc, mFail, mRetry)
  }
}
