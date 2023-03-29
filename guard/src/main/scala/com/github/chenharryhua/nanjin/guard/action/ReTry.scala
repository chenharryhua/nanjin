package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.{
  ActionParams,
  Category,
  CounterKind,
  Importance,
  MetricID,
  MetricName
}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  ActionComplete,
  ActionFail,
  ActionRetry,
  ActionStart
}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent, TraceInfo}
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.lang3.exception.ExceptionUtils
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final private class ReTry[F[_], IN, OUT](
  metricRegistry: MetricRegistry,
  actionParams: ActionParams,
  channel: Channel[F, NJEvent],
  retryPolicy: RetryPolicy[F],
  arrow: IN => F[OUT],
  transError: IN => F[Json],
  transOutput: (IN, OUT) => F[Json],
  isWorthRetry: Throwable => F[Boolean]
)(implicit F: Async[F]) {
  private[this] val measures: MeasureAction = MeasureAction(actionParams, metricRegistry)

  @inline private[this] def buildJson(json: Either[Throwable, Json]): Json =
    json match {
      case Right(value) => value
      case Left(value)  => Json.fromString(ExceptionUtils.getMessage(value))
    }

  private[this] def sendFailureEvent(ai: ActionInfo, input: IN, ex: Throwable): F[Unit] =
    for {
      json <- transError(input).attempt.map(buildJson)
      landTime <- F.realTime
      _ <- channel.send(ActionFail(ai, landTime, NJError(ex), json))
    } yield measures.failure(landTime.minus(ai.launchTime))

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
          measures.countRetry()
          Left(status.addRetry(delay))
        }
    }

  private[this] def go(ai: ActionInfo, input: IN, continue: OUT => F[Unit]): F[OUT] =
    F.onCancel(
      F.tailRecM(RetryStatus.NoRetriesYet) { status =>
        arrow(input).attempt.flatMap {
          case Right(out)                => continue(out).as(Right(out))
          case Left(ex) if !NonFatal(ex) => fail(ai, input, ex)
          case Left(ex) =>
            isWorthRetry(ex).attempt
              .map(_.exists(identity))
              .ifM(retrying(ai, input, ex, status), fail(ai, input, ex))
        }
      },
      sendFailureEvent(ai, input, ActionCancelException)
    )

  private[this] def go(ai: ActionInfo, input: IN): F[OUT] =
    F.onCancel(
      F.tailRecM(RetryStatus.NoRetriesYet) { status =>
        arrow(input).attempt.flatMap {
          case Right(out)                => F.pure(Right(out))
          case Left(ex) if !NonFatal(ex) => fail(ai, input, ex)
          case Left(ex) =>
            isWorthRetry(ex).attempt
              .map(_.exists(identity))
              .ifM(retrying(ai, input, ex, status), fail(ai, input, ex))
        }
      },
      sendFailureEvent(ai, input, ActionCancelException)
    )

  private[this] def sendCompleteEvent(ai: ActionInfo, input: IN, out: OUT): F[FiniteDuration] =
    F.realTime.flatTap(landTime =>
      transOutput(input, out).attempt.flatMap(json =>
        channel.send(ActionComplete(ai, landTime, buildJson(json)))))

  sealed private trait Runner { def run(ai: ActionInfo, input: IN): F[OUT] }

  private[this] val runner: Runner =
    actionParams.importance match {
      case Importance.Critical | Importance.Notice if actionParams.isTiming || actionParams.isCounting =>
        new Runner {
          override def run(ai: ActionInfo, input: IN): F[OUT] = {
            val k = (out: OUT) =>
              sendCompleteEvent(ai, input, out).map(landTime =>
                measures.success(landTime.minus(ai.launchTime)))
            channel.send(ActionStart(ai)) >> go(ai, input, k)
          }
        }
      case Importance.Critical | Importance.Notice =>
        new Runner {
          override def run(ai: ActionInfo, input: IN): F[OUT] = {
            val k = (out: OUT) => sendCompleteEvent(ai, input, out).void
            channel.send(ActionStart(ai)) >> go(ai, input, k)
          }
        }
      case Importance.Aware if actionParams.isTiming || actionParams.isCounting =>
        new Runner {
          override def run(ai: ActionInfo, input: IN): F[OUT] = {
            val k = (out: OUT) =>
              sendCompleteEvent(ai, input, out).map(landTime =>
                measures.success(landTime.minus(ai.launchTime)))
            go(ai, input, k)
          }
        }

      case Importance.Aware =>
        new Runner {
          override def run(ai: ActionInfo, input: IN): F[OUT] = {
            val k = (out: OUT) => sendCompleteEvent(ai, input, out).void
            go(ai, input, k)
          }
        }

      case Importance.Silent if actionParams.isTiming || actionParams.isCounting =>
        new Runner {
          override def run(ai: ActionInfo, input: IN): F[OUT] = {
            val k = (_: OUT) => F.realTime.map(landTime => measures.success(landTime.minus(ai.launchTime)))
            go(ai, input, k)
          }
        }

      case Importance.Silent =>
        new Runner {
          override def run(ai: ActionInfo, input: IN): F[OUT] = go(ai, input)
        }
    }

  def run(input: IN): F[OUT] =
    (F.realTime, F.unique).flatMapN { (launchTime, token) =>
      val ai = ActionInfo(actionParams, token.hash.toString, None, launchTime)
      runner.run(ai, input)
    }

  def run(input: IN, traceInfo: Option[TraceInfo]): F[OUT] = traceInfo match {
    case ti @ Some(value) =>
      F.realTime.flatMap { launchTime =>
        val ai = ActionInfo(actionParams, value.spanId, ti, launchTime)
        runner.run(ai, input)
      }
    case None => run(input)
  }
}

private object ActionCancelException extends Exception("action was canceled")

sealed private trait MeasureAction {
  def success(fd: => FiniteDuration): Unit
  def failure(fd: => FiniteDuration): Unit
  def countRetry(): Unit
}
private object MeasureAction {
  def apply(actionParams: ActionParams, metricRegistry: MetricRegistry): MeasureAction = {
    val metricName: MetricName     = actionParams.metricID.metricName
    val succCat: Category.Counter  = Category.Counter(Some(CounterKind.ActionDone))
    val failCat: Category.Counter  = Category.Counter(Some(CounterKind.ActionFail))
    val retryCat: Category.Counter = Category.Counter(Some(CounterKind.ActionRetry))

    (actionParams.isCounting, actionParams.isTiming) match {
      case (true, true) =>
        new MeasureAction {
          private val fail    = metricRegistry.counter(MetricID(metricName, failCat).asJson.noSpaces)
          private val succ    = metricRegistry.counter(MetricID(metricName, succCat).asJson.noSpaces)
          private val retries = metricRegistry.counter(MetricID(metricName, retryCat).asJson.noSpaces)
          private val timer   = metricRegistry.timer(actionParams.metricID.asJson.noSpaces)

          override def success(fd: => FiniteDuration): Unit = {
            succ.inc(1)
            timer.update(fd.toNanos, TimeUnit.NANOSECONDS)
          }
          override def failure(fd: => FiniteDuration): Unit = {
            fail.inc(1)
            timer.update(fd.toNanos, TimeUnit.NANOSECONDS)
          }
          override def countRetry(): Unit = retries.inc(1)
        }
      case (true, false) =>
        new MeasureAction {
          private val fail    = metricRegistry.counter(MetricID(metricName, failCat).asJson.noSpaces)
          private val succ    = metricRegistry.counter(MetricID(metricName, succCat).asJson.noSpaces)
          private val retries = metricRegistry.counter(MetricID(metricName, retryCat).asJson.noSpaces)

          override def success(fd: => FiniteDuration): Unit = succ.inc(1)
          override def failure(fd: => FiniteDuration): Unit = fail.inc(1)
          override def countRetry(): Unit                   = retries.inc(1)
        }
      case (false, true) =>
        new MeasureAction {
          private val timer = metricRegistry.timer(actionParams.metricID.asJson.noSpaces)

          override def success(fd: => FiniteDuration): Unit =
            timer.update(fd.toNanos, TimeUnit.NANOSECONDS)
          override def failure(fd: => FiniteDuration): Unit =
            timer.update(fd.toNanos, TimeUnit.NANOSECONDS)
          override def countRetry(): Unit = ()
        }

      case (false, false) =>
        new MeasureAction {
          override def success(fd: => FiniteDuration): Unit = sys.error("should not happen")
          override def failure(fd: => FiniteDuration): Unit = ()
          override def countRetry(): Unit                   = ()
        }
    }
  }
}
