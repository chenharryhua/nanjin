package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
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

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

final private class ReTry[F[_], IN, OUT](
  metricRegistry: MetricRegistry,
  actionParams: ActionParams,
  channel: Channel[F, NJEvent],
  retryPolicy: RetryPolicy[F],
  arrow: IN => F[OUT],
  transError: IN => F[Json],
  transOutput: (IN, OUT) => Json,
  isWorthRetry: Throwable => F[Boolean]
)(implicit F: Temporal[F]) {
  private val measures: MeasureAction = MeasureAction(actionParams, metricRegistry)

  private def sendFailureEvent(ai: ActionInfo, in: IN, ex: Throwable): F[Unit] =
    for {
      json <- transError(in).attempt.map(_.fold(ExceptionUtils.getMessage(_).asJson, identity))
      fd <- F.realTime
      _ <- channel.send(ActionFail(ai, fd, NJError(ex), json))
    } yield measures.failure(ai.took(fd))

  private[this] def fail(ai: ActionInfo, in: IN, ex: Throwable): F[Either[RetryStatus, OUT]] =
    sendFailureEvent(ai, in, ex) >> F.raiseError[OUT](ex).map[Either[RetryStatus, OUT]](Right(_))

  private def retrying(
    ai: ActionInfo,
    in: IN,
    ex: Throwable,
    status: RetryStatus): F[Either[RetryStatus, OUT]] =
    retryPolicy.decideNextRetry(status).flatMap {
      case PolicyDecision.GiveUp => fail(ai, in, ex)
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

  private def go(ai: ActionInfo, in: IN): F[OUT] =
    F.tailRecM(RetryStatus.NoRetriesYet) { status =>
      arrow(in).attempt.flatMap {
        case Right(out)                => F.pure(Right(out))
        case Left(ex) if !NonFatal(ex) => fail(ai, in, ex)
        case Left(ex) =>
          isWorthRetry(ex).attempt.map(_.exists(identity)).ifM(retrying(ai, in, ex, status), fail(ai, in, ex))
      }
    }

  private def sendCompleteEvent(ai: ActionInfo, in: IN, out: OUT): F[FiniteDuration] =
    F.realTime.flatMap(fd => channel.send(ActionComplete(ai, fd, transOutput(in, out))).as(fd))

  sealed private trait Runner { def run(ai: ActionInfo, in: IN): F[OUT] }

  private val runner: Runner =
    actionParams.importance match {

      // critical and notice
      case Importance.Critical | Importance.Notice if actionParams.isTiming || actionParams.isCounting =>
        new Runner {
          override def run(ai: ActionInfo, in: IN): F[OUT] =
            for {
              _ <- channel.send(ActionStart(ai))
              out <- go(ai, in)
              fd <- sendCompleteEvent(ai, in, out)
            } yield {
              measures.success(ai.took(fd))
              out
            }
        }
      case Importance.Critical | Importance.Notice =>
        new Runner {
          override def run(ai: ActionInfo, in: IN): F[OUT] =
            for {
              _ <- channel.send(ActionStart(ai))
              out <- go(ai, in)
              _ <- sendCompleteEvent(ai, in, out)
            } yield out
        }

      // aware
      case Importance.Aware if actionParams.isTiming || actionParams.isCounting =>
        new Runner {
          override def run(ai: ActionInfo, in: IN): F[OUT] =
            for {
              out <- go(ai, in)
              fd <- sendCompleteEvent(ai, in, out)
            } yield {
              measures.success(ai.took(fd))
              out
            }
        }
      case Importance.Aware =>
        new Runner {
          override def run(ai: ActionInfo, in: IN): F[OUT] =
            for {
              out <- go(ai, in)
              _ <- sendCompleteEvent(ai, in, out)
            } yield out
        }

      // silent
      case Importance.Silent if actionParams.isTiming =>
        new Runner {
          override def run(ai: ActionInfo, in: IN): F[OUT] =
            for {
              out <- go(ai, in)
              fd <- F.realTime
            } yield {
              measures.success(ai.took(fd))
              out
            }
        }
      case Importance.Silent if actionParams.isCounting =>
        new Runner {
          override def run(ai: ActionInfo, in: IN): F[OUT] =
            go(ai, in).map { out =>
              measures.countSuccess()
              out
            }
        }
      case Importance.Silent =>
        new Runner {
          override def run(ai: ActionInfo, in: IN): F[OUT] = go(ai, in)
        }
    }

  def run(in: IN): F[OUT] =
    (F.realTime, F.unique).flatMapN { (launchTime, token) =>
      val ai = ActionInfo(actionParams, token.hash.toString, None, launchTime)
      F.onCancel(runner.run(ai, in), sendFailureEvent(ai, in, ActionCancelException))
    }

  def run(in: IN, traceInfo: Option[TraceInfo]): F[OUT] = traceInfo match {
    case ti @ Some(value) =>
      F.realTime.flatMap { launchTime =>
        val ai = ActionInfo(actionParams, value.spanId, ti, launchTime)
        F.onCancel(runner.run(ai, in), sendFailureEvent(ai, in, ActionCancelException))
      }
    case None => run(in)
  }
}

private object ActionCancelException extends Exception("action was canceled")

sealed private trait MeasureAction {
  def success(fd: => Duration): Unit
  def failure(fd: => Duration): Unit
  def countRetry(): Unit
  def countSuccess(): Unit
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
          private lazy val fail    = metricRegistry.counter(MetricID(metricName, failCat).asJson.noSpaces)
          private lazy val succ    = metricRegistry.counter(MetricID(metricName, succCat).asJson.noSpaces)
          private lazy val retries = metricRegistry.counter(MetricID(metricName, retryCat).asJson.noSpaces)
          private lazy val timer   = metricRegistry.timer(actionParams.metricID.asJson.noSpaces)

          override def success(fd: => Duration): Unit = {
            succ.inc(1)
            timer.update(fd)
          }
          override def failure(fd: => Duration): Unit = {
            fail.inc(1)
            timer.update(fd)
          }
          override def countRetry(): Unit   = retries.inc(1)
          override def countSuccess(): Unit = succ.inc(1)
        }
      case (true, false) =>
        new MeasureAction {
          private lazy val fail    = metricRegistry.counter(MetricID(metricName, failCat).asJson.noSpaces)
          private lazy val succ    = metricRegistry.counter(MetricID(metricName, succCat).asJson.noSpaces)
          private lazy val retries = metricRegistry.counter(MetricID(metricName, retryCat).asJson.noSpaces)

          override def success(fd: => Duration): Unit = succ.inc(1)
          override def failure(fd: => Duration): Unit = fail.inc(1)
          override def countRetry(): Unit             = retries.inc(1)
          override def countSuccess(): Unit           = succ.inc(1)
        }
      case (false, true) =>
        new MeasureAction {
          private lazy val timer = metricRegistry.timer(actionParams.metricID.asJson.noSpaces)

          override def success(fd: => Duration): Unit = timer.update(fd)
          override def failure(fd: => Duration): Unit = timer.update(fd)
          override def countRetry(): Unit             = ()
          override def countSuccess(): Unit           = ()
        }

      case (false, false) =>
        new MeasureAction {
          override def success(fd: => Duration): Unit = ()
          override def failure(fd: => Duration): Unit = ()
          override def countRetry(): Unit             = ()
          override def countSuccess(): Unit           = ()
        }
    }
  }
}
