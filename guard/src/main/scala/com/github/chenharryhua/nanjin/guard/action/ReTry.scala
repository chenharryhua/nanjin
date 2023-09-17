package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, OptionT}
import cats.effect.implicits.*
import cats.effect.kernel.{Outcome, Temporal}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import com.github.chenharryhua.nanjin.guard.config.{
  ActionParams,
  Category,
  CounterKind,
  MetricID,
  MetricName,
  PublishStrategy
}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionDone, ActionFail, ActionRetry, ActionStart}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent, TraceInfo}
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax.EncoderOps
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

final private class ReTry[F[_], IN, OUT](
  metricRegistry: MetricRegistry,
  actionParams: ActionParams,
  channel: Channel[F, NJEvent],
  retryPolicy: Policy,
  groundZero: Tick,
  arrow: IN => F[OUT],
  transInput: Kleisli[Option, IN, Json],
  transOutput: Option[(IN, OUT) => Json],
  transError: Kleisli[OptionT[F, *], (IN, Throwable), Json],
  isWorthRetry: Throwable => F[Boolean]
)(implicit F: Temporal[F]) {

  private val measures: MeasureAction = MeasureAction(actionParams, metricRegistry)

  private def retypeFailure(ex: Throwable): F[Either[Tick, OUT]] =
    F.raiseError[OUT](ex).map[Either[Tick, OUT]](Right(_))

  private def retrying(ai: ActionInfo, ex: Throwable, prev: Tick): F[Either[Tick, OUT]] =
    F.realTimeInstant.flatMap { now =>
      retryPolicy.decide(prev, now) match {
        case None => retypeFailure(ex)
        case Some(tick) =>
          for {
            _ <- channel.send(
              ActionRetry(
                actionParams = actionParams,
                actionInfo = ai,
                error = NJError(ex),
                tick = tick
              ))
            _ <- F.sleep(tick.snooze.toScala)
          } yield {
            measures.countRetry()
            Left(tick)
          }
      }
    }
  private def compute(ai: ActionInfo, in: IN): F[OUT] =
    F.tailRecM(groundZero) { status =>
      arrow(in).attempt.flatMap {
        case Right(out)                => F.pure(Right(out))
        case Left(ex) if !NonFatal(ex) => retypeFailure(ex)
        case Left(ex) =>
          isWorthRetry(ex).attempt.map(_.exists(identity)).ifM(retrying(ai, ex, status), retypeFailure(ex))
      }
    }

  sealed private trait KickOff { def apply(ai: ActionInfo, in: IN): F[OUT] }
  private val kickoff: KickOff =
    actionParams.publishStrategy match {
      case PublishStrategy.Notice =>
        new KickOff {
          override def apply(ai: ActionInfo, in: IN): F[OUT] =
            channel.send(ActionStart(actionParams, ai, transInput(in))) >> compute(ai, in)
        }
      case PublishStrategy.Aware | PublishStrategy.Silent =>
        new KickOff {
          override def apply(ai: ActionInfo, in: IN): F[OUT] = compute(ai, in)
        }
    }

  sealed private trait Postmortem {
    private def jsonError(throwable: Throwable): Option[Json] =
      Some(
        Json.obj(
          "description" -> Json.fromString("logError is unable to produce Json"),
          "message" -> Json.fromString(ExceptionUtils.getMessage(throwable))))

    private def handleJson(foj: F[Option[Json]]): F[Option[Json]] = foj.attempt.map {
      case Left(ex) => jsonError(ex)
      case Right(value) =>
        value.flatMap(js =>
          Try(js.noSpaces).map(parse) match {
            case Failure(ex) => jsonError(ex)
            case Success(value) =>
              value match {
                case Left(ex)     => jsonError(ex)
                case Right(value) => Some(value)
              }
          })
    }

    final def fail(ai: ActionInfo, in: IN, ex: Throwable): F[Unit] =
      for {
        js <- handleJson(transError((in, ex)).value)
        fd <- F.realTime
        _ <- channel.send(ActionFail(actionParams, ai, fd, NJError(ex), js))
      } yield measures.fail(ai.took(fd))

    def done(ai: ActionInfo, in: IN, fout: F[OUT]): F[Unit]
  }
  private val postmortem: Postmortem =
    actionParams.publishStrategy match {
      case PublishStrategy.Notice | PublishStrategy.Aware =>
        transOutput match {
          case Some(to_json) =>
            new Postmortem {
              override def done(ai: ActionInfo, in: IN, fout: F[OUT]): F[Unit] =
                for {
                  js <- fout.map(to_json(in, _).some)
                  fd <- F.realTime
                  _ <- channel.send(ActionDone(actionParams, ai, fd, js))
                } yield measures.done(ai.took(fd))
            }
          case None =>
            new Postmortem {
              override def done(ai: ActionInfo, in: IN, fout: F[OUT]): F[Unit] =
                for {
                  fd <- F.realTime
                  _ <- channel.send(ActionDone(actionParams, ai, fd, None))
                } yield measures.done(ai.took(fd))
            }
        }

      // silent
      case PublishStrategy.Silent if actionParams.isTiming =>
        new Postmortem {
          override def done(ai: ActionInfo, in: IN, fout: F[OUT]): F[Unit] =
            F.realTime.map(fd => measures.done(ai.took(fd)))
        }
      case PublishStrategy.Silent if actionParams.isCounting =>
        new Postmortem {
          override def done(ai: ActionInfo, in: IN, fout: F[OUT]): F[Unit] =
            F.pure(measures.done(Duration.ZERO))
        }
      case PublishStrategy.Silent =>
        new Postmortem {
          override def done(ai: ActionInfo, in: IN, fout: F[OUT]): F[Unit] =
            F.unit
        }
    }

  def run(in: IN, traceInfo: Option[TraceInfo]): F[OUT] =
    (F.realTime, F.unique).flatMapN { (launchTime, token) =>
      val ai = ActionInfo(token.hash, launchTime, traceInfo)
      kickoff(ai, in).guaranteeCase {
        case Outcome.Succeeded(fout) => postmortem.done(ai, in, fout)
        case Outcome.Errored(ex)     => postmortem.fail(ai, in, ex)
        case Outcome.Canceled()      => postmortem.fail(ai, in, ActionCancelException)
      }
    }
}

private object ActionCancelException extends Exception("action was canceled")

sealed private trait MeasureAction {
  def done(fd: => Duration): Unit
  def fail(fd: => Duration): Unit
  def countRetry(): Unit
}
private object MeasureAction {
  def apply(actionParams: ActionParams, metricRegistry: MetricRegistry): MeasureAction = {
    val metricName: MetricName     = actionParams.metricId.metricName
    val doneCat: Category.Counter  = Category.Counter(CounterKind.ActionDone)
    val failCat: Category.Counter  = Category.Counter(CounterKind.ActionFail)
    val retryCat: Category.Counter = Category.Counter(CounterKind.ActionRetry)

    (actionParams.isCounting, actionParams.isTiming) match {
      case (true, true) =>
        new MeasureAction {
          private lazy val failC  = metricRegistry.counter(MetricID(metricName, failCat).asJson.noSpaces)
          private lazy val doneC  = metricRegistry.counter(MetricID(metricName, doneCat).asJson.noSpaces)
          private lazy val retryC = metricRegistry.counter(MetricID(metricName, retryCat).asJson.noSpaces)
          private lazy val timer  = metricRegistry.timer(actionParams.metricId.asJson.noSpaces)

          override def done(fd: => Duration): Unit = {
            doneC.inc(1)
            timer.update(fd)
          }
          override def fail(fd: => Duration): Unit = {
            failC.inc(1)
            timer.update(fd)
          }
          override def countRetry(): Unit = retryC.inc(1)
        }
      case (true, false) =>
        new MeasureAction {
          private lazy val failC  = metricRegistry.counter(MetricID(metricName, failCat).asJson.noSpaces)
          private lazy val doneC  = metricRegistry.counter(MetricID(metricName, doneCat).asJson.noSpaces)
          private lazy val retryC = metricRegistry.counter(MetricID(metricName, retryCat).asJson.noSpaces)

          override def done(fd: => Duration): Unit = doneC.inc(1)
          override def fail(fd: => Duration): Unit = failC.inc(1)
          override def countRetry(): Unit          = retryC.inc(1)
        }
      case (false, true) =>
        new MeasureAction {
          private lazy val timer = metricRegistry.timer(actionParams.metricId.asJson.noSpaces)

          override def done(fd: => Duration): Unit = timer.update(fd)
          override def fail(fd: => Duration): Unit = timer.update(fd)
          override def countRetry(): Unit          = ()
        }

      case (false, false) =>
        new MeasureAction {
          override def done(fd: => Duration): Unit = ()
          override def fail(fd: => Duration): Unit = ()
          override def countRetry(): Unit          = ()
        }
    }
  }
}
