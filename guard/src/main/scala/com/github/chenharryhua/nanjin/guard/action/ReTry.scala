package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, OptionT}
import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.TickStatus
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, PublishStrategy}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionDone, ActionFail, ActionRetry, ActionStart}
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent}
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.parser.parse
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

final private class ReTry[F[_], IN, OUT](
  metricRegistry: MetricRegistry,
  actionParams: ActionParams,
  channel: Channel[F, NJEvent],
  zerothTickStatus: TickStatus,
  arrow: IN => F[OUT],
  transInput: Kleisli[Option, IN, Json],
  transOutput: Option[(IN, OUT) => Json],
  transError: Kleisli[OptionT[F, *], (IN, Throwable), Json],
  isWorthRetry: Throwable => F[Boolean]
)(implicit F: Temporal[F]) {

  private val measures: MeasureAction = MeasureAction(actionParams, metricRegistry)

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

  private def succeeding(ai: ActionInfo, in: IN, out: OUT): F[Either[TickStatus, OUT]] =
    for {
      fd <- F.realTime
      _ <- channel.send(ActionDone(actionParams, ai, fd, transOutput.map(_(in, out))))
    } yield {
      measures.done(ai.took(fd))
      Right(out)
    }

  private def sendFailure(ai: ActionInfo, in: IN, ex: Throwable): F[Unit] =
    for {
      js <- handleJson(transError((in, ex)).value)
      fd <- F.realTime
      _ <- channel.send(ActionFail(actionParams, ai, fd, NJError(ex), js))
    } yield measures.fail(ai.took(fd))

  private def failing(ai: ActionInfo, in: IN, ex: Throwable): F[Either[TickStatus, OUT]] =
    sendFailure(ai, in, ex) >> F.raiseError[OUT](ex).map[Either[TickStatus, OUT]](Right(_))

  private def retrying(
    ai: ActionInfo,
    in: IN,
    ex: Throwable,
    status: TickStatus): F[Either[TickStatus, OUT]] =
    F.realTimeInstant.flatMap { now =>
      status.next(now) match {
        case None => failing(ai, in, ex)
        case Some(ts) =>
          for {
            _ <- channel.send(
              ActionRetry(
                actionParams = actionParams,
                actionInfo = ai,
                error = NJError(ex),
                tick = ts.tick
              ))
            _ = measures.countRetry()
            _ <- F.sleep(ts.tick.snooze.toScala)
          } yield Left(ts)
      }
    }

  private def compute(ai: ActionInfo, in: IN): F[OUT] =
    F.tailRecM(zerothTickStatus) { status =>
      arrow(in).attempt.flatMap {
        case Right(out)                => succeeding(ai, in, out)
        case Left(ex) if !NonFatal(ex) => failing(ai, in, ex)
        case Left(ex) =>
          isWorthRetry(ex).attempt
            .map(_.exists(identity))
            .ifM(retrying(ai, in, ex, status), failing(ai, in, ex))
      }
    }

  private def computeSilently(ai: ActionInfo, in: IN): F[OUT] =
    F.tailRecM(zerothTickStatus) { status =>
      arrow(in).attempt.flatMap {
        case Right(out)                => F.pure(Right(out))
        case Left(ex) if !NonFatal(ex) => failing(ai, in, ex)
        case Left(ex) =>
          isWorthRetry(ex).attempt
            .map(_.exists(identity))
            .ifM(retrying(ai, in, ex, status), failing(ai, in, ex))
      }
    }

  sealed private trait KickOff { def apply(ai: ActionInfo, in: IN): F[OUT] }
  private val kickoff: KickOff =
    actionParams.publishStrategy match {
      case PublishStrategy.Notice =>
        new KickOff {
          override def apply(ai: ActionInfo, in: IN): F[OUT] =
            for {
              _ <- channel.send(ActionStart(actionParams, ai, transInput(in)))
              out <- compute(ai, in)
            } yield out
        }
      case PublishStrategy.Aware =>
        new KickOff {
          override def apply(ai: ActionInfo, in: IN): F[OUT] = compute(ai, in)
        }

      case PublishStrategy.Silent if actionParams.isTiming =>
        new KickOff {
          override def apply(ai: ActionInfo, in: IN): F[OUT] =
            for {
              out <- computeSilently(ai, in)
              fd <- F.realTime
              _ = measures.done(ai.took(fd))
            } yield out
        }

      case PublishStrategy.Silent if actionParams.isCounting =>
        new KickOff {
          override def apply(ai: ActionInfo, in: IN): F[OUT] =
            computeSilently(ai, in) <* F.pure(measures.done(Duration.ZERO))
        }

      case PublishStrategy.Silent =>
        new KickOff {
          override def apply(ai: ActionInfo, in: IN): F[OUT] = computeSilently(ai, in)
        }
    }

  def run(in: IN): F[OUT] =
    (F.realTime, F.unique).flatMapN { (launchTime, token) =>
      val ai = ActionInfo(token.hash, launchTime)
      F.onCancel(kickoff(ai, in), sendFailure(ai, in, ActionCancelException))
    }
}

private object ActionCancelException extends Exception("action was canceled")
