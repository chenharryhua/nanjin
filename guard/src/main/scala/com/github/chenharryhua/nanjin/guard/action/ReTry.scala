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
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

final private class ReTry[F[_], IN, OUT](
  metricRegistry: MetricRegistry,
  actionParams: ActionParams,
  channel: Channel[F, NJEvent],
  zerothTickStatus: TickStatus,
  arrow: IN => F[OUT],
  transInput: Option[IN => Json],
  transOutput: Option[(IN, OUT) => Json],
  transError: Kleisli[OptionT[F, *], (IN, Throwable), Json],
  isWorthRetry: Throwable => F[Boolean]
)(implicit F: Temporal[F]) {

  private val measures: MeasureAction = MeasureAction(actionParams, metricRegistry)
  def unregister(): Unit              = measures.unregister()

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
      _ = measures.done((fd - ai.launchTime).toJava)
    } yield Right(out)

  private def sendFailure(ai: ActionInfo, in: IN, ex: Throwable): F[Unit] =
    for {
      js <- handleJson(transError((in, ex)).value)
      fd <- F.realTime
      _ <- channel.send(ActionFail(actionParams, ai, fd, NJError(ex), js))
    } yield measures.fail((fd - ai.launchTime).toJava)

  private def failing(ai: ActionInfo, in: IN, ex: Throwable): F[Either[TickStatus, OUT]] =
    sendFailure(ai, in, ex) >> F.raiseError[OUT](ex).map[Either[TickStatus, OUT]](Right(_))

  private def retrying(
    ai: ActionInfo,
    in: IN,
    ex: Throwable,
    status: TickStatus): F[Either[TickStatus, OUT]] =
    isWorthRetry(ex).attempt.map(_.exists(identity)).flatMap {
      case false => failing(ai, in, ex)
      case true =>
        for {
          next <- F.realTimeInstant.map(status.next)
          res <- next match {
            case None => failing(ai, in, ex)
            case Some(ts) =>
              for {
                _ <- channel.send(ActionRetry(actionParams, ai, NJError(ex), ts.tick))
                _ = measures.countRetry()
                _ <- F.sleep(ts.tick.snooze.toScala)
              } yield Left(ts)
          }
        } yield res
    }

  private[this] val kickoff: (ActionInfo, IN) => F[OUT] =
    actionParams.publishStrategy match {
      case PublishStrategy.Notice =>
        (ai, in) =>
          channel.send(ActionStart(actionParams, ai, transInput.map(_(in)))).flatMap { _ =>
            F.tailRecM(zerothTickStatus) { status =>
              arrow(in).attempt.flatMap {
                case Right(out)                => succeeding(ai, in, out)
                case Left(ex) if !NonFatal(ex) => failing(ai, in, ex)
                case Left(ex)                  => retrying(ai, in, ex, status)
              }
            }
          }
      case PublishStrategy.Aware =>
        (ai, in) =>
          F.tailRecM(zerothTickStatus) { status =>
            arrow(in).attempt.flatMap {
              case Right(out)                => succeeding(ai, in, out)
              case Left(ex) if !NonFatal(ex) => failing(ai, in, ex)
              case Left(ex)                  => retrying(ai, in, ex, status)
            }
          }
      case PublishStrategy.Silent if actionParams.isTiming =>
        (ai, in) =>
          F.tailRecM(zerothTickStatus) { status =>
            arrow(in).attempt.flatMap {
              case Right(out) =>
                F.realTime.map { fd => measures.done((fd - ai.launchTime).toJava); Right(out) }
              case Left(ex) if !NonFatal(ex) => failing(ai, in, ex)
              case Left(ex)                  => retrying(ai, in, ex, status)
            }
          }

      case PublishStrategy.Silent if actionParams.isCounting =>
        (ai, in) =>
          F.tailRecM(zerothTickStatus) { status =>
            arrow(in).attempt.flatMap {
              case Right(out) =>
                measures.done(Duration.ZERO)
                F.pure(Right(out))
              case Left(ex) if !NonFatal(ex) => failing(ai, in, ex)
              case Left(ex)                  => retrying(ai, in, ex, status)
            }
          }

      case PublishStrategy.Silent =>
        (ai, in) =>
          F.tailRecM(zerothTickStatus) { status =>
            arrow(in).attempt.flatMap {
              case Right(out)                => F.pure(Right(out))
              case Left(ex) if !NonFatal(ex) => failing(ai, in, ex)
              case Left(ex)                  => retrying(ai, in, ex, status)
            }
          }
    }

  def run(in: IN): F[OUT] =
    (F.unique, F.realTime).flatMapN { (token, launchTime) =>
      val ai = ActionInfo(token.hash, launchTime)
      F.onCancel(kickoff(ai, in), sendFailure(ai, in, ActionCancelException))
    }
}

private object ActionCancelException extends Exception("action was canceled")
