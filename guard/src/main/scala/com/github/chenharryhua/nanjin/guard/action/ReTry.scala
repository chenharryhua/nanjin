package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, OptionT}
import cats.effect.kernel.{Temporal, Unique}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.TickStatus
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, PublishStrategy}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionDone, ActionFail, ActionRetry, ActionStart}
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.jawn.parse
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}
import scala.util.Try
import scala.util.control.NonFatal

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
        Try(js.noSpaces).toEither.flatMap(parse) match {
          case Left(ex)     => jsonError(ex)
          case Right(value) => Some(value)
        })
  }

  private def succeeding(
    token: Unique.Token,
    launchTime: FiniteDuration,
    in: IN,
    out: OUT): F[Either[TickStatus, OUT]] =
    for {
      fd <- F.realTime
      _ <- channel.send(ActionDone(actionParams, token.hash, launchTime, fd, transOutput.map(_(in, out))))
      _ = measures.done(fd.minus(launchTime).toJava)
    } yield Right(out)

  private def sendFailure(token: Unique.Token, launchTime: FiniteDuration, in: IN, ex: Throwable): F[Unit] =
    for {
      js <- handleJson(transError((in, ex)).value)
      fd <- F.realTime
      _ <- channel.send(ActionFail(actionParams, token.hash, launchTime, fd, NJError(ex), js))
    } yield measures.fail(fd.minus(launchTime).toJava)

  private def failing(
    token: Unique.Token,
    launchTime: FiniteDuration,
    in: IN,
    ex: Throwable): F[Either[TickStatus, OUT]] =
    sendFailure(token, launchTime, in, ex).flatMap(_ => F.raiseError[Either[TickStatus, OUT]](ex))

  private def retrying(
    token: Unique.Token,
    launchTime: FiniteDuration,
    in: IN,
    ex: Throwable,
    status: TickStatus): F[Either[TickStatus, OUT]] =
    isWorthRetry(ex).attempt.map(_.exists(identity)).flatMap {
      case false => failing(token, launchTime, in, ex)
      case true =>
        for {
          next <- F.realTimeInstant.map(status.next)
          res <- next match {
            case None => failing(token, launchTime, in, ex)
            case Some(ts) =>
              for {
                _ <- channel.send(ActionRetry(actionParams, token.hash, launchTime, NJError(ex), ts.tick))
                _ <- F.sleep(ts.tick.snooze.toScala)
                _ = measures.countRetry()
              } yield Left(ts)
          }
        } yield res
    }

  private def bipartite(in: IN): F[OUT] =
    (F.unique, F.realTime).flatMapN { (token, launchTime) =>
      val go =
        channel.send(ActionStart(actionParams, token.hash, launchTime, transInput.map(_(in)))).flatMap { _ =>
          F.tailRecM(zerothTickStatus) { status =>
            arrow(in).attempt.flatMap[Either[TickStatus, OUT]] {
              case Right(out)                => succeeding(token, launchTime, in, out)
              case Left(ex) if !NonFatal(ex) => failing(token, launchTime, in, ex)
              case Left(ex)                  => retrying(token, launchTime, in, ex, status)
            }
          }
        }
      F.onCancel(go, sendFailure(token, launchTime, in, ActionCancelException))
    }

  private def unipartite(in: IN): F[OUT] =
    (F.unique, F.realTime).flatMapN { (token, launchTime) =>
      val go = F.tailRecM(zerothTickStatus) { status =>
        arrow(in).attempt.flatMap[Either[TickStatus, OUT]] {
          case Right(out)                => succeeding(token, launchTime, in, out)
          case Left(ex) if !NonFatal(ex) => failing(token, launchTime, in, ex)
          case Left(ex)                  => retrying(token, launchTime, in, ex, status)
        }
      }
      F.onCancel(go, sendFailure(token, launchTime, in, ActionCancelException))
    }

  private def silentTime(in: IN): F[OUT] =
    F.realTime.flatMap { launchTime =>
      val go = F.tailRecM(zerothTickStatus) { status =>
        arrow(in).attempt.flatMap[Either[TickStatus, OUT]] {
          case Right(out) =>
            F.realTime.map { fd => measures.done(fd.minus(launchTime).toJava); Right(out) }
          case Left(ex) if !NonFatal(ex) => F.unique.flatMap(failing(_, launchTime, in, ex))
          case Left(ex)                  => F.unique.flatMap(retrying(_, launchTime, in, ex, status))
        }
      }
      F.onCancel(go, F.unique.flatMap(sendFailure(_, launchTime, in, ActionCancelException)))
    }

  private def silentCount(in: IN): F[OUT] =
    F.realTime.flatMap { launchTime =>
      val go = F.tailRecM(zerothTickStatus) { status =>
        arrow(in).attempt.flatMap[Either[TickStatus, OUT]] {
          case Right(out) =>
            measures.done(Duration.ZERO)
            F.pure(Right(out))
          case Left(ex) if !NonFatal(ex) => F.unique.flatMap(failing(_, launchTime, in, ex))
          case Left(ex)                  => F.unique.flatMap(retrying(_, launchTime, in, ex, status))
        }
      }
      F.onCancel(go, F.unique.flatMap(sendFailure(_, launchTime, in, ActionCancelException)))
    }

  private def silent(in: IN): F[OUT] =
    F.realTime.flatMap { launchTime =>
      val go = F.tailRecM(zerothTickStatus) { status =>
        arrow(in).attempt.flatMap[Either[TickStatus, OUT]] {
          case Right(out)                => F.pure(Right(out))
          case Left(ex) if !NonFatal(ex) => F.unique.flatMap(failing(_, launchTime, in, ex))
          case Left(ex)                  => F.unique.flatMap(retrying(_, launchTime, in, ex, status))
        }
      }
      F.onCancel(go, F.unique.flatMap(sendFailure(_, launchTime, in, ActionCancelException)))
    }

  val run: IN => F[OUT] =
    actionParams.publishStrategy match {
      case PublishStrategy.Bipartite                         => bipartite
      case PublishStrategy.Unipartite                        => unipartite
      case PublishStrategy.Silent if actionParams.isTiming   => silentTime
      case PublishStrategy.Silent if actionParams.isCounting => silentCount
      case PublishStrategy.Silent                            => silent
    }
}

private object ActionCancelException extends Exception("action was canceled")
