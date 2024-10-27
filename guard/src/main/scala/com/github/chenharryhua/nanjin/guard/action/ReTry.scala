package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.implicits.monadCancelOps
import cats.effect.kernel.{Async, Outcome, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.TickStatus
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceMessage
import fs2.concurrent.Channel
import io.circe.Json
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.ZonedDateTime
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.Try

private case object ActionCancelException extends Exception("action was canceled")

final private class ReTry[F[_]: Async, IN, OUT] private (
  private[this] val metricName: MetricName,
  private[this] val serviceParams: ServiceParams,
  private[this] val publishStrategy: PublishStrategy,
  private[this] val channel: Channel[F, NJEvent],
  private[this] val zerothTickStatus: TickStatus,
  private[this] val arrow: Kleisli[F, IN, OUT],
  private[this] val transInput: Reader[IN, Json],
  private[this] val transOutput: Reader[(IN, OUT), Json],
  private[this] val transError: Reader[(IN, Throwable), Json],
  private[this] val isWorthRetry: Reader[(IN, Throwable), Boolean]
) {
  private[this] val F = Async[F]

  private[this] def to_zdt(fd: FiniteDuration): ZonedDateTime =
    serviceParams.toZonedDateTime(fd)

  private[this] def bad_json(ex: Throwable): Json =
    Json.fromString(ExceptionUtils.getMessage(ex))

  private[this] def output_json(in: IN, out: OUT): Json =
    Try(transOutput.run((in, out))).fold(bad_json, identity)
  private[this] def input_json(in: IN): Json =
    Try(transInput.run(in)).fold(bad_json, identity)
  private[this] def error_json(in: IN, ex: Throwable): Json =
    Try(transError.run((in, ex))).fold(bad_json, identity)

  private[this] def send_failure(in: IN, ex: Throwable): F[Unit] =
    for {
      now <- F.realTime
      _ <- channel.send(
        ServiceMessage(
          metricName = metricName,
          timestamp = to_zdt(now),
          serviceParams = serviceParams,
          level = AlarmLevel.Error,
          message = error_json(in, ex)
        ))
    } yield ()

  private[this] def send_success(in: IN, out: OUT): F[Unit] =
    F.realTime.flatMap { now =>
      channel.send(
        ServiceMessage(
          metricName = metricName,
          timestamp = to_zdt(now),
          serviceParams = serviceParams,
          level = AlarmLevel.Done,
          message = output_json(in, out)
        ))
    }.void

  private[this] def retrying(in: IN, ex: Throwable, status: TickStatus): F[Either[TickStatus, OUT]] =
    if (isWorthRetry.run((in, ex))) {
      for {
        next <- F.realTimeInstant.map(status.next)
        tickStatus <- next match {
          case None => F.raiseError(ex) // run out of policy
          case Some(ts) =>
            for {
              _ <- channel.send(
                ServiceMessage(
                  metricName = metricName,
                  timestamp = ts.tick.zonedAcquire,
                  serviceParams = serviceParams,
                  level = AlarmLevel.Warn,
                  message = error_json(in, ex)
                ))
              _ <- F.sleep(ts.tick.snooze.toScala)
            } yield Left(ts)
        }
      } yield tickStatus
    } else {
      F.raiseError(ex)
    }

  private[this] def execute(in: IN): F[OUT] =
    F.tailRecM(zerothTickStatus) { status =>
      arrow.run(in).attempt.flatMap[Either[TickStatus, OUT]] {
        case Right(out) => F.pure(Right(out))
        case Left(ex)   => retrying(in, ex, status)
      }
    }

  // static functions

  private[this] def bipartite(in: IN): F[OUT] =
    F.realTime.flatMap { launchTime =>
      channel
        .send(
          ServiceMessage(
            metricName = metricName,
            timestamp = to_zdt(launchTime),
            serviceParams = serviceParams,
            level = AlarmLevel.Info,
            message = input_json(in)
          ))
        .flatMap(_ => execute(in))
        .guaranteeCase {
          case Outcome.Succeeded(fa) => fa.flatMap(send_success(in, _))
          case Outcome.Errored(e)    => send_failure(in, e)
          case Outcome.Canceled()    => send_failure(in, ActionCancelException)
        }
    }

  private[this] def unipartite(in: IN): F[OUT] =
    execute(in).guaranteeCase {
      case Outcome.Succeeded(fa) => fa.flatMap(send_success(in, _))
      case Outcome.Errored(e)    => send_failure(in, e)
      case Outcome.Canceled()    => send_failure(in, ActionCancelException)
    }

  private[this] def silent(in: IN): F[OUT] =
    execute(in).guaranteeCase {
      case Outcome.Succeeded(_) => F.unit
      case Outcome.Errored(e)   => send_failure(in, e)
      case Outcome.Canceled()   => send_failure(in, ActionCancelException)
    }

  val kleisli: Kleisli[F, IN, OUT] = {
    val choose: IN => F[OUT] =
      publishStrategy match {
        case PublishStrategy.Bipartite  => bipartite
        case PublishStrategy.Unipartite => unipartite
        case PublishStrategy.Silent     => silent
      }
    Kleisli[F, IN, OUT](choose)
  }
}

private object ReTry {
  def apply[F[_], IN, OUT](
    metricName: MetricName,
    serviceParams: ServiceParams,
    publishStrategy: PublishStrategy,
    zerothTickStatus: TickStatus,
    channel: Channel[F, NJEvent],
    arrow: Kleisli[F, IN, OUT],
    transInput: Reader[IN, Json],
    transOutput: Reader[(IN, OUT), Json],
    transError: Reader[(IN, Throwable), Json],
    isWorthRetry: Reader[(IN, Throwable), Boolean])(implicit
    F: Async[F]): Resource[F, Kleisli[F, IN, OUT]] = {
    val action_runner: ReTry[F, IN, OUT] =
      new ReTry[F, IN, OUT](
        metricName = metricName,
        serviceParams = serviceParams,
        publishStrategy = publishStrategy,
        channel = channel,
        zerothTickStatus = zerothTickStatus,
        arrow = arrow,
        transInput = transInput,
        transOutput = transOutput,
        transError = transError,
        isWorthRetry = isWorthRetry
      )

    Resource.pure(action_runner).map(_.kleisli)
  }
}
