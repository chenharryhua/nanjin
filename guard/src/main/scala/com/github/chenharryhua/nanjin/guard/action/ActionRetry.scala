package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, Reader}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickStatus}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceMessage
import fs2.concurrent.Channel
import io.circe.Json
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.Try

final private class ActionRetry[F[_]: Async, IN, OUT] private (
  private[this] val metricName: MetricName,
  private[this] val serviceParams: ServiceParams,
  private[this] val channel: Channel[F, NJEvent],
  private[this] val zerothTickStatus: TickStatus,
  private[this] val arrow: Kleisli[F, IN, OUT],
  private[this] val transError: Reader[(Tick, IN, Throwable), Json],
  private[this] val isWorthRetry: Reader[(IN, Throwable), Boolean]
) {
  private[this] val F = Async[F]

  private[this] def bad_json(ex: Throwable): Json =
    Json.fromString(ExceptionUtils.getMessage(ex))

  private[this] def error_json(tick: Tick, in: IN, ex: Throwable): Json =
    Try(transError.run((tick, in, ex))).fold(bad_json, identity)

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
                  message = error_json(ts.tick, in, ex)
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

  val kleisli: Kleisli[F, IN, OUT] = Kleisli[F, IN, OUT](execute)
}

private object ActionRetry {
  def apply[F[_], IN, OUT](
    metricName: MetricName,
    serviceParams: ServiceParams,
    zerothTickStatus: TickStatus,
    channel: Channel[F, NJEvent],
    arrow: Kleisli[F, IN, OUT],
    transError: Reader[(Tick, IN, Throwable), Json],
    isWorthRetry: Reader[(IN, Throwable), Boolean])(implicit
    F: Async[F]): Resource[F, Kleisli[F, IN, OUT]] = {
    val action_runner: ActionRetry[F, IN, OUT] =
      new ActionRetry[F, IN, OUT](
        metricName = metricName,
        serviceParams = serviceParams,
        channel = channel,
        zerothTickStatus = zerothTickStatus,
        arrow = arrow,
        transError = transError,
        isWorthRetry = isWorthRetry
      )

    Resource.pure(action_runner).map(_.kleisli)
  }
}
