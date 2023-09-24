package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Outcome, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.TickStatus
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStopCause}
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.lang3.exception.ExceptionUtils
import org.typelevel.cats.time.instances.duration

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.control.NonFatal

final private class ReStart[F[_], A](
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  initTickStatus: TickStatus,
  theService: F[A])(implicit F: Temporal[F])
    extends duration {

  private def stop(cause: ServiceStopCause): F[Either[TickStatus, ServiceStopCause]] =
    F.pure(Right(cause))

  private def stopByException(err: Throwable): F[Either[TickStatus, ServiceStopCause]] =
    stop(ServiceStopCause.ByException(ExceptionUtils.getMessage(err)))

  private def panic(ts: TickStatus, err: Throwable): F[Either[TickStatus, ServiceStopCause]] =
    for {
      _ <- publisher.servicePanic(channel, serviceParams, ts.tick, err)
      _ <- F.sleep(ts.tick.snooze.toScala)
    } yield Left(ts)

  private val loop: F[ServiceStopCause] =
    F.tailRecM(initTickStatus) { status =>
      (publisher.serviceReStart(channel, serviceParams) >> theService).attempt.flatMap {
        case Right(_)                    => stop(ServiceStopCause.Normally)
        case Left(err) if !NonFatal(err) => stopByException(err)
        case Left(err) =>
          F.realTimeInstant.flatMap { now =>
            val tickStatus: TickStatus = serviceParams.threshold match {
              case Some(threshold) =>
                if (Duration.between(status.tick.acquire, now) > threshold) status.resetPolicy else status
              case None => status
            }

            tickStatus.next(now) match {
              case None      => stopByException(err)
              case Some(nts) => panic(nts, err)
            }
          }
      }
    }

  val stream: Stream[F, Nothing] =
    Stream
      .eval(F.guaranteeCase(loop) {
        case Outcome.Succeeded(fa) =>
          fa.flatMap(cause => publisher.serviceStop(channel, serviceParams, cause))
        case Outcome.Errored(e) =>
          publisher
            .serviceStop(channel, serviceParams, ServiceStopCause.ByException(ExceptionUtils.getMessage(e)))
        case Outcome.Canceled() =>
          publisher.serviceStop(channel, serviceParams, ServiceStopCause.ByCancelation)
      })
      .drain
}
