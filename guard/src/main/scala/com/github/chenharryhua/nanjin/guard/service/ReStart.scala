package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Outcome, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStopCause}
import fs2.Stream
import fs2.concurrent.Channel
import monocle.syntax.all.*
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.control.NonFatal

final private class ReStart[F[_], A](
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  policy: Policy,
  groundZero: Tick,
  theService: F[A])(implicit F: Temporal[F]) {

  private def stop(cause: ServiceStopCause): F[Either[Tick, ServiceStopCause]] =
    F.pure(Right(cause))

  private def stopByException(err: Throwable): F[Either[Tick, ServiceStopCause]] =
    stop(ServiceStopCause.ByException(ExceptionUtils.getMessage(err)))

  private def panic(tick: Tick, err: Throwable): F[Either[Tick, ServiceStopCause]] =
    for {
      _ <- publisher.servicePanic(channel, serviceParams, tick, err)
      _ <- F.sleep(tick.snooze.toScala)
    } yield Left(tick)

  private val loop: F[ServiceStopCause] =
    F.tailRecM(groundZero) { prev =>
      (publisher.serviceReStart(channel, serviceParams) >> theService).attempt.flatMap {
        case Right(_)                    => stop(ServiceStopCause.Normally)
        case Left(err) if !NonFatal(err) => stopByException(err)
        case Left(err) =>
          F.realTimeInstant.flatMap { now =>
            policy.decide(prev, now) match {
              case None => stopByException(err)
              // if no error happens for long enough, start over the policies
              case Some(tick) =>
                serviceParams.policyThreshold match {
                  case None => panic(tick, err)
                  case Some(threshold) =>
                    if (Duration.between(prev.wakeup, tick.wakeup).compareTo(threshold) > 0) {
                      policy.decide(prev.focus(_.counter).replace(0), now) match {
                        case Some(value) => panic(value, err)
                        case None        => stopByException(err)
                      }
                    } else panic(tick, err)
                }
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
